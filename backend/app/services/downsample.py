"""降采样领域服务：维护高频时序到目标频率的降采样聚合方式与压缩比产出。
按聚合方式对时序数据进行降采样计算，写入降采样结果序列数据集。"""
from __future__ import annotations

import time
from typing import Any

from app.services import time_basis
from app.services.base import BaseModuleService
from app.store import store

# 降采样产物格式版本：v2 起每个窗口额外记录窗口开始/结束时刻，
# v1 产物只有 window_index（旧序号），仍可按序号回读。
PRODUCT_SCHEMA_VERSION = "v2"
LEGACY_SCHEMA_VERSION = "v1"


class DownsampleService(BaseModuleService):
    MODULE = "downsample"
    ENTITY = "降采样任务"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["任务编号", "源序列", "源采样频率", "目标频率", "聚合方式", "输出点数", "压缩比", "任务水位"]
    REQUIRED_FIELDS = ["任务编号", "源序列", "源采样频率"]
    STATUS_ORDER = ["草稿", "待执行", "执行中", "已完成"]
    ACTION_RULES = {"启动执行": "执行中", "标记完成": "已完成", "标记失败": "已完成"}
    NEGATIVE_ACTIONS = []

    # 数据聚合操作：按窗口执行降采样聚合计算生成降采样结果序列数据集
    def downsample_series(self, task_id: int, source_data_file: str | None = None) -> dict[str, Any]:
        """按聚合方式对高频时序数据进行降采样计算生成降采样结果数据集。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"error": f"降采样任务 {task_id} 不存在"}
        entry, _ = self.run_action(task_id, "启动执行")
        point_key = entry.get("源序列", entry["任务编号"])
        agg_method = entry.get("聚合方式", "均值")

        # 读取过滤后的序列数据
        filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
        records = self._read_jsonl(filtered_file)
        if not records:
            cal_file = self._data_dir("calibrated") / f"{point_key}_calibrated.jsonl"
            records = self._read_jsonl(cal_file)
        if not records:
            edge_file = self._data_dir("edge_samples") / f"{point_key}.jsonl"
            records = self._read_jsonl(edge_file)

        # 按窗口执行降采样聚合计算
        source_freq = float(entry.get("源采样频率", "10").replace("Hz", ""))
        target_freq = float(entry.get("目标频率", "1").replace("Hz", ""))
        window_size = max(1, int(source_freq / target_freq))
        declared_anchor, _ = time_basis.resolve_anchor(entry)
        declared_width, _ = time_basis.resolve_window_width(entry)
        downsampled = []
        for i in range(0, len(records), window_size):
            window = records[i:i + window_size]
            values = []
            for r in window:
                # 数据产物输出：将降采样结果写入降采样序列数据JSONL文件
                v = r.get("calibrated_value_V", r.get("raw_value_mV", r.get("raw_value", 0)))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
            if not values:
                continue
            if agg_method in ("均值", "avg"):
                agg_val = round(sum(values) / len(values), 4)
            elif agg_method in ("最大值", "max"):
                agg_val = round(max(values), 4)
            elif agg_method in ("最小值", "min"):
                agg_val = round(min(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            ordinal = len(downsampled) + 1
            # 新版窗口：从采样时间戳确定性算出窗口开始/结束时刻
            start, end, grain = time_basis.explicit_window_times(window)
            time_basis_label = "explicit"
            if start is None or end is None:
                # 源数据没有时间戳时退回到任务登记的锚点 + 宽度
                if declared_anchor is not None and declared_width:
                    start, end = time_basis.legacy_window_interval(
                        declared_anchor, declared_width, ordinal)
                    grain = declared_width
                    time_basis_label = "declared"
                else:
                    time_basis_label = "ordinal_only"
            downsampled.append({
                "task_id": entry["任务编号"],
                "point": point_key,
                "window_index": ordinal,
                "window_start": time_basis.format_ts(start),
                "window_end": time_basis.format_ts(end),
                "window_grain_seconds": grain,
                "time_basis": time_basis_label,
                "schema_version": PRODUCT_SCHEMA_VERSION,
                "source_count": len(window),
                "aggregated_value": agg_val,
                "method": agg_method,
                "target_freq": target_freq,
            })
        # 将降采样结果写入降采样序列数据文件
        output_file = self._data_dir("downsampled") / f"{entry['任务编号']}_downsampled.jsonl"
        written = self._write_jsonl(output_file, downsampled)
        compression_ratio = round(len(records) / max(written, 1), 2)
        entry["输出点数"] = str(written)
        entry["压缩比"] = str(compression_ratio)
        entry["任务水位"] = str(written)
        self.run_action(task_id, "标记完成")
        timed = sum(1 for r in downsampled if r.get("window_start") and r.get("window_end"))
        return {"task_id": entry["任务编号"], "point": point_key,
                "input_records": len(records), "output_points": written,
                "compression_ratio": compression_ratio, "method": agg_method,
                "schema_version": PRODUCT_SCHEMA_VERSION,
                "windows_with_time": timed, "windows_ordinal_only": written - timed}

    def batch_downsample(self, point_keys: list[str], agg_method: str = "均值") -> dict[str, Any]:
        """批量对多个测点的时序数据执行降采样聚合转换。"""
        results = []
        for pk in point_keys:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "任务编号": f"DOWN-{pk}-{tid}",
                "源序列": pk,
                "源采样频率": "10Hz",
                "目标频率": "1Hz",
                "聚合方式": agg_method,
                "输出点数": "0",
                "压缩比": "0",
                "任务水位": "0",
            })
            r = self.downsample_series(tid)
            results.append(r)
        return {"total_points": len(point_keys), "executed": len(results), "results": results}