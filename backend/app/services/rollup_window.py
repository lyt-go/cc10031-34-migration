"""窗口归并领域服务：维护固定、滑动与会话窗口的时序归并聚合指标与窗口条数。
对降采样后的序列数据按窗口类型进行窗口归并计算，生成归并结果数据集。"""
from __future__ import annotations

import time
from typing import Any

from app.services import time_basis
from app.services.base import BaseModuleService
from app.store import store


class RollupWindowService(BaseModuleService):
    MODULE = "rollup_window"
    ENTITY = "归并窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "测点编号", "窗口类型", "窗口长度", "滑动步长", "聚合指标", "窗口条数", "归并水位"]
    REQUIRED_FIELDS = ["窗口编号", "测点编号", "窗口类型"]
    STATUS_ORDER = ["草稿", "待归并", "已归并", "已作废"]
    ACTION_RULES = {"启动归并": "待归并", "确认归并": "已归并", "标记作废": "已作废"}
    # 数据归并操作：按窗口归并规则执行聚合计算生成归并结果数据集
    NEGATIVE_ACTIONS = []

    def rollup_windows(self, rollup_id: int) -> dict[str, Any]:
        """按窗口归并规则对降采样后的时序数据执行聚合计算，生成归并结果数据集。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        self.run_action(rollup_id, "启动归并")
        point_key = entry["测点编号"]
        window_type = entry.get("窗口类型", "固定窗口")
        window_len = int(entry.get("窗口长度", "5"))
        agg_metric = entry.get("聚合指标", "均值")

        # 读取降采样后的数据
        ds_files = list(self._data_dir("downsampled").glob(f"*{point_key}*_downsampled.jsonl")) if self._data_dir("downsampled").exists() else []
        records = []
        for f in ds_files[:3]:
            records.extend(self._read_jsonl(f))
        if not records:
            filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
            records = self._read_jsonl(filtered_file)

        # 按窗口类型执行归并聚合计算
        merged = []
        for i in range(0, len(records), window_len):
            window = records[i:i + window_len]
            values = []
            # 数据产物输出：写入归并聚合结果到窗口数据JSONL文件
            for r in window:
                v = r.get("aggregated_value", r.get("calibrated_value_V", 0))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
            if not values:
                continue
            if agg_metric in ("均值", "avg"):
                agg_val = round(sum(values) / len(values), 4)
            elif agg_metric in ("求和", "sum"):
                agg_val = round(sum(values), 4)
            elif agg_metric in ("最大值", "max"):
                agg_val = round(max(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            # 归并窗口的起点字段仍是记录序号（window_start），
            # 时间区间另存为 window_start_time / window_end_time：
            # 只有成员降采样都带窗口时刻时才可推导，否则留空。
            member_starts = [time_basis.parse_window_bound(r.get("window_start")) for r in window]
            member_ends = [time_basis.parse_window_bound(r.get("window_end")) for r in window]
            derivable = all(m is not None for m in member_starts + member_ends)
            merged.append({
                "window_id": entry["窗口编号"],
                "point": point_key,
                "window_type": window_type,
                "window_start": i + 1,
                "window_end": i + len(window),
                "window_start_time": (
                    time_basis.format_ts(min(member_starts)) if derivable else None
                ),
                "window_end_time": (
                    time_basis.format_ts(max(member_ends)) if derivable else None
                ),
                "window_grain_seconds": (
                    min(
                        (time_basis.parse_window_bound(r.get("window_end"))
                         - time_basis.parse_window_bound(r.get("window_start"))).total_seconds()
                        for r in window
                        if time_basis.parse_window_bound(r.get("window_start"))
                        and time_basis.parse_window_bound(r.get("window_end"))
                    ) if derivable else None
                ),
                "record_count": len(window),
                "rollup_value": agg_val,
                "metric": agg_metric,
            })
        # 将窗口归并结果写入归并窗口数据文件
        output_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        written = self._write_jsonl(output_file, merged)
        entry["窗口条数"] = str(written)
        entry["归并水位"] = str(written)
        self.run_action(rollup_id, "确认归并")
        timed = sum(1 for item in merged if item.get("window_start_time"))
        return {"rollup_id": entry["窗口编号"], "point": point_key,
                "window_type": window_type, "window_period_count": written,
                "metric": agg_metric, "windows_with_time": timed,
                "windows_ordinal_only": written - timed}

    def compute_cumulative_rollup(self, rollup_id: int) -> dict[str, Any]:
        """对归并窗口数据计算累计聚合指标写入累计快照数据文件。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        # 先执行归并
        rollup_result = self.rollup_windows(rollup_id)
        if "error" in rollup_result:
            return rollup_result
        # 读取归并结果计算累计
        rollup_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        records = self._read_jsonl(rollup_file)
        cumulative = []
        running_sum = 0.0
        for i, r in enumerate(records):
            val = float(r.get("rollup_value", 0))
            running_sum += val
            cumulative.append({
                "window_index": i + 1,
                "point": r["point"],
                "window_value": val,
                "cumulative_value": round(running_sum, 4),
                "window_id": r["window_id"],
            })
        # 写入累计聚合数据文件
        cum_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_cumulative.jsonl"
        self._write_jsonl(cum_file, cumulative)
        return {"rollup_id": entry["窗口编号"], "cumulative_periods": len(cumulative),
                "final_cumulative_value": round(running_sum, 4)}
    def rollup_fixed_window(self, rollup_id: int) -> dict[str, Any]:
        """固定窗口归并：按降采样窗口时刻均分成员，窗口长度 = 成员数。

        与 rollup_windows 的区别是按成员数分窗（而不是按序号），
        保证窗口边界与降采样时基对齐、可重放。
        """
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        self.run_action(rollup_id, "启动归并")
        point_key = entry["测点编号"]
        window_len = max(1, int(float(entry.get("窗口长度", "4") or 4)))
        agg_metric = entry.get("聚合指标", "均值")
        members: list[dict[str, Any]] = []
        for path in sorted(self._data_dir("downsampled").glob("*_downsampled.jsonl")):
            for record in self._read_jsonl(path):
                if str(record.get("point", "")) != point_key:
                    continue
                start = time_basis.parse_window_bound(record.get("window_start"))
                end = time_basis.parse_window_bound(record.get("window_end"))
                if start is None or end is None:
                    continue
                members.append({"record": record, "start": start, "end": end})
        members.sort(key=lambda item: (item["start"], str(item["record"].get("task_id")),
                                       int(item["record"].get("window_index", 0))))
        merged = []
        for index in range(0, len(members), window_len):
            chunk = members[index:index + window_len]
            if not chunk:
                continue
            values = []
            for item in chunk:
                try:
                    values.append(float(item["record"].get("aggregated_value", 0)))
                except (TypeError, ValueError):
                    values.append(0.0)
            if agg_metric in ("求和", "sum"):
                agg_val = round(sum(values), 4)
            elif agg_metric in ("最大值", "max"):
                agg_val = round(max(values), 4)
            elif agg_metric in ("最小值", "min"):
                agg_val = round(min(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            start = min(item["start"] for item in chunk)
            end = max(item["end"] for item in chunk)
            merged.append({
                "window_id": entry["窗口编号"],
                "point": point_key,
                "window_type": entry.get("窗口类型", "固定窗口"),
                "window_start": index + 1,
                "window_end": index + len(chunk),
                "window_start_time": time_basis.format_ts(start),
                "window_end_time": time_basis.format_ts(end),
                "window_grain_seconds": (end - start).total_seconds(),
                "member_windows": [
                    {"task_id": item["record"].get("task_id"),
                     "window_index": item["record"].get("window_index")}
                    for item in chunk
                ],
                "record_count": len(chunk),
                "rollup_value": agg_val,
                "metric": agg_metric,
            })
        output_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        written = self._write_jsonl_replace(output_file, merged)
        entry["窗口条数"] = str(written)
        entry["归并水位"] = str(written)
        self.run_action(rollup_id, "确认归并")
        return {"rollup_id": entry["窗口编号"], "point": point_key,
                "window_period_count": written, "member_count": len(members),
                "windows_with_time": written, "metric": agg_metric}
