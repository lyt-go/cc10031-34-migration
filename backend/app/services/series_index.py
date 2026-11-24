"""序列索引领域服务：维护时序数据按时间分区的点区间索引与存储位置登记。

新版（v2）索引同时落三份产物：

* ``<索引编号>_interval.jsonl``：**时间区间索引**，每条记录带窗口开始/结束时刻，
  并保留来源旧窗口标识（window_id）与窗口序号，用于回答“这段时间有哪些窗口”。
* ``<索引编号>_legacy.jsonl``：**旧序号索引**，覆盖全部旧产物记录（含只有条数的），
  按 ``window_id + window_index`` 定位，用于新版索引无法证明时基时继续按旧序号复盘。
* ``<索引编号>_manifest.json``：构建清单，记录覆盖率、时间粒度、待补证项与切换状态。

同序号但时基不同的旧窗口按 window_id 分组，绝不按序号合并。
"""
from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any

from app.services import time_basis
from app.services.base import BaseModuleService
from app.store import store

INDEX_SCHEMA_VERSION = "v2.0"
MODE_ORDINAL = "旧序号"
MODE_INTERVAL = "时间区间"


class SeriesIndexService(BaseModuleService):
    MODULE = "series_index"
    ENTITY = "索引分区"
    OPERATION_TERM = "存储"
    LIST_FIELDS = [
        "索引编号", "测点编号", "时间分区", "点区间", "索引粒度", "索引条数",
        "存储位置", "索引版本", "时间范围", "旧窗口标识", "查询口径",
    ]
    REQUIRED_FIELDS = ["索引编号", "测点编号", "时间分区"]
    STATUS_ORDER = ["草稿", "待构建", "已构建", "已失效"]
    ACTION_RULES = {"启动构建": "待构建", "确认构建": "已构建", "标记失效": "已失效"}
    # 数据存储操作：扫描归并窗口结果按时间分区构建序列索引分区写入索引元数据
    NEGATIVE_ACTIONS = ["标记失效"]

    # ------------------------------------------------------------------ #
    # 构建
    # ------------------------------------------------------------------ #
    def build_index(self, index_id: int) -> dict[str, Any]:
        """按时间分区构建新版时间区间索引，同时保留可按旧序号定位的完整索引。"""
        entry = store.find(self.MODULE, index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        self.run_action(index_id, "启动构建")
        point_key = entry["测点编号"]
        previous_mode = entry.get("查询口径") or MODE_ORDINAL
        fallback_partition = entry.get("时间分区") or time.strftime(
            "%Y-%m-%d", time.localtime())

        intervals: list[dict[str, Any]] = []
        legacy_rows: list[dict[str, Any]] = []
        legacy_windows: list[dict[str, Any]] = []
        pending_evidence: list[dict[str, Any]] = []

        # 1) 降采样产物：新版自带窗口时刻；旧版只有 window_index。
        for path in sorted(self._data_dir("downsampled").glob("*_downsampled.jsonl")):
            records = self._read_jsonl(path)
            if not records:
                continue
            task_no = str(records[0].get("task_id") or path.stem.removesuffix("_downsampled"))
            product_point = str(records[0].get("point") or point_key)
            if point_key and product_point != point_key:
                continue
            task = self._find_downsample_task(task_no)
            self._index_downsample_product(
                entry, task_no, product_point, records, task,
                intervals, legacy_rows, legacy_windows, pending_evidence,
                fallback_partition,
            )

        # 2) 归并产物：起点字段是记录序号，不能直接当时刻；只有归并时从降采样
        #    成员推导出的 window_start_time 才能进入时间区间索引。
        for path in sorted(self._data_dir("rollup_windows").glob("*_rollup.jsonl")):
            records = self._read_jsonl(path)
            if not records:
                continue
            window_id = str(records[0].get("window_id") or path.stem.removesuffix("_rollup"))
            product_point = str(records[0].get("point") or point_key)
            if point_key and product_point != point_key:
                continue
            self._index_rollup_product(
                entry, window_id, product_point, records,
                intervals, legacy_rows, legacy_windows, pending_evidence,
                fallback_partition,
            )

        intervals = time_basis.merge_intervals(intervals)
        legacy_rows = sorted(
            legacy_rows,
            key=lambda r: (str(r.get("window_id", "")), int(r.get("window_index", 0))),
        )
        for position, row in enumerate(legacy_rows, start=1):
            row["ordinal_position"] = position
            # 旧序号回读的稳定键：同一 window_id 内同序号是唯一记录
            row["ordinal_key"] = f"{row.get('window_id')}#{row.get('window_index')}"

        coverage = time_basis.coverage_summary(intervals)
        grains = sorted({
            round(float(item["window_grain_seconds"]), 6)
            for item in intervals
            if item.get("window_grain_seconds")
        })
        gaps = time_basis.detect_gaps(intervals)

        interval_file = self._data_dir("series_index") / f"{entry['索引编号']}_interval.jsonl"
        legacy_file = self._data_dir("series_index") / f"{entry['索引编号']}_legacy.jsonl"
        manifest_file = self._data_dir("series_index") / f"{entry['索引编号']}_manifest.json"
        compat_file = self._data_dir("series_index") / f"{entry['索引编号']}_index.jsonl"
        self._write_jsonl_replace(interval_file, intervals)
        self._write_jsonl_replace(legacy_file, legacy_rows)
        # 兼容产物：下游（冷热分层、导出血缘）仍按旧文件名扫描索引，内容改为
        # 带时间区间的记录，保证旧归档在升级后继续可复盘。
        self._write_jsonl_replace(compat_file, intervals or legacy_rows)

        unique_window_ids = sorted({str(item["window_id"]) for item in legacy_rows})
        manifest = {
            "index_id": entry["索引编号"],
            "point": point_key,
            "partition": fallback_partition,
            "schema_version": INDEX_SCHEMA_VERSION,
            "built_at": time.time(),
            "query_mode": previous_mode,
            "interval_count": len(intervals),
            "legacy_count": len(legacy_rows),
            "total_count": len(legacy_rows),
            "coverage": coverage,
            "time_grains_seconds": grains,
            "gaps": gaps,
            "legacy_windows": legacy_windows,
            "pending_evidence": pending_evidence,
            "rollup_windows": sorted({
                str(item["window_id"]) for item in intervals if item.get("source") == "rollup_window"
            } | {
                str(item["window_id"]) for item in legacy_rows if item.get("source") == "rollup_window"
            }),
            "interval_file": str(interval_file),
            "legacy_file": str(legacy_file),
        }
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)

        entry["索引条数"] = str(len(legacy_rows))
        entry["点区间"] = f"1-{len(legacy_rows)}" if legacy_rows else "0-0"
        entry["存储位置"] = str(interval_file if intervals else legacy_file)
        entry["索引版本"] = INDEX_SCHEMA_VERSION
        entry["时间范围"] = (
            f"{coverage['start']} ~ {coverage['end']}" if coverage["complete"] else "缺乏时间依据"
        )
        entry["旧窗口标识"] = ",".join(unique_window_ids)
        # 重建索引不改变已切换的查询口径：补证后重建不把当班读取打回旧序号
        entry["查询口径"] = previous_mode
        self.run_action(index_id, "确认构建")
        return {
            "index_id": entry["索引编号"],
            "point": point_key,
            "partition": fallback_partition,
            "indexed_count": len(legacy_rows),
            "interval_count": len(intervals),
            "legacy_count": len(legacy_rows),
            "pending_evidence_count": len(pending_evidence),
            "coverage": coverage,
            "time_grains_seconds": grains,
            "window_ids": unique_window_ids,
        }

    # -- 各类产物入库 --------------------------------------------------- #

    def _index_downsample_product(
        self,
        entry: dict[str, Any],
        window_id: str,
        point: str,
        records: list[dict[str, Any]],
        task: dict[str, Any] | None,
        intervals: list[dict[str, Any]],
        legacy_rows: list[dict[str, Any]],
        legacy_windows: list[dict[str, Any]],
        pending_evidence: list[dict[str, Any]],
        fallback_partition: str,
    ) -> None:
        evidenced = time_basis.normalize_new_intervals(window_id, records, point=point)
        if evidenced:
            for item in evidenced:
                intervals.append(self._interval_row(entry, item, point,
                                                    source="downsample",
                                                    origin="new_product",
                                                    evidence="产物自带窗口时间",
                                                    legacy_version="v2"))
            for item in evidenced:
                legacy_rows.append(self._legacy_row(entry, item, point,
                                                   source="downsample",
                                                   reason="",
                                                   time_known=True,
                                                   legacy_version="v2"))
            legacy_windows.append(self._window_bucket(
                window_id, point, [item["window_index"] for item in evidenced],
                time_basis_value="新版时间区间", grain=(
                    evidenced[0].get("window_grain_seconds")), has_time=True))
            return

        # 旧版产物：先看任务登记/补证后的时基证据
        task = task or {}
        derived, evidence = time_basis.derive_legacy_intervals(task, records)
        if derived:
            for item in derived:
                intervals.append(self._interval_row(entry, item, point,
                                                    source="downsample",
                                                    origin="legacy_derived",
                                                    evidence=evidence["evidence"],
                                                    legacy_version="v1"))
            for item in derived:
                legacy_rows.append(self._legacy_row(entry, item, point,
                                                   source="downsample",
                                                   reason="",
                                                   time_known=True,
                                                   legacy_version="v1"))
            legacy_windows.append(self._window_bucket(
                window_id, point, [item["window_index"] for item in derived],
                time_basis_value=evidence["evidence"],
                grain=evidence["window_width_seconds"], has_time=True))
            return

        # 只有条数：留在旧序号索引，明确列为待补证
        reason = "；".join(evidence.get("problems") or ["旧产物无窗口时刻"]) or "旧产物无窗口时刻"
        ordinals = []
        for position, record in enumerate(records, start=1):
            ordinal = self.ordinal_of(record, position)
            ordinals.append(ordinal)
            legacy_rows.append({
                "index_id": entry["索引编号"],
                "time_partition": fallback_partition,
                "point": point,
                "source": "downsample",
                "window_id": window_id,
                "window_index": ordinal,
                "window_grain_seconds": evidence.get("explicit_width_seconds"),
                "window_start": None,
                "window_end": None,
                "value": record.get("aggregated_value"),
                "method": record.get("method"),
                "record_count": record.get("source_count"),
                "origin": "legacy_ordinal_only",
                "time_basis": time_basis.EVIDENCE_COUNT_ONLY,
                "time_known": False,
                "legacy_readable": True,
                "legacy_version": "v1",
                "reason": reason,
            })
        legacy_windows.append(self._window_bucket(
            window_id, point, ordinals,
            time_basis_value=time_basis.EVIDENCE_COUNT_ONLY,
            grain=evidence.get("explicit_width_seconds"), has_time=False))
        pending_evidence.append({
            "window_id": window_id,
            "point": point,
            "source": "downsample",
            "ordinal_range": self.ordinal_range(ordinals),
            "record_count": len(records),
            "reason": reason,
        })

    def _index_rollup_product(
        self,
        entry: dict[str, Any],
        window_id: str,
        point: str,
        records: list[dict[str, Any]],
        intervals: list[dict[str, Any]],
        legacy_rows: list[dict[str, Any]],
        legacy_windows: list[dict[str, Any]],
        pending_evidence: list[dict[str, Any]],
        fallback_partition: str,
    ) -> None:
        timed: list[dict[str, Any]] = []
        for position, record in enumerate(records, start=1):
            start = time_basis.parse_window_bound(record.get("window_start_time"))
            end = time_basis.parse_window_bound(record.get("window_end_time"))
            if start is None or end is None or end <= start:
                continue
            timed.append({
                "window_id": window_id,
                "window_index": self.ordinal_of(record, position),
                "window_start": time_basis.format_ts(start),
                "window_end": time_basis.format_ts(end),
                "window_grain_seconds": record.get("window_grain_seconds"),
                "value": record.get("rollup_value"),
                "method": record.get("metric"),
                "record_count": record.get("record_count"),
                "origin": "rollup_derived",
            })
        ordinals = [self.ordinal_of(record, position)
                    for position, record in enumerate(records, start=1)]
        for item in timed:
            intervals.append(self._interval_row(entry, item, point,
                                                source="rollup_window",
                                                origin="rollup_derived",
                                                evidence="归并成员降采样窗口时间",
                                                legacy_version="v1"))
            legacy_rows.append(self._legacy_row(entry, item, point,
                                               source="rollup_window",
                                               reason="",
                                               time_known=True,
                                               legacy_version="v1"))
        timed_ordinals = {item["window_index"] for item in timed}
        for position, record in enumerate(records, start=1):
            ordinal = self.ordinal_of(record, position)
            if ordinal in timed_ordinals:
                continue
            legacy_rows.append({
                "index_id": entry["索引编号"],
                "time_partition": fallback_partition,
                "point": point,
                "source": "rollup_window",
                "window_id": window_id,
                "window_index": ordinal,
                "window_grain_seconds": None,
                "window_start": None,
                "window_end": None,
                "value": record.get("rollup_value"),
                "method": record.get("metric"),
                "record_count": record.get("record_count"),
                "origin": "rollup_ordinal_only",
                "time_basis": time_basis.EVIDENCE_COUNT_ONLY,
                "time_known": False,
                "legacy_readable": True,
                "legacy_version": "v1",
                "reason": "归并窗口起点为记录序号，缺乏时间依据",
            })
        has_time = bool(timed)
        legacy_windows.append(self._window_bucket(
            window_id, point, ordinals,
            time_basis_value=(
                "归并成员降采样窗口时间" if has_time else time_basis.EVIDENCE_COUNT_ONLY),
            grain=(timed[0].get("window_grain_seconds") if timed else None),
            has_time=has_time))
        if not has_time:
            pending_evidence.append({
                "window_id": window_id,
                "point": point,
                "source": "rollup_window",
                "ordinal_range": self.ordinal_range(ordinals),
                "record_count": len(records),
                "reason": "归并窗口起点为记录序号，缺乏时间依据",
            })

    # -- 行构造 --------------------------------------------------------- #

    def _interval_row(
        self, entry: dict[str, Any], item: dict[str, Any], point: str,
        *, source: str, origin: str, evidence: str, legacy_version: str,
    ) -> dict[str, Any]:
        start = time_basis.parse_timestamp(item["window_start"])
        partition = start.strftime("%Y-%m-%d") if start else entry.get("时间分区")
        return {
            "index_id": entry["索引编号"],
            "index_version": INDEX_SCHEMA_VERSION,
            "time_partition": partition,
            "point": point,
            "source": source,
            "window_id": item["window_id"],
            "window_index": int(item["window_index"]),
            "window_grain_seconds": item.get("window_grain_seconds"),
            "window_start": item["window_start"],
            "window_end": item["window_end"],
            "value": item.get("value"),
            "method": item.get("method"),
            "record_count": item.get("record_count") or item.get("source_count"),
            "origin": origin,
            "evidence": evidence,
            "time_known": True,
            "time_verified": True,
            "query_eligible": True,
            "legacy_window": item["window_id"],
            "legacy_version": legacy_version,
        }

    def _legacy_row(
        self, entry: dict[str, Any], item: dict[str, Any], point: str,
        *, source: str, reason: str, time_known: bool, legacy_version: str,
    ) -> dict[str, Any]:
        return {
            "index_id": entry["索引编号"],
            "time_partition": entry.get("时间分区"),
            "point": point,
            "source": source,
            "window_id": item["window_id"],
            "window_index": int(item["window_index"]),
            "window_grain_seconds": item.get("window_grain_seconds"),
            "window_start": item["window_start"],
            "window_end": item["window_end"],
            "value": item.get("value"),
            "method": item.get("method"),
            "record_count": item.get("record_count") or item.get("source_count"),
            "origin": item.get("origin"),
            "time_basis": "时间区间" if time_known else time_basis.EVIDENCE_COUNT_ONLY,
            "time_known": time_known,
            "legacy_readable": True,
            "legacy_version": legacy_version,
            "reason": reason,
        }

    @staticmethod
    def _window_bucket(
        window_id: str, point: str, ordinals: list[int], *,
        time_basis_value: str, grain: float | None, has_time: bool,
    ) -> dict[str, Any]:
        ordered = sorted(ordinals)
        return {
            "window_id": window_id,
            "point": point,
            "ordinal_range": SeriesIndexService.ordinal_range(ordered),
            "record_count": len(ordered),
            "time_basis": time_basis_value,
            "time_grain_seconds": grain,
            "has_time_basis": has_time,
        }

    @staticmethod
    def ordinal_of(record: dict[str, Any], position: int) -> int:
        try:
            return int(record.get("window_index", position))
        except (TypeError, ValueError):
            return position

    @staticmethod
    def ordinal_range(ordinals: list[int]) -> str:
        if not ordinals:
            return ""
        return f"{min(ordinals)}-{max(ordinals)}"

    @staticmethod
    def _find_downsample_task(task_no: str) -> dict[str, Any] | None:
        for row in store.rows("downsample"):
            if str(row.get("任务编号", "")) == task_no:
                return row
        return None

    # ------------------------------------------------------------------ #
    # 清单与查询
    # ------------------------------------------------------------------ #
    def index_manifest(self, index_id: int | str) -> dict[str, Any]:
        entry = self.resolve_index(index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        manifest = self.read_manifest(entry["索引编号"])
        if manifest is None:
            return {"error": f"索引分区 {entry['索引编号']} 尚未构建清单"}
        return manifest

    def read_legacy_ordinals(
        self,
        index_id: int | str,
        *,
        window_id: str | None = None,
        start_ordinal: int | None = None,
        end_ordinal: int | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        """按旧窗口标识 + 序号回读；新版索引无法证明时基时，这条路径仍然可用。"""
        entry = self.resolve_index(index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        path = self._data_dir("series_index") / f"{entry['索引编号']}_legacy.jsonl"
        rows = self._read_jsonl(path)
        if not rows:
            return {"index_id": entry["索引编号"], "records": [], "ordinal_groups": {},
                    "matched": 0, "note": "旧序号索引为空，请先构建"}
        matched = []
        for row in rows:
            if window_id is not None and str(row.get("window_id")) != str(window_id):
                continue
            try:
                ordinal = int(row.get("window_index", 0))
            except (TypeError, ValueError):
                ordinal = 0
            if start_ordinal is not None and ordinal < start_ordinal:
                continue
            if end_ordinal is not None and ordinal > end_ordinal:
                continue
            matched.append(row)
        groups = time_basis.group_ordinals(matched)
        return {
            "index_id": entry["索引编号"],
            "window_id": window_id,
            "ordinal_filter": [start_ordinal, end_ordinal],
            "matched": len(matched),
            "ordinal_groups": groups,
            "records": matched[:limit],
            "truncated": len(matched) > limit,
            "mode": entry.get("查询口径") or MODE_ORDINAL,
        }

    def query_time_range(
        self,
        index_id: int | str,
        start: Any,
        end: Any,
        *,
        limit: int = 500,
    ) -> dict[str, Any]:
        """按时间区间查询新版索引，返回命中窗口、覆盖范围与待补证项。"""
        entry = self.resolve_index(index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        begin = time_basis.parse_timestamp(start)
        finish = time_basis.parse_timestamp(end)
        if begin is None or finish is None or finish <= begin:
            return {"error": "查询时间区间无法解析或起止颠倒"}
        manifest = self.read_manifest(entry["索引编号"]) or {}
        rows = self._read_jsonl(self._data_dir("series_index") / f"{entry['索引编号']}_interval.jsonl")
        hits = []
        for row in rows:
            row_start = time_basis.parse_timestamp(row.get("window_start"))
            row_end = time_basis.parse_timestamp(row.get("window_end"))
            if row_start is None or row_end is None:
                continue
            if row_start < finish and row_end > begin:
                hits.append(row)
        hits.sort(key=lambda r: (str(r.get("window_start")), str(r.get("window_id")),
                                 int(r.get("window_index", 0))))
        coverage = time_basis.coverage_summary(hits)
        return {
            "index_id": entry["索引编号"],
            "query_range": [time_basis.format_ts(begin), time_basis.format_ts(finish)],
            "query_mode": entry.get("查询口径") or MODE_ORDINAL,
            "matched": len(hits),
            "windows": hits[:limit],
            "truncated": len(hits) > limit,
            "coverage": coverage,
            "partial": coverage["start"] is not None and (
                time_basis.parse_timestamp(coverage["start"]) > begin
                or time_basis.parse_timestamp(coverage["end"]) < finish
            ),
            "pending_evidence": manifest.get("pending_evidence", []),
        }

    # ------------------------------------------------------------------ #
    # 切换 / 回滚
    # ------------------------------------------------------------------ #
    def switch_index_mode(self, index_id: int | str, mode: str = MODE_INTERVAL) -> dict[str, Any]:
        """把索引查询口径切到指定模式；切换前核对归并历史依赖仍在索引内。"""
        entry = self.resolve_index(index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        if mode not in (MODE_INTERVAL, MODE_ORDINAL):
            return {"error": f"未知查询口径：{mode}"}
        manifest = self.read_manifest(entry["索引编号"])
        if manifest is None:
            return {"error": f"索引分区 {entry['索引编号']} 尚未构建，不能切换"}
        if mode == MODE_INTERVAL:
            rollup_products = [
                path for path in self._data_dir("rollup_windows").glob("*_rollup.jsonl")
                if self._file_has_point(path, entry["测点编号"])
            ]
            if rollup_products and not manifest.get("rollup_windows"):
                return {
                    "error": "归并历史依赖未进入索引，切换会丢失历史数据定位能力，已拒绝",
                    "missing_rollup_products": [p.name for p in rollup_products],
                }
        previous = entry.get("查询口径") or MODE_ORDINAL
        entry["查询口径"] = mode
        manifest["query_mode"] = mode
        manifest["last_switch"] = {
            "from": previous,
            "to": mode,
            "at": time.time(),
            "interval_count": manifest.get("interval_count"),
            "legacy_count": manifest.get("legacy_count"),
        }
        self._write_manifest(entry["索引编号"], manifest)
        self._write_jsonl_replace(
            self._data_dir("series_index") / f"{entry['索引编号']}_switch_log.jsonl",
            [{"index_id": entry["索引编号"], "from": previous, "to": mode,
              "at": time.time(), "interval_count": manifest.get("interval_count"),
              "legacy_count": manifest.get("legacy_count"),
              "rollup_dependency": manifest.get("rollup_windows", [])}],
        )
        return {
            "index_id": entry["索引编号"],
            "from": previous,
            "to": mode,
            "coverage": manifest.get("coverage"),
            "legacy_count": manifest.get("legacy_count"),
            "interval_count": manifest.get("interval_count"),
            "legacy_still_readable": True,
            "rollup_dependency": manifest.get("rollup_windows", []),
            "pending_evidence": manifest.get("pending_evidence", []),
        }

    def rollback_index_mode(self, index_id: int | str) -> dict[str, Any]:
        """回滚到旧序号口径，核对回滚后仍可定位的窗口与失去时间依据的历史范围。"""
        entry = self.resolve_index(index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        manifest = self.read_manifest(entry["索引编号"])
        if manifest is None:
            return {"error": f"索引分区 {entry['索引编号']} 尚未构建，不能回滚"}
        result = self.switch_index_mode(entry["索引编号"], MODE_ORDINAL)
        if "error" in result:
            return result
        legacy_rows = self._read_jsonl(
            self._data_dir("series_index") / f"{entry['索引编号']}_legacy.jsonl")
        interval_rows = self._read_jsonl(
            self._data_dir("series_index") / f"{entry['索引编号']}_interval.jsonl")
        interval_keys = {
            (str(row.get("window_id")), int(row.get("window_index", 0))) for row in interval_rows
        }
        locatable = time_basis.group_ordinals(legacy_rows)
        lost_time = [
            {"window_id": window_id, "ordinal_range": bucket["ordinal_range"],
             "record_count": bucket["record_count"]}
            for window_id, bucket in locatable.items()
            if not bucket["has_time_basis"]
        ]
        time_lost = [
            {"window_id": str(row.get("window_id")),
             "window_index": int(row.get("window_index", 0)),
             "window_grain_seconds": row.get("window_grain_seconds")}
            for row in interval_rows
            if (str(row.get("window_id")), int(row.get("window_index", 0))) not in interval_keys
        ]
        return {
            "index_id": entry["索引编号"],
            "mode": entry.get("查询口径"),
            "legacy_records": len(legacy_rows),
            "interval_records_preserved": len(interval_rows),
            "ordinal_groups": locatable,
            "ranges_without_time_basis": lost_time,
            "interval_windows_now_unreachable_by_time": time_lost,
            "note": "回滚只改查询口径；新版区间产物原地保留，可随时再切回。",
        }

    # ------------------------------------------------------------------ #
    # 工具
    # ------------------------------------------------------------------ #
    def resolve_index(self, ref: int | str) -> dict[str, Any] | None:
        if isinstance(ref, int) or (isinstance(ref, str) and ref.isdigit()):
            return store.find(self.MODULE, int(ref))
        for row in store.rows(self.MODULE):
            if str(row.get("索引编号", "")) == str(ref):
                return row
        return None

    def _manifest_path(self, index_no: str):
        return self._data_dir("series_index") / f"{index_no}_manifest.json"

    def read_manifest(self, index_no: str) -> dict[str, Any] | None:
        path = self._manifest_path(index_no)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return None

    def _write_manifest(self, index_no: str, manifest: dict[str, Any]) -> None:
        path = self._manifest_path(index_no)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)

    def _file_has_point(self, path, point_key: str) -> bool:
        records = self._read_jsonl(path)
        return any(str(r.get("point", "")) == str(point_key) for r in records)

    def build_all_partitions(self, point_key: str) -> dict[str, Any]:
        """为指定测点构建所有时间分区的序列索引数据集。"""
        partitions = ["2026-09-01", "2026-09-02", "2026-09-03"]
        results = []
        for part in partitions:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "索引编号": f"IDX-{point_key}-{part}",
                "测点编号": point_key,
                "时间分区": part,
                "点区间": "0-0",
                "索引粒度": "1h",
                "索引条数": "0",
                "存储位置": "",
                "索引版本": INDEX_SCHEMA_VERSION,
                "时间范围": "缺乏时间依据",
                "旧窗口标识": "",
                "查询口径": MODE_ORDINAL,
            })
            r = self.build_index(tid)
            results.append(r)
        return {"point": point_key, "partitions_built": len(results), "results": results}

    # ------------------------------------------------------------------ #
    # 供区间查询服务复用的只读入口
    # ------------------------------------------------------------------ #
    def data_dir(self, name: str):
        return self._data_dir(name)

    def read_jsonl(self, path) -> list[dict[str, Any]]:
        return self._read_jsonl(path)

    def write_jsonl(self, path, records: list[dict[str, Any]]) -> int:
        return self._write_jsonl(path, records)

    def write_jsonl_replace(self, path, records: list[dict[str, Any]]) -> int:
        return self._write_jsonl_replace(path, records)

    # ------------------------------------------------------------------ #
    # 迁移验证：两份同序号不同粒度的旧产物 + 一份新版产物
    # ------------------------------------------------------------------ #
    def seed_migration_demo(
        self,
        point: str = "SENS-DEMO",
        anchor: str = "2026-09-01T10:00:00+00:00",
    ) -> dict[str, Any]:
        """构造迁移验证素材并执行一次切换/回滚，返回可定位窗口与缺时基范围。

        素材：
        * 旧产物 A：12 个 60s 窗口，序号 1-12，未登记采集开始时刻（只有条数）；
        * 旧产物 B：24 个 30s 窗口，序号 1-24，登记了采集开始时刻 + 30s 宽度；
        * 新版产物 N：12 个 60s 窗口，自带窗口开始/结束时刻；
        * 归并产物 R：按新版成员的窗口时间归并出的 3 个窗口。
        """
        start = time_basis.parse_timestamp(anchor)
        if start is None:
            return {"error": f"锚点时刻无法解析：{anchor}"}
        downsample_dir = self._data_dir("downsampled")

        legacy_a = [{
            "task_id": "DOWN-DEMO-A", "point": point, "window_index": i,
            "source_count": 10, "aggregated_value": round(20 + i * 0.1, 4),
            "method": "均值", "target_freq": 1 / 60,
        } for i in range(1, 13)]
        legacy_b = [{
            "task_id": "DOWN-DEMO-B", "point": point, "window_index": i,
            "source_count": 5, "aggregated_value": round(30 + i * 0.1, 4),
            "method": "均值", "target_freq": 1 / 30,
        } for i in range(1, 25)]
        new_product = [{
            "task_id": "DOWN-DEMO-N", "point": point, "window_index": i,
            "window_start": time_basis.format_ts(start + timedelta(seconds=(i - 1) * 60)),
            "window_end": time_basis.format_ts(start + timedelta(seconds=i * 60)),
            "window_grain_seconds": 60, "time_basis": "explicit", "schema_version": "v2",
            "source_count": 10, "aggregated_value": round(40 + i * 0.1, 4),
            "method": "均值", "target_freq": 1 / 60,
        } for i in range(1, 13)]
        self._write_jsonl_replace(downsample_dir / "DOWN-DEMO-A_downsampled.jsonl", legacy_a)
        self._write_jsonl_replace(downsample_dir / "DOWN-DEMO-B_downsampled.jsonl", legacy_b)
        self._write_jsonl_replace(downsample_dir / "DOWN-DEMO-N_downsampled.jsonl", new_product)

        self._reset_demo_row("downsample", {
            "任务编号": "DOWN-DEMO-A", "源序列": point, "源采样频率": "1Hz",
            "目标频率": "0.0167Hz", "聚合方式": "均值", "输出点数": "12",
            "压缩比": "1", "任务水位": "12",
        }, seed_only=True)
        self._reset_demo_row("downsample", {
            "任务编号": "DOWN-DEMO-B", "源序列": point, "源采样频率": "1Hz",
            "目标频率": "0.0333Hz", "聚合方式": "均值", "输出点数": "24",
            "压缩比": "1", "任务水位": "24",
            "采集开始时刻": time_basis.format_ts(start), "窗口宽度": "30s",
        }, seed_only=True)
        self._reset_demo_row("downsample", {
            "任务编号": "DOWN-DEMO-N", "源序列": point, "源采样频率": "1Hz",
            "目标频率": "0.0167Hz", "聚合方式": "均值", "输出点数": "12",
            "压缩比": "1", "任务水位": "12",
        }, seed_only=True)
        rollup_row = self._reset_demo_row("rollup_window", {
            "窗口编号": "ROLL-DEMO", "测点编号": point, "窗口类型": "固定窗口",
            "窗口长度": "4", "滑动步长": "4", "聚合指标": "均值",
            "窗口条数": "0", "归并水位": "0",
        }, seed_only=True)
        from app.services.rollup_window import RollupWindowService
        rollup = RollupWindowService().rollup_fixed_window(rollup_row["id"])
        # 只保留新版产物参与归并验证
        index_row = self._reset_demo_row("series_index", {
            "索引编号": "IDX-DEMO", "测点编号": point, "时间分区": "2026-09-01",
            "点区间": "0-0", "索引粒度": "1min", "索引条数": "0", "存储位置": "",
            "索引版本": INDEX_SCHEMA_VERSION, "时间范围": "", "旧窗口标识": "",
            "查询口径": MODE_ORDINAL,
        }, seed_only=True)
        built = self.build_index(index_row["id"])
        switched = self.switch_index_mode("IDX-DEMO", MODE_INTERVAL)
        queried = self.query_time_range(
            "IDX-DEMO", time_basis.format_ts(start),
            time_basis.format_ts(start + timedelta(minutes=30)))
        rolled = self.rollback_index_mode("IDX-DEMO")
        return {
            "point": point,
            "anchor": time_basis.format_ts(start),
            "products": {
                "legacy_a": {"window_id": "DOWN-DEMO-A", "grain_seconds": 60,
                             "ordinals": "1-12", "time_basis": time_basis.EVIDENCE_COUNT_ONLY},
                "legacy_b": {"window_id": "DOWN-DEMO-B", "grain_seconds": 30,
                             "ordinals": "1-24", "time_basis": time_basis.EVIDENCE_TIME_RANGE},
                "new": {"window_id": "DOWN-DEMO-N", "grain_seconds": 60,
                        "ordinals": "1-12", "time_basis": "产物自带窗口时间"},
                "rollup": {"window_id": "ROLL-DEMO", **rollup},
            },
            "build": built,
            "switch": switched,
            "query": {
                "matched": queried.get("matched"),
                "coverage": queried.get("coverage"),
                "window_ids": sorted({row["window_id"] for row in queried.get("windows", [])}),
            },
            "rollback": rolled,
        }

    def _reset_demo_row(
        self, module: str, values: dict[str, Any], *, seed_only: bool = False,
    ) -> dict[str, Any]:
        """演示素材可重复执行：同名记录先停用（不删除），再登记一条新的。"""
        key_field = (
            "索引编号" if "索引编号" in values else
            "窗口编号" if "窗口编号" in values else "任务编号"
        )
        for row in store.rows(module):
            if row.get(key_field) != values.get(key_field):
                continue
            if seed_only and row.get("测点编号") == values.get("测点编号"):
                return row
            if module == self.MODULE:
                row["status"] = "已失效"
            elif module == "rollup_window":
                row["status"] = "已作废"
            else:
                row["status"] = "已完成"
            row["pending"] = False
            row["superseded"] = True
            if seed_only:
                return row
        rows = store.rows(module)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update(values)
        entry["status"] = "草稿" if module in ("downsample", "rollup_window") else "草稿"
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry
