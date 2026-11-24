"""时间区间索引查询受理：先受理、后核实测点时基、再出可交付结果。

一次查询的生命周期：

1. ``受理``：记录请求的测点与查询区间，结果未定（verbatim 未核实前不允许交付）；
2. ``核实``：补证测点时基（采集开始时刻 + 窗口宽度），或确认某个旧窗口的时基；
3. ``确认``：按同一个请求返回三部分——实际找到的时间覆盖、归并结果里
   可当场重放的数据、仍只能按旧序号回读的范围。

已核实的旧窗口时基与新版本产物自带的时间区间具备同等查询资格。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import time_basis
from app.services.base import BaseModuleService
from app.store import store

REQUEST_PENDING = "待补证"
REQUEST_READY = "可交付"
REQUEST_REJECTED = "无法证明"


class RangeQueryService(BaseModuleService):
    MODULE = "range_query"
    ENTITY = "区间查询请求"
    OPERATION_TERM = "查询"
    LIST_FIELDS = [
        "请求编号", "索引编号", "测点编号", "查询开始", "查询结束",
        "受理时刻", "核实时刻", "待补证项", "交付状态",
    ]
    REQUIRED_FIELDS = ["请求编号", "索引编号", "测点编号", "查询开始", "查询结束"]
    STATUS_ORDER = ["已受理", "已核实", "已交付", "无法证明"]
    ACTION_RULES = {"受理": "已受理", "核实": "已核实", "确认交付": "已交付",
                    "标记无法证明": "无法证明"}
    NEGATIVE_ACTIONS = ["标记无法证明"]

    # ------------------------------------------------------------------ #
    # 受理
    # ------------------------------------------------------------------ #
    def accept_request(self, http_request_id: int) -> dict[str, Any]:
        """受理一次时间区间查询：只落请求与待补证项，不承诺可交付结果。"""
        request = store.find(self.MODULE, http_request_id)
        if request is None:
            return {"error": f"区间查询请求 {http_request_id} 不存在"}
        self.run_action(http_request_id, "受理")
        begin = time_basis.parse_timestamp(request.get("查询开始"))
        finish = time_basis.parse_timestamp(request.get("查询结束"))
        problems: list[str] = []
        if begin is None or finish is None or finish <= begin:
            problems.append("查询区间无法解析或起止颠倒")
        index_svc = self._index_service()
        index_entry = index_svc.resolve_index(request.get("索引编号"))
        if index_entry is None:
            problems.append(f"索引 {request.get('索引编号')} 不存在")
        manifest = index_svc.read_manifest(request.get("索引编号")) if index_entry else None
        pending = list((manifest or {}).get("pending_evidence", []))
        if not pending:
            # 索引里没有待补证项（可能尚未构建），按产物与任务登记逐项判断时基
            pending.extend(self._product_pending(index_svc, str(request.get("测点编号"))))
        request["待补证项"] = "；".join(
            f"{item.get('window_id')}:{item.get('reason', '')}" for item in pending
        )
        request["受理时刻"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        request["交付状态"] = REQUEST_PENDING if pending else REQUEST_READY
        if problems:
            request["交付状态"] = REQUEST_REJECTED
            request["待补证项"] = "；".join(problems)
        self._log_request(request, "受理", {
            "pending_count": len(pending),
            "query_range": [request.get("查询开始"), request.get("查询结束")],
        })
        return {
            "request_id": request["请求编号"],
            "status": request["交付状态"],
            "deliverable": request["交付状态"] == REQUEST_READY,
            "pending_evidence": pending,
            "problems": problems,
        }

    # ------------------------------------------------------------------ #
    # 核实
    # ------------------------------------------------------------------ #
    def verify_point_basis(
        self,
        request_id: int,
        *,
        window_id: str | None = None,
        anchor: Any = None,
        window_width: Any = None,
    ) -> dict[str, Any]:
        """补证测点时基：把采集开始时刻/窗口宽度写回旧任务，或只核实指定旧窗口。"""
        request = store.find(self.MODULE, request_id)
        if request is None:
            return {"error": f"区间查询请求 {request_id} 不存在"}
        if request.get("交付状态") == REQUEST_REJECTED:
            return {"error": "请求已标记无法证明，不能再补证"}
        anchor_dt = time_basis.parse_timestamp(anchor) if anchor is not None else None
        width_s = time_basis.parse_duration_seconds(window_width) if window_width is not None else None
        if anchor is not None and anchor_dt is None:
            return {"error": f"采集开始时刻无法解析：{anchor}"}
        if window_width is not None and width_s is None:
            return {"error": f"窗口宽度无法解析：{window_width}"}

        point = str(request.get("测点编号"))
        updated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for task in store.rows("downsample"):
            if str(task.get("源序列")) != point:
                continue
            if window_id is not None and str(task.get("任务编号")) != str(window_id):
                continue
            evidence_before = time_basis.describe_legacy_evidence(task)
            if evidence_before["provable"] and window_id is None:
                skipped.append({"window_id": task.get("任务编号"), "reason": "已有可证明时基"})
                continue
            if anchor_dt is not None:
                task["采集开始时刻"] = time_basis.format_ts(anchor_dt)
            if width_s is not None:
                task["窗口宽度"] = f"{width_s:g}s"
            evidence_after = time_basis.describe_legacy_evidence(task)
            updated.append({
                "window_id": task.get("任务编号"),
                "provable": evidence_after["provable"],
                "evidence": evidence_after["evidence"],
            })
        if not updated and not skipped:
            return {"error": f"测点 {point} 没有可补证的旧降采样任务"}

        request["核实时刻"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        request["交付状态"] = REQUEST_READY
        request["待补证项"] = ""
        self.run_action(request_id, "核实")
        self._log_request(request, "核实时基", {
            "window_id": window_id,
            "anchor": time_basis.format_ts(anchor_dt),
            "window_width_seconds": width_s,
            "updated": updated,
            "skipped": skipped,
        })
        return {
            "request_id": request["请求编号"],
            "point": point,
            "updated": updated,
            "skipped": skipped,
            "status": request["交付状态"],
        }

    # ------------------------------------------------------------------ #
    # 确认交付
    # ------------------------------------------------------------------ #
    def confirm_request(self, request_id: int) -> dict[str, Any]:
        """按同一请求重算并交付：时间覆盖 + 可当场重放的归并 + 仍可旧序号回读的范围。"""
        request = store.find(self.MODULE, request_id)
        if request is None:
            return {"error": f"区间查询请求 {request_id} 不存在"}
        index_no = str(request.get("索引编号", ""))
        index_svc = self._index_service()
        index_entry = index_svc.resolve_index(index_no)
        if index_entry is None:
            return {"error": f"索引 {index_no} 不存在"}
        point = str(request.get("测点编号"))
        begin = time_basis.parse_timestamp(request.get("查询开始"))
        finish = time_basis.parse_timestamp(request.get("查询结束"))
        if begin is None or finish is None or finish <= begin:
            return {"error": "查询区间无法解析或起止颠倒"}

        # 1) 按当前证据重建索引（幂等覆盖），再按同一请求查时间覆盖
        built = index_svc.build_index(index_entry["id"])
        if "error" in built:
            return built
        coverage_result = index_svc.query_time_range(
            index_no, request.get("查询开始"), request.get("查询结束"))
        if "error" in coverage_result:
            return coverage_result

        # 2) 归并结果中可当场重放的数据：窗口时间能覆盖查询区间且产物仍在
        replayable = self._replayable_rollups(index_svc, point, begin, finish)

        # 3) 仍只能按旧序号回读的范围（含索引内与索引外的旧降采样产物）
        legacy = self._legacy_scope(index_svc, point)

        manifest = index_svc.read_manifest(index_no) or {}
        pending = list(manifest.get("pending_evidence", []))
        deliverable = coverage_result["matched"] > 0 or bool(replayable)
        request["交付状态"] = REQUEST_READY if deliverable else (
            REQUEST_PENDING if pending else request.get("交付状态"))
        request["待补证项"] = "；".join(
            f"{item.get('window_id')}:{item.get('reason', '')}" for item in pending)
        self.run_action(request_id, "确认交付" if deliverable else "核实")
        self._log_request(request, "确认交付", {
            "matched": coverage_result["matched"],
            "replayable_rollups": len(replayable),
            "legacy_windows": len(legacy),
            "pending": len(pending),
        })
        return {
            "request_id": request["请求编号"],
            "point": point,
            "query_range": coverage_result["query_range"],
            "deliverable": deliverable,
            "status": request["交付状态"],
            "coverage": coverage_result["coverage"],
            "partial": coverage_result["partial"],
            "time_windows": coverage_result["windows"],
            "replayable_rollups": replayable,
            "legacy_ordinal_scope": legacy,
            "pending_evidence": pending,
            "note": (
                "部分覆盖：命中窗口只覆盖查询区间的一部分，其余范围见待补证项。"
                if coverage_result["partial"] else
                ("" if deliverable else "尚无可证明的时间覆盖，仅可按旧序号回读。")
            ),
        }

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def _replayable_rollups(
        self, index_svc, point: str, begin, finish,
    ) -> list[dict[str, Any]]:
        """找出可当场重放的归并窗口：时间区间与查询相交且原始降采样成员仍在。"""
        results: list[dict[str, Any]] = []
        ds_dir = index_svc.data_dir("downsampled")
        for path in sorted(index_svc.data_dir("rollup_windows").glob("*_rollup.jsonl")):
            records = index_svc.read_jsonl(path)
            for record in records:
                if str(record.get("point", "")) != point:
                    continue
                start = time_basis.parse_window_bound(record.get("window_start_time"))
                end = time_basis.parse_window_bound(record.get("window_end_time"))
                if start is None or end is None or start >= finish or end <= begin:
                    continue
                members = self._member_windows(index_svc, ds_dir, point, record)
                results.append({
                    "rollup_id": record.get("window_id"),
                    "point": point,
                    "window_index": record.get("window_start"),
                    "window_start": time_basis.format_ts(start),
                    "window_end": time_basis.format_ts(end),
                    "rollup_value": record.get("rollup_value"),
                    "metric": record.get("metric"),
                    "record_count": record.get("record_count"),
                    "replayable": members["complete"],
                    "member_count": members["found"],
                    "missing_members": members["missing"],
                })
        return results

    def _member_windows(
        self, index_svc, ds_dir, point: str, record: dict[str, Any],
    ) -> dict[str, Any]:
        """核对归并窗口覆盖的降采样窗口是否都还在产物里。"""
        start = time_basis.parse_window_bound(record.get("window_start_time"))
        end = time_basis.parse_window_bound(record.get("window_end_time"))
        found = 0
        missing: list[str] = []
        for path in sorted(ds_dir.glob("*_downsampled.jsonl")):
            for member in index_svc.read_jsonl(path):
                if str(member.get("point", "")) != point:
                    continue
                m_start = time_basis.parse_window_bound(member.get("window_start"))
                m_end = time_basis.parse_window_bound(member.get("window_end"))
                if m_start is None or m_end is None:
                    continue
                if m_start >= start and m_end <= end:
                    found += 1
                elif m_start < end and m_end > start:
                    missing.append(f"{member.get('task_id')}#{member.get('window_index')}")
        return {"complete": not missing and found > 0, "found": found, "missing": missing}

    def _legacy_scope(self, index_svc, point: str) -> list[dict[str, Any]]:
        """列出该测点仍可按旧序号回读的旧产物范围（不受索引构建影响）。"""
        scope: list[dict[str, Any]] = []
        for path in sorted(index_svc.data_dir("downsampled").glob("*_downsampled.jsonl")):
            records = index_svc.read_jsonl(path)
            if not records or str(records[0].get("point", "")) != point:
                continue
            window_id = str(records[0].get("task_id") or path.stem.removesuffix("_downsampled"))
            ordinals = [index_svc.ordinal_of(r, i + 1) for i, r in enumerate(records)]
            timed = sum(1 for r in records
                        if time_basis.parse_window_bound(r.get("window_start"))
                        and time_basis.parse_window_bound(r.get("window_end")))
            scope.append({
                "source": "downsample",
                "window_id": window_id,
                "ordinal_range": index_svc.ordinal_range(ordinals),
                "record_count": len(records),
                "windows_with_time": timed,
                "legacy_readable": True,
                "time_provable": timed == len(records),
            })
        for path in sorted(index_svc.data_dir("rollup_windows").glob("*_rollup.jsonl")):
            records = index_svc.read_jsonl(path)
            if not records or str(records[0].get("point", "")) != point:
                continue
            window_id = str(records[0].get("window_id") or path.stem.removesuffix("_rollup"))
            ordinals = [index_svc.ordinal_of(r, i + 1) for i, r in enumerate(records)]
            timed = sum(1 for r in records
                        if time_basis.parse_window_bound(r.get("window_start_time")))
            scope.append({
                "source": "rollup_window",
                "window_id": window_id,
                "ordinal_range": index_svc.ordinal_range(ordinals),
                "record_count": len(records),
                "windows_with_time": timed,
                "legacy_readable": True,
                "time_provable": timed == len(records),
            })
        return scope

    @staticmethod
    def _index_service():
        from app.services import SERVICES
        return SERVICES["series_index"]

    def _log_request(self, request: dict[str, Any], action: str,
                     detail: dict[str, Any]) -> None:
        self._write_jsonl(
            self._data_dir("range_queries") / f"{request['请求编号']}.jsonl",
            [{"request_id": request["请求编号"], "action": action,
              "status": request.get("交付状态"), "at": time.time(), **detail}],
        )

    # ------------------------------------------------------------------ #
    # 当场重放
    # ------------------------------------------------------------------ #
    def replay_on_the_fly(
        self,
        request_id: int,
        *,
        offset: int = 0,
        limit: int = 500,
        metric: str = "均值",
        window_length: int | None = None,
    ) -> dict[str, Any]:
        """按已受理请求的区间，从仍在的降采样产物重新聚合出归并结果。

        重放不依赖旧归并产物本身：只读取带窗口时刻的降采样成员，
        因此即使归并产物只有序号，也能重建出可交付的时间序列。
        """
        request = store.find(self.MODULE, request_id)
        if request is None:
            return {"error": f"区间查询请求 {request_id} 不存在"}
        if request.get("交付状态") == REQUEST_REJECTED:
            return {"error": "请求已标记无法证明，不能重放"}
        begin = time_basis.parse_timestamp(request.get("查询开始"))
        finish = time_basis.parse_timestamp(request.get("查询结束"))
        if begin is None or finish is None or finish <= begin:
            return {"error": "查询区间无法解析或起止颠倒"}
        point = str(request.get("测点编号"))
        index_svc = self._index_service()
        members: list[dict[str, Any]] = []
        for path in sorted(index_svc.data_dir("downsampled").glob("*_downsampled.jsonl")):
            for record in index_svc.read_jsonl(path):
                if str(record.get("point", "")) != point:
                    continue
                start = time_basis.parse_window_bound(record.get("window_start"))
                end = time_basis.parse_window_bound(record.get("window_end"))
                if start is None or end is None or start >= finish or end <= begin:
                    continue
                members.append({"record": record, "start": start, "end": end})
        members.sort(key=lambda item: (item["start"], str(item["record"].get("task_id")),
                                       int(item["record"].get("window_index", 0))))
        # 默认按来源窗口自身的粒度成窗（不把不同时基混进同一个窗口）
        buckets: dict[float, dict[str, list[dict[str, Any]]]] = {}
        if window_length and window_length > 0:
            for index in range(0, len(members), window_length):
                key = float(index)
                buckets.setdefault(key, {"__mixed__": []})["__mixed__"].extend(
                    members[index:index + window_length])
        else:
            for item in members:
                grain = item["record"].get("window_grain_seconds") or round(
                    (item["end"] - item["start"]).total_seconds(), 6)
                buckets.setdefault(grain, {}).setdefault(
                    str(item["record"].get("task_id")), []).append(item)
        replayed: list[dict[str, Any]] = []
        for grain, groups in sorted(buckets.items(), key=lambda kv: kv[0]):
            for window_id, items in groups.items():
                items = sorted(items, key=lambda item: item["start"])
                values = []
                for item in items:
                    try:
                        values.append(float(item["record"].get("aggregated_value", 0)))
                    except (TypeError, ValueError):
                        values.append(0.0)
                if metric in ("求和", "sum"):
                    value = round(sum(values), 4)
                elif metric in ("最大值", "max"):
                    value = round(max(values), 4)
                elif metric in ("最小值", "min"):
                    value = round(min(values), 4)
                else:
                    value = round(sum(values) / max(len(values), 1), 4)
                replayed.append({
                    "window_id": window_id,
                    "window_grain_seconds": grain,
                    "window_start": time_basis.format_ts(min(i["start"] for i in items)),
                    "window_end": time_basis.format_ts(max(i["end"] for i in items)),
                    "member_count": len(items),
                    "members": [
                        {"task_id": i["record"].get("task_id"),
                         "window_index": i["record"].get("window_index")}
                        for i in items
                    ],
                    "value": value,
                    "metric": metric,
                    "origin": "replayed",
                })
        page = replayed[offset:offset + limit]
        self._log_request(request, "当场重放", {
            "member_count": len(members),
            "replayed_windows": len(replayed),
            "offset": offset,
            "limit": limit,
            "metric": metric,
        })
        return {
            "request_id": request["请求编号"],
            "point": point,
            "query_range": [time_basis.format_ts(begin), time_basis.format_ts(finish)],
            "member_windows": len(members),
            "replayed_windows": len(replayed),
            "offset": offset,
            "limit": limit,
            "truncated": offset + limit < len(replayed),
            "windows": page,
            "coverage": time_basis.coverage_summary([
                {"window_start": item["window_start"], "window_end": item["window_end"],
                 "record_count": item["member_count"]} for item in replayed
            ]),
            "note": "仅对带窗口时刻的降采样成员重放；只有序号的旧产物不参与。",
        }

    def _product_pending(self, index_svc, point: str) -> list[dict[str, Any]]:
        """逐产物判断是否缺乏可证明时基，作为受理时的待补证项。"""
        pending: list[dict[str, Any]] = []
        tasks = {
            str(row.get("任务编号")): row for row in store.rows("downsample")
            if str(row.get("源序列")) == point
        }
        for path in sorted(index_svc.data_dir("downsampled").glob("*_downsampled.jsonl")):
            records = index_svc.read_jsonl(path)
            if not records or str(records[0].get("point", "")) != point:
                continue
            window_id = str(records[0].get("task_id")
                            or path.stem.removesuffix("_downsampled"))
            timed = any(
                time_basis.parse_window_bound(r.get("window_start")) is not None
                and time_basis.parse_window_bound(r.get("window_end")) is not None
                for r in records
            )
            evidence = time_basis.describe_legacy_evidence(tasks.get(window_id, {}))
            if timed or evidence["provable"]:
                continue
            pending.append({
                "window_id": window_id,
                "point": point,
                "source": "downsample",
                "ordinal_range": index_svc.ordinal_range(
                    [index_svc.ordinal_of(r, i + 1) for i, r in enumerate(records)]),
                "record_count": len(records),
                "reason": "；".join(evidence.get("problems") or ["只有条数"]),
            })
        for path in sorted(index_svc.data_dir("rollup_windows").glob("*_rollup.jsonl")):
            records = index_svc.read_jsonl(path)
            if not records or str(records[0].get("point", "")) != point:
                continue
            timed = any(time_basis.parse_window_bound(r.get("window_start_time")) is not None
                        for r in records)
            if timed:
                continue
            window_id = str(records[0].get("window_id")
                            or path.stem.removesuffix("_rollup"))
            pending.append({
                "window_id": window_id,
                "point": point,
                "source": "rollup_window",
                "ordinal_range": index_svc.ordinal_range(
                    [index_svc.ordinal_of(r, i + 1) for i, r in enumerate(records)]),
                "record_count": len(records),
                "reason": "归并窗口起点为记录序号，缺乏时间依据",
            })
        return pending
