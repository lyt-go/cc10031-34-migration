"""降采样窗口时间迁移测试：新版时间区间索引 + 旧序号回读 + 切换/回滚 + 查询受理。

覆盖题干要求的关键约束：

* 旧产物同序号但时基不同，不得按序号合并；
* 只有可证明的旧记录（采集开始时刻 + 窗口宽度）才进入新版时间索引；
* 索引切换前必须保留归并对历史数据的依赖；
* 一项查询在补证前已受理，交付前给部分覆盖与待补证项；
* 补证后按同一请求返回时间覆盖、可重放归并、仍可旧序号回读的范围；
* 用两份同序号不同粒度的旧产物 + 一份新版产物执行切换与回滚。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

T0 = "2026-09-01T10:00:00+00:00"


@pytest.fixture(autouse=True)
def data_dir():
    from app.seed import SEED_ROWS
    from app.store import store

    # store 是模块级单例：每个用例从种子重建，避免旧任务/旧请求跨用例泄漏
    saved = store._tables
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with patch("app.services.base.DATA_ROOT", root):
            import app.services.rollup_window as rw
            import app.services.series_index as si
            import app.services.range_query as rq
            for module in (rw, si, rq):
                module.DATA_ROOT = root
            try:
                yield root
            finally:
                store._tables = saved


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def seed_three_products(root: Path) -> None:
    """两份同序号、不同粒度（1min / 30s）的旧产物，加一份新版产物。"""
    legacy_a = [
        {"task_id": "DOWN-A", "point": "SENS-M", "window_index": i, "source_count": 10,
         "aggregated_value": round(1.0 + i, 4), "method": "均值", "target_freq": 1 / 60}
        for i in range(1, 6)
    ]
    legacy_b = [
        {"task_id": "DOWN-B", "point": "SENS-M", "window_index": i, "source_count": 5,
         "aggregated_value": round(2.0 + i, 4), "method": "均值", "target_freq": 1 / 30}
        for i in range(1, 6)
    ]
    new_prod = [
        {"task_id": "DOWN-N", "point": "SENS-M", "window_index": i,
         "window_start": f"2026-09-01T10:{i - 1:02d}:00+00:00",
         "window_end": f"2026-09-01T10:{i:02d}:00+00:00",
         "window_grain_seconds": 60, "time_basis": "explicit", "schema_version": "v2",
         "source_count": 10, "aggregated_value": round(3.0 + i, 4), "method": "均值"}
        for i in range(1, 6)
    ]
    for name, rows in (("DOWN-A", legacy_a), ("DOWN-B", legacy_b), ("DOWN-N", new_prod)):
        write_jsonl(root / "downsampled" / f"{name}_downsampled.jsonl", rows)


def add_downsample_task(task_id: str, **extra) -> None:
    from app.store import store
    row = {"id": abs(hash(task_id)) % 100000, "任务编号": task_id, "源序列": "SENS-M",
           "源采样频率": "1Hz", "目标频率": "0.0167Hz", "聚合方式": "均值",
           "输出点数": "5", "压缩比": "1", "任务水位": "5",
           "status": "已完成", "pending": False, "abnormal": False}
    row.update(extra)
    store.rows("downsample").append(row)


def add_index(index_no: str = "IDX-M") -> int:
    from app.store import store
    rows = store.rows("series_index")
    entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
             "索引编号": index_no, "测点编号": "SENS-M", "时间分区": "2026-09-01",
             "点区间": "0-0", "索引粒度": "1min", "索引条数": "0", "存储位置": "",
             "索引版本": "v2.0", "时间范围": "", "旧窗口标识": "", "查询口径": "旧序号",
             "status": "待构建", "pending": True, "abnormal": False}
    rows.append(entry)
    return entry["id"]


class TestTimeBasisParsing:
    def test_parse_duration_and_frequency(self):
        from app.services import time_basis
        assert time_basis.parse_duration_seconds("30s") == 30
        assert time_basis.parse_duration_seconds("500ms") == 0.5
        assert time_basis.parse_duration_seconds("2min") == 120
        assert time_basis.parse_frequency_hz("10Hz") == 10
        assert time_basis.parse_frequency_hz("1kHz") == 1000
        assert time_basis.parse_duration_seconds("不是宽度") is None

    def test_numeric_rollup_start_is_not_a_timestamp(self):
        from app.services import time_basis
        assert time_basis.parse_window_bound(3) is None
        assert time_basis.parse_window_bound("3") is None
        assert time_basis.parse_window_bound(T0) is not None

    def test_evidence_levels(self):
        from app.services import time_basis
        provable = time_basis.describe_legacy_evidence(
            {"采集开始时刻": T0, "窗口宽度": "1min", "输出点数": "5"})
        assert provable["provable"] is True
        assert provable["evidence"] == time_basis.EVIDENCE_TIME_RANGE
        count_only = time_basis.describe_legacy_evidence({"输出点数": "5"})
        assert count_only["provable"] is False
        assert count_only["evidence"] == time_basis.EVIDENCE_COUNT_ONLY
        assert count_only["problems"]


class TestNewIndexBuild:
    def test_ordinals_are_not_merged_across_time_bases(self, data_dir):
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-N")
        idx = add_index()
        result = SeriesIndexService().build_index(idx)
        assert result["interval_count"] == 5  # 只有新版产物有时间区间
        assert result["legacy_count"] == 15   # 三份产物都保留旧序号
        assert set(result["window_ids"]) == {"DOWN-A", "DOWN-B", "DOWN-N"}

        groups = SeriesIndexService().read_legacy_ordinals("IDX-M")["ordinal_groups"]
        # 同序号 1-5 出现三次，按旧窗口标识分组，各自保留自己的粒度
        assert len(groups) == 3
        assert groups["DOWN-A"]["ordinal_range"] == "1-5"
        assert groups["DOWN-B"]["ordinal_range"] == "1-5"
        assert groups["DOWN-A"]["time_basis"] == groups["DOWN-B"]["time_basis"] == "只有条数"
        assert groups["DOWN-N"]["has_time_basis"] is True

    def test_only_provable_legacy_records_enter_time_index(self, data_dir):
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-A", 采集开始时刻=T0, 窗口宽度="1min")
        add_downsample_task("DOWN-B", 输出点数="5")  # 只有条数
        add_downsample_task("DOWN-N")
        idx = add_index()
        SeriesIndexService().build_index(idx)
        manifest = SeriesIndexService().read_manifest("IDX-M")
        window_ids = {row["window_id"] for row in read_jsonl(data_dir / "series_index" / "IDX-M_interval.jsonl")}
        assert "DOWN-A" in window_ids  # 补证后可入时间索引
        assert "DOWN-B" not in window_ids  # 只有条数不得进入
        pending = {item["window_id"] for item in manifest["pending_evidence"]}
        assert pending == {"DOWN-B"}
        assert manifest["coverage"]["window_count"] == 10

    def test_rollup_dependency_preserved_for_history(self, data_dir):
        from app.services.series_index import SeriesIndexService
        from app.services.rollup_window import RollupWindowService
        from app.store import store
        seed_three_products(data_dir)
        add_downsample_task("DOWN-N")
        store.rows("rollup_window").append({
            "id": 7001, "窗口编号": "ROLL-M", "测点编号": "SENS-M",
            "窗口类型": "固定窗口", "窗口长度": "2", "滑动步长": "2",
            "聚合指标": "均值", "窗口条数": "0", "归并水位": "0",
            "status": "待归并", "pending": True, "abnormal": False})
        rollup = RollupWindowService().rollup_fixed_window(7001)
        assert rollup["windows_with_time"] == 3  # 5 个 1min 窗口按 2 个一组

        idx = add_index()
        result = SeriesIndexService().build_index(idx)
        # A/B 旧产物只有序号不进时间索引；5 个新版窗口 + 3 个按成员时间归并的窗口
        assert result["interval_count"] == 8
        manifest = SeriesIndexService().read_manifest("IDX-M")
        assert "ROLL-M" in manifest["rollup_windows"]


class TestSwitchAndRollback:
    def test_switch_blocks_when_rollup_dependency_missing(self, data_dir):
        """索引里没有归并窗口，但产物里存在归并历史 -> 拒绝切换。"""
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-N")
        idx = add_index()
        svc = SeriesIndexService()
        svc.build_index(idx)
        write_jsonl(data_dir / "rollup_windows" / "ROLL-ORPHAN_rollup.jsonl", [
            {"window_id": "ROLL-ORPHAN", "point": "SENS-M", "window_start": 1,
             "window_end": 2, "rollup_value": 1.0, "metric": "均值", "record_count": 2}])
        result = svc.switch_index_mode("IDX-M", "时间区间")
        assert "error" in result
        assert "ROLL-ORPHAN_rollup.jsonl" in result["missing_rollup_products"]

    def test_switch_then_rollback_keeps_ordinals_and_time(self, data_dir):
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-N")
        add_downsample_task("DOWN-A", 采集开始时刻=T0, 窗口宽度="1min")
        add_downsample_task("DOWN-B", 输出点数="5")
        idx = add_index()
        svc = SeriesIndexService()
        svc.build_index(idx)

        switched = svc.switch_index_mode("IDX-M", "时间区间")
        assert switched["to"] == "时间区间"
        assert switched["legacy_still_readable"] is True
        assert switched["interval_count"] == 10
        assert switched["legacy_count"] == 15
        # 切换到时间区间后，按时间能查到 A、N 两组窗口
        hit = svc.query_time_range("IDX-M", "2026-09-01T10:00:00Z", "2026-09-01T10:02:00Z")
        assert hit["matched"] > 0
        assert {row["window_id"] for row in hit["windows"]} == {"DOWN-A", "DOWN-N"}

        rolled = svc.rollback_index_mode("IDX-M")
        assert rolled["mode"] == "旧序号"
        assert rolled["legacy_records"] == 15
        # 回滚后仍能按旧序号定位，且明确列出失去时间依据的范围
        assert {item["window_id"] for item in rolled["ranges_without_time_basis"]} == {"DOWN-B"}
        assert rolled["interval_records_preserved"] == 10
        groups = rolled["ordinal_groups"]
        assert groups["DOWN-B"]["has_time_basis"] is False
        # 回滚不改产物，翻上去仍可再切回
        again = svc.switch_index_mode("IDX-M", "时间区间")
        assert again["interval_count"] == 10


class TestQueryLifecycle:
    def _make_request(self, **overrides) -> int:
        from app.store import store
        rows = store.rows("range_query")
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                 "请求编号": "REQ-001", "索引编号": "IDX-M", "测点编号": "SENS-M",
                 "查询开始": "2026-09-01T10:00:00Z", "查询结束": "2026-09-01T10:05:00Z",
                 "受理时刻": "", "核实时刻": "", "待补证项": "", "交付状态": "",
                 "status": "草稿", "pending": True, "abnormal": False}
        entry.update(overrides)
        rows.append(entry)
        return entry["id"]

    def test_accept_before_evidence_has_no_final_delivery(self, data_dir):
        from app.services.range_query import RangeQueryService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-B", 输出点数="5")
        add_downsample_task("DOWN-N")
        add_index()
        req = self._make_request()
        accepted = RangeQueryService().accept_request(req)
        assert accepted["deliverable"] is False
        # A 未登记时基、B 只有条数：两者都必须列为待补证
        assert {item["window_id"] for item in accepted["pending_evidence"]} == {"DOWN-A", "DOWN-B"}

    def test_partial_coverage_then_supplement_then_same_request(self, data_dir):
        from app.services.range_query import RangeQueryService
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-B", 输出点数="5")
        add_downsample_task("DOWN-N")
        idx = add_index()
        SeriesIndexService().build_index(idx)

        req = self._make_request(查询结束="2026-09-01T10:06:00Z")
        svc = RangeQueryService()
        accepted = svc.accept_request(req)
        assert accepted["deliverable"] is False

        first = svc.confirm_request(req)
        assert first["deliverable"] is True  # 有新版窗口可交付
        assert first["partial"] is True      # 但 10:05-10:06 无覆盖
        assert {item["window_id"] for item in first["pending_evidence"]} == {"DOWN-A", "DOWN-B"}
        assert first["query_range"][0].startswith("2026-09-01T10:00")
        legacy_ids = {item["window_id"] for item in first["legacy_ordinal_scope"]}
        assert {"DOWN-B", "DOWN-N"} <= legacy_ids

        verified = svc.verify_point_basis(
            req, window_id="DOWN-B", anchor="2026-09-01T10:00:00Z", window_width="30s")
        assert verified["updated"][0]["provable"] is True

        # 同一个请求确认交付：补证后 B 的时间覆盖应进入结果
        second = svc.confirm_request(req)
        assert second["deliverable"] is True
        covered_ids = {row["window_id"] for row in second["time_windows"]}
        assert {"DOWN-B", "DOWN-N"} <= covered_ids
        b_windows = [row for row in second["time_windows"] if row["window_id"] == "DOWN-B"]
        assert len(b_windows) == 5
        assert b_windows[0]["window_start"].startswith("2026-09-01T10:00")
        assert b_windows[0]["window_end"].startswith("2026-09-01T10:00:30")
        assert second["legacy_ordinal_scope"]  # 旧序号回读范围仍然给出

    def test_verified_interval_windows_are_query_eligible(self, data_dir):
        from app.services.range_query import RangeQueryService
        from app.services.series_index import SeriesIndexService
        seed_three_products(data_dir)
        add_downsample_task("DOWN-A", 采集开始时刻=T0, 窗口宽度="1min")
        add_downsample_task("DOWN-B", 输出点数="5")
        add_downsample_task("DOWN-N")
        idx = add_index()
        SeriesIndexService().build_index(idx)
        req = self._make_request(查询结束="2026-09-01T10:01:30Z")
        svc = RangeQueryService()
        svc.accept_request(req)
        # A(N) 两组同序号、同时段的窗口都能进入实际查询
        result = svc.confirm_request(req)
        ids = {row["window_id"] for row in result["time_windows"]}
        assert ids == {"DOWN-A", "DOWN-N"}
        for row in result["time_windows"]:
            assert row["query_eligible"] is True
            assert row["time_verified"] is True

    def test_replay_rebuilds_rollup_from_surviving_downsample(self, data_dir):
        from app.services.range_query import RangeQueryService
        from app.services.rollup_window import RollupWindowService
        from app.services.series_index import SeriesIndexService
        from app.store import store
        seed_three_products(data_dir)
        add_downsample_task("DOWN-N")
        store.rows("rollup_window").append({
            "id": 7002, "窗口编号": "ROLL-M", "测点编号": "SENS-M",
            "窗口类型": "固定窗口", "窗口长度": "2", "滑动步长": "2",
            "聚合指标": "均值", "窗口条数": "0", "归并水位": "0",
            "status": "待归并", "pending": True, "abnormal": False})
        rollup = RollupWindowService().rollup_fixed_window(7002)
        assert rollup["window_period_count"] == 3
        idx = add_index()
        SeriesIndexService().build_index(idx)

        req = self._make_request(查询结束="2026-09-01T10:05:00Z")
        svc = RangeQueryService()
        svc.accept_request(req)
        replayed = svc.replay_on_the_fly(req)
        assert replayed["member_windows"] == 5
        # 默认按来源窗口粒度成窗：5 个 60s 成员重放为 1 个归并窗口
        assert replayed["replayed_windows"] == 1
        assert replayed["coverage"]["start"].startswith("2026-09-01T10:00")
        assert replayed["coverage"]["end"].startswith("2026-09-01T10:05")
        first = replayed["windows"][0]
        assert first["member_count"] == 5
        # 旧 A/B 没有窗口时刻，不参与重放
        assert {item["window_id"] for item in replayed["windows"]} == {"DOWN-N"}

    def test_confirm_with_no_time_evidence_stays_pending(self, data_dir):
        from app.services.range_query import RangeQueryService
        from app.services.series_index import SeriesIndexService
        # 只有两份旧产物、没有新版时间区间
        write_jsonl(data_dir / "downsampled" / "DOWN-A_downsampled.jsonl", [
            {"task_id": "DOWN-A", "point": "SENS-M", "window_index": i,
             "source_count": 10, "aggregated_value": 1.0, "method": "均值"}
            for i in range(1, 6)])
        write_jsonl(data_dir / "downsampled" / "DOWN-B_downsampled.jsonl", [
            {"task_id": "DOWN-B", "point": "SENS-M", "window_index": i,
             "source_count": 5, "aggregated_value": 2.0, "method": "均值"}
            for i in range(1, 6)])
        add_downsample_task("DOWN-A", 输出点数="5")
        add_downsample_task("DOWN-B", 输出点数="5")
        add_index()
        SeriesIndexService().build_index(1)
        req = self._make_request()
        svc = RangeQueryService()
        svc.accept_request(req)
        result = svc.confirm_request(req)
        assert result["deliverable"] is False
        assert result["time_windows"] == []
        assert result["note"]
        assert {item["window_id"] for item in result["legacy_ordinal_scope"]} >= {"DOWN-A", "DOWN-B"}
