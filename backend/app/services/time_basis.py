"""时基解析与时间区间运算：新旧降采样产物共用的时间依据判定。

旧版降采样产物只有窗口序号（window_index），没有窗口开始/结束时刻。
要把它们放进新版的时间区间索引，必须先证明其绝对时基：

* 新版产物自带 window_start / window_end，属于自描述时基；
* 旧任务若登记了「采集开始时刻」+「窗口宽度」，可推算出每个窗口的时间区间；
* 旧任务若只有条数（或只有宽度没有锚点），无法证明绝对时基，只能按旧序号回读。

本模块只做纯计算，不依赖内存仓库，方便被索引、查询、迁移三个服务复用。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

UTC = timezone.utc

_DURATION_RE = re.compile(
    r"^(\d+(?:\.\d+)?)(ms|s|sec|秒|m|min|分钟|h|hr|小时|d|天)?$", re.IGNORECASE
)
_FREQ_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(hz|khz|赫兹|赫)?$", re.IGNORECASE)

_DURATION_FACTORS = {
    "ms": 0.001,
    "s": 1.0,
    "sec": 1.0,
    "秒": 1.0,
    "m": 60.0,
    "min": 60.0,
    "分钟": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "小时": 3600.0,
    "d": 86400.0,
    "天": 86400.0,
}

ANCHOR_FIELDS = ("采集开始时刻", "窗口起点", "起始时刻", "锚点时刻")
WIDTH_FIELDS = ("窗口宽度", "窗口长度")

EVIDENCE_TIME_RANGE = "采集开始时刻+窗口宽度"
EVIDENCE_COUNT_ONLY = "只有条数"
EVIDENCE_NEW_PRODUCT = "新版时间区间"


def parse_timestamp(value: Any) -> datetime | None:
    """把常见时间表示统一成 UTC 时区时间；无法识别时返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    if text.isdigit() and len(text) >= 10:
        number = float(text)
        if len(text) >= 13:  # 毫秒时间戳
            number /= 1000.0
        return parse_timestamp(number)
    if text.isdigit() and len(text) == 8:
        try:
            return datetime.strptime(text, "%Y%m%d").replace(tzinfo=UTC)
        except ValueError:
            return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
    if parsed is None:
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_duration_seconds(value: Any) -> float | None:
    """解析窗口宽度：支持 "10s"、"500ms"、"1min"、纯秒数等写法。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    match = _DURATION_RE.match(str(value).strip())
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "s").lower()
    factor = _DURATION_FACTORS.get(unit)
    if factor is None:
        return None
    seconds = number * factor
    return seconds if seconds > 0 else None


def parse_frequency_hz(value: Any) -> float | None:
    """解析采样频率：支持 "10Hz"、"1 kHz"、纯 Hz 数值。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    match = _FREQ_RE.match(str(value).strip())
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "hz").lower()
    if unit == "khz" or unit in ("千赫兹", "千赫"):
        number *= 1000.0
    return number if number > 0 else None


def parse_window_bound(value: Any) -> datetime | None:
    """只把显式时间串当成窗口边界。

    归并产物里的 window_start 是记录序号，数字不能被误解成时间戳，
    所以这里对纯数字一律返回 None，必须使用 *_time 字段。
    """
    if value is None or isinstance(value, bool) or isinstance(value, (int, float)):
        return None
    if str(value).strip().isdigit():
        return None
    return parse_timestamp(value)


def format_ts(moment: datetime | None) -> str | None:
    """统一输出带时区的 ISO 8601 时间串。"""
    if moment is None:
        return None
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def resolve_window_width(entry: dict[str, Any]) -> tuple[float | None, str | None]:
    """从任务登记字段里解析窗口宽度（秒）与其来源字段。"""
    for field in WIDTH_FIELDS:
        raw = entry.get(field)
        seconds = parse_duration_seconds(raw)
        if seconds is not None:
            return seconds, field
    src_freq = parse_frequency_hz(entry.get("源采样频率"))
    tgt_freq = parse_frequency_hz(entry.get("目标频率"))
    if src_freq and tgt_freq and src_freq >= tgt_freq:
        return src_freq / tgt_freq, "采源频率比"
    return None, None


def resolve_anchor(entry: dict[str, Any]) -> tuple[datetime | None, str | None]:
    """从任务登记字段里解析采集开始时刻（UTC）。"""
    for field in ANCHOR_FIELDS:
        raw = entry.get(field)
        moment = parse_timestamp(raw)
        if moment is not None:
            return moment, field
    return None, None


def declared_count(entry: dict[str, Any]) -> int | None:
    """读取旧任务登记的窗口条数，用于核对按宽度推算出的区间个数。"""
    for field in ("输出点数", "窗口条数", "任务水位", "归并水位"):
        raw = entry.get(field)
        try:
            count = int(float(str(raw)))
        except (TypeError, ValueError):
            continue
        if count >= 0:
            return count
    return None


def describe_legacy_evidence(entry: dict[str, Any]) -> dict[str, Any]:
    """判定旧任务的时间依据级别，给出可证明/不可证明的说明。"""
    anchor, anchor_field = resolve_anchor(entry)
    width, width_field = resolve_window_width(entry)
    explicit_width = width if width_field in WIDTH_FIELDS else None
    count = declared_count(entry)
    problems: list[str] = []
    if anchor is None:
        problems.append("缺少可解析的采集开始时刻")
    if width is None:
        problems.append("缺少可解析的窗口宽度")
    count_mismatch = False
    if anchor is not None and width is not None and count is not None:
        # 登记条数只能说明规模，不能替代锚点；条数对不上时单独提示，
        # 但窗口宽度本身仍可证明每个序号的时间区间。
        count_mismatch = count < 1
    provable = anchor is not None and width is not None
    evidence = EVIDENCE_TIME_RANGE if provable else EVIDENCE_COUNT_ONLY
    return {
        "provable": provable,
        "evidence": evidence,
        "anchor": format_ts(anchor),
        "anchor_field": anchor_field,
        "window_width_seconds": width,
        "explicit_width_seconds": explicit_width,
        "width_source": width_field,
        "declared_count": count,
        "count_mismatch": count_mismatch,
        "problems": [] if provable else problems,
    }


def legacy_window_interval(
    anchor: datetime,
    width_seconds: float,
    ordinal: int,
) -> tuple[datetime, datetime]:
    """按锚点 + 宽度 + 序号推出第 ordinal 个窗口的 [开始, 结束) 时间区间。"""
    if ordinal < 1:
        raise ValueError("窗口序号从 1 开始")
    start = anchor + timedelta(seconds=(ordinal - 1) * width_seconds)
    return start, start + timedelta(seconds=width_seconds)


def derive_legacy_intervals(
    entry: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    max_windows: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """为旧版降采样产物推算带时间区间的索引记录。

    只有锚点与宽度都可证明时才返回区间；否则返回空列表并附上原因。
    """
    evidence = describe_legacy_evidence(entry)
    if not evidence["provable"]:
        return [], evidence
    anchor = parse_timestamp(evidence["anchor"])
    width = float(evidence["window_width_seconds"])
    window_id = str(entry.get("任务编号", ""))
    intervals: list[dict[str, Any]] = []
    for position, record in enumerate(records, start=1):
        ordinal = record.get("window_index")
        try:
            ordinal_int = int(ordinal)
        except (TypeError, ValueError):
            ordinal_int = position
        if ordinal_int < 1:
            ordinal_int = position
        start, end = legacy_window_interval(anchor, width, ordinal_int)
        intervals.append({
            "window_id": window_id,
            "window_index": ordinal_int,
            "window_start": format_ts(start),
            "window_end": format_ts(end),
            "window_grain_seconds": width,
            "source_count": record.get("source_count"),
            "value": record.get("aggregated_value"),
            "method": record.get("method"),
            "origin": "legacy_derived",
        })
        if max_windows is not None and len(intervals) >= max_windows:
            break
    return intervals, evidence


def normalize_new_intervals(
    window_id: str,
    records: list[dict[str, Any]],
    *,
    point: str | None = None,
) -> list[dict[str, Any]]:
    """读取自带时间区间的新版降采样产物；缺开始或结束时刻的记录不算合格。"""
    normalized: list[dict[str, Any]] = []
    for position, record in enumerate(records, start=1):
        start = parse_window_bound(record.get("window_start"))
        end = parse_window_bound(record.get("window_end"))
        if start is None or end is None or end <= start:
            continue
        try:
            ordinal = int(record.get("window_index", position))
        except (TypeError, ValueError):
            ordinal = position
        normalized.append({
            "window_id": window_id,
            "window_index": ordinal,
            "window_start": format_ts(start),
            "window_end": format_ts(end),
            "window_grain_seconds": (
                record.get("window_grain_seconds")
                if record.get("window_grain_seconds")
                else (end - start).total_seconds()
            ),
            "source_count": record.get("source_count"),
            "value": record.get("aggregated_value"),
            "method": record.get("method"),
            "point": record.get("point", point),
            "origin": "new_product",
        })
    return normalized


def merge_intervals(
    intervals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """按时间开始排序并标记可当场重放的重叠关系（同序号不同时基不合并）。"""
    ordered = sorted(
        intervals,
        key=lambda item: (str(item.get("window_start", "")), str(item.get("window_id", "")),
                          int(item.get("window_index", 0))),
    )
    for position, item in enumerate(ordered):
        item["interval_index"] = position + 1
    return ordered


def coverage_summary(intervals: list[dict[str, Any]]) -> dict[str, Any]:
    """汇总一组时间区间的实际覆盖范围（只统计可证明的区间）。"""
    if not intervals:
        return {
            "window_count": 0,
            "start": None,
            "end": None,
            "point_equivalent": 0,
            "complete": False,
        }
    starts = [parse_timestamp(item["window_start"]) for item in intervals]
    ends = [parse_timestamp(item["window_end"]) for item in intervals]
    starts_ok = [item for item in starts if item is not None]
    ends_ok = [item for item in ends if item is not None]
    if not starts_ok or not ends_ok:
        return {
            "window_count": len(intervals),
            "start": None,
            "end": None,
            "point_equivalent": 0,
            "complete": False,
        }
    return {
        "window_count": len(intervals),
        "start": format_ts(min(starts_ok)),
        "end": format_ts(max(ends_ok)),
        "point_equivalent": sum(
            int(item.get("record_count") or item.get("source_count") or 1)
            for item in intervals
        ),
        "complete": True,
    }


def explicit_window_times(
    records: list[dict[str, Any]],
    fallback_width: float | None = None,
) -> tuple[datetime | None, datetime | None, float | None]:
    """从带 timestamp 的采样记录推出该窗口的 [开始, 结束) 与窗口宽度。

    取窗口内最早/最晚时间戳；结束时刻按相邻采样间隔外推到窗口边界，
    只有一个采样点时退回到登记宽度，仍不可知则返回单点区间。
    """
    moments = sorted({m for m in (parse_timestamp(r.get("timestamp")) for r in records) if m})
    if not moments:
        return None, None, fallback_width
    start = moments[0]
    last = moments[-1]
    step: float | None = None
    if len(moments) >= 2:
        deltas = sorted(
            (later - earlier).total_seconds()
            for earlier, later in zip(moments, moments[1:])
            if later > earlier
        )
        if deltas:
            step = deltas[len(deltas) // 2]
    if step is None:
        step = fallback_width
    end = last + timedelta(seconds=step) if step else last
    return start, end, step


def window_width_seconds(window: dict[str, Any]) -> float | None:
    """读取一条带时间区间记录的窗口宽度。"""
    start = parse_timestamp(window.get("window_start"))
    end = parse_timestamp(window.get("window_end"))
    if start is None or end is None or end <= start:
        return None
    return (end - start).total_seconds()


def group_ordinals(records: list[dict[str, Any]]) -> dict[str, Any]:
    """按旧窗口标识汇总序号范围，供按旧序号回读时展示。

    每个旧窗口标识单独成组：同序号但时基/粒度不同的旧产物不会被合并。
    """
    grouped: dict[str, dict[str, Any]] = {}
    for record in records:
        window_id = str(record.get("window_id", ""))
        bucket = grouped.setdefault(window_id, {
            "window_id": window_id,
            "point": record.get("point"),
            "ordinals": [],
            "record_count": 0,
            "time_known_ordinals": [],
            "time_grain_seconds": record.get("window_grain_seconds"),
            "time_basis": record.get("time_basis"),
        })
        try:
            ordinal = int(record.get("window_index", 0))
        except (TypeError, ValueError):
            ordinal = 0
        if ordinal:
            bucket["ordinals"].append(ordinal)
        bucket["record_count"] += 1
        if record.get("window_start") and record.get("window_end"):
            bucket["time_known_ordinals"].append(ordinal)
        if record.get("window_grain_seconds"):
            bucket["time_grain_seconds"] = record["window_grain_seconds"]
        if record.get("time_basis"):
            bucket["time_basis"] = record["time_basis"]
    for bucket in grouped.values():
        ordinals = sorted(bucket["ordinals"])
        bucket["ordinals"] = ordinals
        bucket["ordinal_range"] = f"{ordinals[0]}-{ordinals[-1]}" if ordinals else ""
        bucket["has_time_basis"] = bool(bucket["time_known_ordinals"])
        del bucket["time_known_ordinals"]
    return grouped


def detect_gaps(
    intervals: list[dict[str, Any]],
    width_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """按窗口宽度检查区间集合中的覆盖空洞与重叠。"""
    ordered = merge_intervals(intervals)
    gaps: list[dict[str, Any]] = []
    previous_end: datetime | None = None
    for item in ordered:
        start = parse_timestamp(item["window_start"])
        end = parse_timestamp(item["window_end"])
        if start is None or end is None:
            continue
        if previous_end is not None:
            if start > previous_end:
                gaps.append({
                    "kind": "gap",
                    "start": format_ts(previous_end),
                    "end": format_ts(start),
                    "seconds": round((start - previous_end).total_seconds(), 6),
                })
            elif start < previous_end:
                gaps.append({
                    "kind": "overlap",
                    "start": format_ts(start),
                    "end": format_ts(min(previous_end, end)),
                    "seconds": round((min(previous_end, end) - start).total_seconds(), 6),
                })
        if previous_end is None or end > previous_end:
            previous_end = end
    return gaps
