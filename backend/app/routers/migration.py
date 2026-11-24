"""迁移与查询专用路由：时间区间索引、切换回滚、旧序号回读与查询受理。

通用资源路由（/api/{module}）仍然负责列表/登记/状态动作；
这里只挂载需要额外参数的领域操作，避免把业务判断塞进通用路由。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import SERVICES

router = APIRouter(prefix="/api", tags=["迁移与查询"])


def _index_service():
    return SERVICES["series_index"]


def _query_service():
    return SERVICES["range_query"]


class IndexRef(BaseModel):
    index_id: str | int


class ModePayload(BaseModel):
    mode: str = "时间区间"


class TimeRangeQueryPayload(BaseModel):
    start: str
    end: str
    limit: int = Field(default=500, ge=1, le=5000)


class OrdinalQueryPayload(BaseModel):
    window_id: str | None = None
    start_ordinal: int | None = Field(default=None, ge=1)
    end_ordinal: int | None = Field(default=None, ge=1)
    limit: int = Field(default=500, ge=1, le=5000)


class AcceptPayload(BaseModel):
    pass


class VerifyPayload(BaseModel):
    window_id: str | None = None
    anchor: str | None = None
    window_width: str | None = None


class ReplayPayload(BaseModel):
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=500, ge=1, le=5000)
    metric: str = "均值"
    window_length: int | None = Field(default=None, ge=1)


def _fail(result: dict[str, Any]) -> dict[str, Any]:
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/series_index/{index_id}/manifest")
def index_manifest(index_id: str) -> dict[str, Any]:
    """查看索引构建清单：覆盖率、时间粒度、旧窗口标识与待补证项。"""
    return _fail(_index_service().index_manifest(index_id))


@router.post("/series_index/{index_id}/query/time")
def query_time_range(index_id: str, payload: TimeRangeQueryPayload) -> dict[str, Any]:
    """按时间区间查询新版索引，返回实际覆盖范围与是否部分覆盖。"""
    return _fail(_index_service().query_time_range(
        index_id, payload.start, payload.end, limit=payload.limit))


@router.post("/series_index/{index_id}/query/ordinals")
def query_ordinals(index_id: str, payload: OrdinalQueryPayload) -> dict[str, Any]:
    """按旧窗口标识 + 旧序号回读，同序号不同时基的旧窗口分组返回。"""
    return _fail(_index_service().read_legacy_ordinals(
        index_id,
        window_id=payload.window_id,
        start_ordinal=payload.start_ordinal,
        end_ordinal=payload.end_ordinal,
        limit=payload.limit,
    ))


@router.post("/series_index/{index_id}/switch")
def switch_mode(index_id: str, payload: ModePayload) -> dict[str, Any]:
    """切换索引查询口径，切换前校验归并历史依赖仍在索引中。"""
    return _fail(_index_service().switch_index_mode(index_id, payload.mode))


@router.post("/series_index/{index_id}/rollback")
def rollback_mode(index_id: str) -> dict[str, Any]:
    """回滚到旧序号口径，返回可定位窗口与失去时间依据的历史范围。"""
    return _fail(_index_service().rollback_index_mode(index_id))


@router.get("/series_index/{index_id}/legacy")
def legacy_scope(index_id: str) -> dict[str, Any]:
    """只读展示旧序号索引全部分组（不依赖当前查询口径）。"""
    return _fail(_index_service().read_legacy_ordinals(index_id, limit=5000))


# ---------------------------------------------------------------------- #
# 区间查询请求：受理 -> 核实 -> 确认
# ---------------------------------------------------------------------- #
@router.post("/range_query/{request_id}/accept")
def accept_request(request_id: int) -> dict[str, Any]:
    """受理查询请求；此时只给出待补证项，不承诺可交付结果。"""
    return _fail(_query_service().accept_request(request_id))


@router.post("/range_query/{request_id}/verify")
def verify_basis(request_id: int, payload: VerifyPayload) -> dict[str, Any]:
    """补证测点时基（采集开始时刻 + 窗口宽度），可只补指定旧窗口。"""
    return _fail(_query_service().verify_point_basis(
        request_id,
        window_id=payload.window_id,
        anchor=payload.anchor,
        window_width=payload.window_width,
    ))


@router.post("/range_query/{request_id}/confirm")
def confirm_request(request_id: int) -> dict[str, Any]:
    """按同一请求确认交付：时间覆盖 + 可重放归并 + 旧序号可回读范围。"""
    return _fail(_query_service().confirm_request(request_id))


@router.post("/range_query/{request_id}/replay")
def replay_rollups(request_id: int, payload: ReplayPayload) -> dict[str, Any]:
    """当场重放：按查询请求命中的归并窗口重新聚合，并回报耗时与覆盖。"""
    return _fail(_query_service().replay_on_the_fly(
        request_id,
        offset=payload.offset,
        limit=payload.limit,
        metric=payload.metric,
        window_length=payload.window_length,
    ))


@router.post("/series_index/migration/seed-demo")
def seed_migration_demo(point: str = "SENS-DEMO") -> dict[str, Any]:
    """构造迁移验证素材（两份同序号不同粒度旧产物 + 新版产物）并跑一遍切换/回滚。"""
    return _fail(_index_service().seed_migration_demo(point))
