"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.sensor_point import SensorPointService
from app.services.edge_buffer import EdgeBufferService
from app.services.upload_batch import UploadBatchService
from app.services.unit_calibrate import UnitCalibrateService
from app.services.time_align import TimeAlignService
from app.services.gap_fill import GapFillService
from app.services.spike_filter import SpikeFilterService
from app.services.downsample import DownsampleService
from app.services.rollup_window import RollupWindowService
from app.services.series_index import SeriesIndexService
from app.services.tier_storage import TierStorageService
from app.services.device_health import DeviceHealthService
from app.services.ingest_watermark import IngestWatermarkService
from app.services.range_replay import RangeReplayService
from app.services.series_lineage import SeriesLineageService
from app.services.series_export import SeriesExportService
from app.services.series_quality import SeriesQualityService
from app.services.series_snapshot import SeriesSnapshotService
from app.services.range_query import RangeQueryService

SERVICES: dict[str, BaseModuleService] = {
    "sensor_point": SensorPointService(),
    "edge_buffer": EdgeBufferService(),
    "upload_batch": UploadBatchService(),
    "unit_calibrate": UnitCalibrateService(),
    "time_align": TimeAlignService(),
    "gap_fill": GapFillService(),
    "spike_filter": SpikeFilterService(),
    "downsample": DownsampleService(),
    "rollup_window": RollupWindowService(),
    "series_index": SeriesIndexService(),
    "tier_storage": TierStorageService(),
    "device_health": DeviceHealthService(),
    "ingest_watermark": IngestWatermarkService(),
    "range_replay": RangeReplayService(),
    "series_lineage": SeriesLineageService(),
    "series_export": SeriesExportService(),
    "series_quality": SeriesQualityService(),
    "series_snapshot": SeriesSnapshotService(),
    "range_query": RangeQueryService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
