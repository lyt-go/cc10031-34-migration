<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">工业传感器时序采集与降采样平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向工业传感器高频时序数据的测点接入、边缘缓冲上送、单位标定、时间对齐、降采样归并与分层归档的采集平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "测点台账", path: "/sensor_point" }, { label: "边缘缓冲", path: "/edge_buffer" }, { label: "批量上送", path: "/upload_batch" }, { label: "单位标定", path: "/unit_calibrate" }, { label: "时间对齐", path: "/time_align" }, { label: "断点填补", path: "/gap_fill" }, { label: "尖峰剔除", path: "/spike_filter" }, { label: "降采样", path: "/downsample" }, { label: "窗口归并", path: "/rollup_window" }, { label: "序列索引", path: "/series_index" }, { label: "冷热分层", path: "/tier_storage" }, { label: "采集心跳", path: "/device_health" }, { label: "接入水位", path: "/ingest_watermark" }, { label: "区间重采", path: "/range_replay" }, { label: "采集血缘", path: "/series_lineage" }, { label: "序列导出", path: "/series_export" }, { label: "时序质量", path: "/series_quality" }, { label: "序列快照", path: "/series_snapshot" }, { label: "区间查询", path: "/range_query" }]
</script>
