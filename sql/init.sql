-- ============================================================================
-- IT4931 - LƯU TRỮ VÀ XỬ LÝ DỮ LIỆU LỚN (NHÓM 3)
-- SERVING LAYER DATABASE SCHEMA (PostgreSQL)
-- Reference: Lambda Architecture (Note 8) & Project Proposal Section III.4, IV
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. BATCH LAYER VIEWS (Pre-computed from Master Dataset on HDFS/Parquet)
-- ----------------------------------------------------------------------------

-- Doanh thu và số lượng đơn hàng theo Năm, Tháng và Danh mục (Category)
CREATE TABLE IF NOT EXISTS batch_revenue_summary (
    year INT NOT NULL,
    month INT NOT NULL,
    category VARCHAR(128) NOT NULL,
    total_orders BIGINT NOT NULL,
    completed_orders BIGINT NOT NULL,
    cancelled_orders BIGINT NOT NULL,
    total_revenue NUMERIC(18, 2) NOT NULL,
    avg_order_value NUMERIC(12, 2) NOT NULL,
    unique_customers BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month, category)
);

CREATE INDEX IF NOT EXISTS idx_batch_rev_ym ON batch_revenue_summary (year, month);
CREATE INDEX IF NOT EXISTS idx_batch_rev_cat ON batch_revenue_summary (category);

-- Top 10 sản phẩm bán chạy nhất (Best-Selling Products)
CREATE TABLE IF NOT EXISTS batch_top_products (
    rank_id INT PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(128) NOT NULL,
    category VARCHAR(128) NOT NULL,
    total_units_sold BIGINT NOT NULL,
    total_revenue NUMERIC(18, 2) NOT NULL,
    avg_price NUMERIC(12, 2) NOT NULL,
    profit_margin_pct NUMERIC(6, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Phân bố trạng thái đơn hàng lịch sử (Order Status Breakdown)
CREATE TABLE IF NOT EXISTS batch_order_status_summary (
    order_status VARCHAR(64) PRIMARY KEY,
    order_count BIGINT NOT NULL,
    percentage NUMERIC(6, 2) NOT NULL,
    total_value NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Phân khúc khách hàng RFM bằng Spark MLlib K-Means (Customer Segmentation)
CREATE TABLE IF NOT EXISTS batch_customer_segments (
    segment_id INT PRIMARY KEY,
    segment_name VARCHAR(64) NOT NULL,
    customer_count BIGINT NOT NULL,
    avg_recency_days NUMERIC(10, 2) NOT NULL,
    avg_frequency_orders NUMERIC(10, 2) NOT NULL,
    avg_monetary_spend NUMERIC(14, 2) NOT NULL,
    share_of_revenue_pct NUMERIC(6, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Báo cáo Chất lượng Dữ liệu (Veracity - Data Cleansing Raw -> Refined)
CREATE TABLE IF NOT EXISTS data_quality_report (
    check_name VARCHAR(128) PRIMARY KEY,
    raw_records_count BIGINT NOT NULL,
    invalid_records_filtered BIGINT NOT NULL,
    clean_records_retained BIGINT NOT NULL,
    defect_rate_pct NUMERIC(6, 3) NOT NULL,
    resolution_method VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. SPEED LAYER VIEWS (Real-Time Spark Structured Streaming + Watermarking)
-- Sliding Window: 1 minute, slide every 10 seconds; Watermark: 10 minutes
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS streaming_window_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_events BIGINT NOT NULL,
    active_users BIGINT NOT NULL,
    active_sessions BIGINT NOT NULL,
    view_count BIGINT NOT NULL,
    cart_count BIGINT NOT NULL,
    remove_cart_count BIGINT NOT NULL,
    purchase_count BIGINT NOT NULL,
    gross_sales NUMERIC(16, 2) NOT NULL,
    conversion_rate_pct NUMERIC(6, 2) NOT NULL,
    late_events_accepted BIGINT DEFAULT 0,
    late_events_dropped BIGINT DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_stream_win_start ON streaming_window_metrics (window_start DESC);

-- Thống kê thời gian thực theo loại sự kiện và danh mục
CREATE TABLE IF NOT EXISTS streaming_category_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    category VARCHAR(128) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_count BIGINT NOT NULL,
    total_value NUMERIC(16, 2) NOT NULL,
    PRIMARY KEY (window_start, window_end, category, event_type)
);

-- ----------------------------------------------------------------------------
-- 3. SQL BENCHMARK & STORAGE OPTIMIZATION EXPERIMENTS (Section IV)
-- ----------------------------------------------------------------------------

-- Kết quả thực nghiệm Benchmark SQL đối chứng 3 mô hình lưu trữ
CREATE TABLE IF NOT EXISTS benchmark_results (
    experiment_id SERIAL PRIMARY KEY,
    query_id VARCHAR(64) NOT NULL,
    query_name VARCHAR(255) NOT NULL,
    query_complexity VARCHAR(128) NOT NULL,
    storage_architecture VARCHAR(64) NOT NULL,
    execution_time_sec NUMERIC(10, 4) NOT NULL,
    bytes_read_mb NUMERIC(12, 3) NOT NULL,
    rows_scanned BIGINT NOT NULL,
    cpu_utilization_pct NUMERIC(6, 2) NOT NULL,
    ram_usage_mb NUMERIC(10, 2) NOT NULL,
    speedup_vs_csv NUMERIC(8, 2) NOT NULL,
    io_reduction_pct NUMERIC(6, 2) NOT NULL,
    predicate_pushdown_active BOOLEAN NOT NULL,
    partition_pruning_active BOOLEAN NOT NULL,
    physical_plan_snippet TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng đối chiếu dung lượng lưu trữ & tỷ lệ nén trên HDFS
CREATE TABLE IF NOT EXISTS storage_comparison (
    storage_layer VARCHAR(64) PRIMARY KEY,
    format_name VARCHAR(64) NOT NULL,
    compression_codec VARCHAR(32) NOT NULL,
    partition_strategy VARCHAR(128) NOT NULL,
    total_size_mb NUMERIC(12, 2) NOT NULL,
    compression_ratio NUMERIC(6, 2) NOT NULL,
    file_count INT NOT NULL,
    avg_file_size_mb NUMERIC(10, 2) NOT NULL,
    hdfs_block_size_mb INT DEFAULT 128,
    notes TEXT
);

-- Thực nghiệm ảnh hưởng của kích thước Row Group (parquet.block.size)
CREATE TABLE IF NOT EXISTS rowgroup_experiments (
    row_group_size_mb INT PRIMARY KEY,
    write_time_sec NUMERIC(10, 3) NOT NULL,
    storage_size_mb NUMERIC(12, 2) NOT NULL,
    avg_query_time_sec NUMERIC(10, 4) NOT NULL,
    scan_volume_mb NUMERIC(12, 3) NOT NULL,
    memory_peak_mb NUMERIC(10, 2) NOT NULL,
    hdfs_block_alignment VARCHAR(128) NOT NULL
);

