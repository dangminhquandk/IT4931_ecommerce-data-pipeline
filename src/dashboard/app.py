"""
Streamlit Visualization Dashboard (Core 4 - Visualization Layer).
Connects to the PostgreSQL Serving Layer (with automatic local SQLite fallback)
to display:
1. Lambda Architecture Overview & 5V Big Data Veracity Report
2. Batch Layer Analytics (Spark SQL Revenue, Top 10 Products, Order Status, MLlib RFM Segments)
3. Speed Layer Real-Time Monitoring (Kafka + Spark Structured Streaming 1m/10s Sliding Window & 10m Watermark)
4. SQL Benchmark & HDFS/Parquet/Snappy Storage Optimization Experiments (Proposal Section IV)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.ingestion.event_generator import ClickstreamEventGenerator
from src.serving.db_client import ServingDatabaseClient
from src.streaming.stream_processor import ClickstreamStreamProcessor

st.set_page_config(
    page_title="IT4931 - E-Commerce Big Data Pipeline (Group 3)",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db_client() -> ServingDatabaseClient:
    return ServingDatabaseClient()


def render_dashboard() -> None:
    db = get_db_client()

    # Sidebar
    with st.sidebar:
        st.markdown("## 🏛️ IT4931 - Nhóm 3")
        st.caption("**Học phần:** Lưu trữ và Xử lý Dữ liệu Lớn")
        st.caption("**GVHD:** TS. Vũ Tuyết Trinh")
        st.markdown("---")
        st.markdown(
            "**Thành viên Nhóm 3:**\n"
            "- Phạm Văn Quyết (`202416331`)\n"
            "- Vũ Việt Tiến (`202416362`)\n"
            "- Nguyễn Xuân Hồng Quân (`202416321`)\n"
            "- Đặng Minh Quân (`202416323`)"
        )
        st.markdown("---")
        st.markdown("### ⚙️ Cấu hình Kiến trúc")
        st.info(
            f"**Serving DB:** `{db.backend_name}`\n\n"
            "**HDFS Block Size:** `128 MB`\n\n"
            "**HDFS Replication:** `1` (Docker Local)\n\n"
            "**Parquet Codec:** `Snappy + Dictionary`\n\n"
            "**Sliding Window:** `1 phút (trượt 10 giây)`\n\n"
            "**Watermark:** `10 phút`"
        )

        if st.button("🔄 Sinh Luồng Clickstream Mới (Speed Layer)", use_container_width=True):
            gen = ClickstreamEventGenerator(seed=int(pd.Timestamp.now().timestamp()) % 10000)
            events = gen.generate_batch(count=1800, step_ms=100)
            proc = ClickstreamStreamProcessor(db_client=db)
            proc.process_clickstream_events_windowed(events)
            st.success("Đã cập nhật cửa sổ trượt 1m/10s từ luồng Clickstream!")
            st.rerun()

    st.title("🛒 Hệ Thống Đường Ống Dữ Liệu Thương Mại Điện Tử Thời Gian Thực")
    st.markdown(
        "**Mô hình tham chiếu:** `Lambda Architecture` | "
        "**Lưu trữ phân tán:** `HDFS (Raw CSV/JSON -> Refined Parquet/Snappy -> Partitioned)` | "
        "**Xử lý:** `Apache Spark (Batch SQL/MLlib & Structured Streaming)` | "
        "**Thu thập:** `Apache Kafka`"
    )

    rev_df = db.read_table("batch_revenue_summary")
    top10_df = db.read_table("batch_top_products")
    status_df = db.read_table("batch_order_status_summary")
    seg_df = db.read_table("batch_customer_segments")
    qual_df = db.read_table("data_quality_report")
    stream_df = db.read_table("streaming_window_metrics")
    stream_cat_df = db.read_table("streaming_category_metrics")
    bench_df = db.read_table("benchmark_results")
    storage_df = db.read_table("storage_comparison")
    rg_df = db.read_table("rowgroup_experiments")

    if rev_df.empty or bench_df.empty:
        st.warning(
            "Chưa có dữ liệu tổng hợp trong tầng Serving. Hãy chạy script `python scripts/run_local_demo.py` "
            "hoặc khởi động cụm Docker Compose để nạp dữ liệu thực nghiệm."
        )
        return

    # Top-level Merged KPI Cards (Lambda Serving Layer merging Batch & Speed views)
    total_batch_orders = int(rev_df["total_orders"].sum())
    total_batch_rev = float(rev_df["total_revenue"].sum())
    total_stream_events = int(stream_df["total_events"].sum()) if not stream_df.empty else 0
    latest_active_users = int(stream_df.iloc[-1]["active_users"]) if not stream_df.empty else 0
    max_speedup = float(bench_df["speedup_vs_csv"].max()) if not bench_df.empty else 1.0
    max_io_saved = float(bench_df["io_reduction_pct"].max()) if not bench_df.empty else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("📦 Tổng Đơn Hàng Lịch Sử (Batch)", f"{total_batch_orders:,}", "HDFS >1M Records")
    k2.metric("💰 Tổng Doanh Thu Thực Thực (Batch)", f"${total_batch_rev:,.2f}", "Completed Orders")
    k3.metric("⚡ Sự Kiện Clickstream (Speed)", f"{total_stream_events:,}", f"{latest_active_users} Active Users/min")
    k4.metric("🚀 Tăng Tốc Truy Vấn Parquet", f"{max_speedup:.2f}x", "So với CSV thô (Baseline)")
    k5.metric("📉 Giảm Dung Lượng Đọc I/O", f"{max_io_saved:.1f}%", "Partition Pruning + Pushdown")

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "🏛️ 1. Tổng Quan Hệ Thống & 5V Big Data",
            "📊 2. Phân Tích Khối (Batch Layer - Spark SQL & MLlib)",
            "⚡ 3. Giám Sát Luồng (Speed Layer - Kafka & Streaming)",
            "🔬 4. Thực Nghiệm Benchmark SQL & Tối Ưu HDFS/Parquet",
        ]
    )

    # =========================================================================
    # TAB 1: SYSTEM OVERVIEW & 5V DATA VERACITY
    # =========================================================================
    with tab1:
        st.subheader("Kiến Trúc Hệ Thống 4 Khối Cốt Lõi (Lambda Architecture)")
        c1, c2 = st.columns([3, 2])
        with c1:
            st.markdown(
                """
                Hệ thống triển khai đầy đủ 4 khối kiến trúc theo đề xuất kết hợp lý thuyết môn học **IT4931**:
                1. **Core 1 - Tầng Thu thập & Lưu trữ Phân tán (`Kafka` + `HDFS`)**:
                   - **Apache Kafka (`clickstream-events`)**: Tiếp nhận luồng sự kiện người dùng theo thời gian thực, áp dụng *Semantic Partitioning* theo `user_id` để bảo toàn thứ tự sự kiện trên từng Partition.
                   - **HDFS 3 Phân Tầng (`Block Size = 128MB`, `Replication = 1`)**:
                     - *Raw Data Layer*: Lưu trữ bất biến (*Immutable Append-Only / WORM*) CSV lịch sử (>1M bản ghi) và JSON Clickstream.
                     - *Refined Data Layer*: Chuẩn hóa kiểu dữ liệu, lọc nhiễu và chuyển đổi sang định dạng cột **Apache Parquet** nén **Snappy** + *Dictionary Encoding*.
                     - *Partitioned Data Layer*: Phân vùng Transactions theo `year=/month=/` và Clickstream theo `year=/month=/event_type=/`.
                2. **Core 2 - Tầng Xử lý Khối (`PySpark / Spark SQL & MLlib`)**:
                   - Tối ưu hóa truy vấn với *Spark Catalyst Optimizer* & *Cost-Based Optimizer (CBO)*.
                   - Tính toán doanh thu, đơn hàng, Top 10 sản phẩm bán chạy và phân cụm khách hàng **RFM K-Means (Spark MLlib)**.
                3. **Core 3 - Tầng Xử lý Luồng (`Spark Structured Streaming`)**:
                   - Xử lý theo *Event-Time* với **Cửa sổ trượt (Sliding Window: 1 phút, trượt 10 giây)** và **Watermarking (10 phút)** chống rò rỉ bộ nhớ *State Store*.
                4. **Core 4 - Tầng Phục vụ & Trực quan hóa (`PostgreSQL` + `Streamlit`)**:
                   - Hợp nhất *Batch Views* và *Real-time Speed Views* phục vụ truy vấn độ trễ thấp.
                """
            )
        with c2:
            if not storage_df.empty:
                fig_store = px.bar(
                    storage_df,
                    x="storage_layer",
                    y="total_size_mb",
                    color="format_name",
                    text="total_size_mb",
                    title="Dung Lượng Lưu Trữ Giữa 3 Tầng HDFS (MB)",
                    labels={"storage_layer": "Tầng lưu trữ", "total_size_mb": "Dung lượng (MB)"},
                )
                fig_store.update_traces(texttemplate="%{text:.2f} MB", textposition="outside")
                st.plotly_chart(fig_store, use_container_width=True)

        st.markdown("### 🛡️ Báo Cáo Độ Tin Cậy Dữ Liệu (Veracity - Tiền Xử Lý Raw -> Refined)")
        if not qual_df.empty:
            st.dataframe(qual_df, use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 2: BATCH LAYER ANALYTICS (SPARK SQL & MLLIB)
    # =========================================================================
    with tab2:
        st.subheader("Kết Quả Phân Tích Dữ Liệu Lịch Sử (>1,000,000 Giao Dịch)")
        monthly_df = (
            rev_df.groupby(["year", "month"], as_index=False)
            .agg(
                total_revenue=("total_revenue", "sum"),
                total_orders=("total_orders", "sum"),
                completed_orders=("completed_orders", "sum"),
            )
            .sort_values(["year", "month"])
        )
        monthly_df["period"] = (
            monthly_df["year"].astype(str) + "-" + monthly_df["month"].astype(str).str.zfill(2)
        )

        col_a, col_b = st.columns(2)
        with col_a:
            fig_rev = px.line(
                monthly_df,
                x="period",
                y="total_revenue",
                markers=True,
                title="Xu Hướng Doanh Thu Theo Tháng (Năm 2025)",
                labels={"period": "Tháng", "total_revenue": "Doanh thu ($)"},
            )
            st.plotly_chart(fig_rev, use_container_width=True)

        with col_b:
            cat_summary = (
                rev_df.groupby("category", as_index=False)
                .agg(total_revenue=("total_revenue", "sum"), total_orders=("total_orders", "sum"))
                .sort_values("total_revenue", ascending=True)
            )
            fig_cat = px.bar(
                cat_summary,
                x="total_revenue",
                y="category",
                orientation="h",
                color="total_orders",
                title="Doanh Thu & Số Đơn Hàng Theo Danh Mục Sản Phẩm",
                labels={"total_revenue": "Doanh thu ($)", "category": "Danh mục"},
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        col_c, col_d = st.columns([3, 2])
        with col_c:
            st.markdown("#### 🏆 Top 10 Sản Phẩm Bán Chạy Nhất (Spark SQL Join + Catalyst CBO)")
            if not top10_df.empty:
                fig_top10 = px.bar(
                    top10_df.sort_values("total_revenue", ascending=True),
                    x="total_revenue",
                    y="product_name",
                    orientation="h",
                    color="profit_margin_pct",
                    text="total_units_sold",
                    title="Top 10 Sản Phẩm Theo Doanh Thu & Biên Lợi Nhuận (%)",
                    labels={
                        "total_revenue": "Doanh thu ($)",
                        "product_name": "Sản phẩm",
                        "profit_margin_pct": "Biên LN (%)",
                    },
                )
                st.plotly_chart(fig_top10, use_container_width=True)
                st.dataframe(top10_df, use_container_width=True, hide_index=True)

        with col_d:
            st.markdown("#### 📌 Phân Bố Trạng Thái Đơn Hàng & Phân Khúc Khách Hàng (Spark MLlib)")
            if not status_df.empty:
                fig_status = px.pie(
                    status_df,
                    names="order_status",
                    values="order_count",
                    hole=0.42,
                    title="Tỷ Lệ Trạng Thái Đơn Hàng (%)",
                )
                st.plotly_chart(fig_status, use_container_width=True)

            if not seg_df.empty:
                fig_seg = px.bar(
                    seg_df,
                    x="segment_name",
                    y="share_of_revenue_pct",
                    color="avg_monetary_spend",
                    text="customer_count",
                    title="Phân Khúc Khách Hàng RFM (K-Means Clustering)",
                    labels={
                        "segment_name": "Phân khúc RFM",
                        "share_of_revenue_pct": "% Đóng góp Doanh thu",
                    },
                )
                st.plotly_chart(fig_seg, use_container_width=True)

    # =========================================================================
    # TAB 3: SPEED LAYER STREAMING MONITORING (KAFKA & STRUCTURED STREAMING)
    # =========================================================================
    with tab3:
        st.subheader("Giám Sát Luồng Clickstream Thời Gian Thực (Sliding Window: 1 phút, trượt 10 giây)")
        if not stream_df.empty:
            s_col1, s_col2 = st.columns(2)
            with s_col1:
                fig_win = go.Figure()
                fig_win.add_trace(
                    go.Scatter(
                        x=stream_df["window_start"],
                        y=stream_df["total_events"],
                        mode="lines+markers",
                        name="Tổng sự kiện / phút",
                    )
                )
                fig_win.add_trace(
                    go.Scatter(
                        x=stream_df["window_start"],
                        y=stream_df["active_users"],
                        mode="lines+markers",
                        name="Người dùng tương tác (Active Users)",
                    )
                )
                fig_win.add_trace(
                    go.Scatter(
                        x=stream_df["window_start"],
                        y=stream_df["purchase_count"],
                        mode="lines+markers",
                        name="Giao dịch mua hàng (Purchases)",
                    )
                )
                fig_win.update_layout(
                    title="Chỉ Số Cửa Sổ Trượt Thời Gian Thực (Sliding Window 1m / 10s)",
                    xaxis_title="Thời điểm bắt đầu cửa sổ (window_start)",
                    yaxis_title="Số lượng",
                )
                st.plotly_chart(fig_win, use_container_width=True)

            with s_col2:
                total_views = int(stream_df["view_count"].sum())
                total_carts = int(stream_df["cart_count"].sum())
                total_purchases = int(stream_df["purchase_count"].sum())
                fig_funnel = go.Figure(
                    go.Funnel(
                        y=["1. Xem sản phẩm (view)", "2. Thêm giỏ hàng (cart)", "3. Thanh toán (purchase)"],
                        x=[total_views, total_carts, total_purchases],
                        textinfo="value+percent initial",
                    )
                )
                fig_funnel.update_layout(title="Phễu Chuyển Đổi Clickstream Thời Gian Thực")
                st.plotly_chart(fig_funnel, use_container_width=True)

            w_col1, w_col2 = st.columns(2)
            with w_col1:
                fig_wm = go.Figure()
                fig_wm.add_trace(
                    go.Bar(
                        x=stream_df["window_start"],
                        y=stream_df["late_events_accepted"],
                        name="Late Data Được Chấp Nhận (<= 10m Watermark)",
                    )
                )
                fig_wm.add_trace(
                    go.Bar(
                        x=stream_df["window_start"],
                        y=stream_df["late_events_dropped"],
                        name="Late Data Bị Loại Bỏ (> 10m Watermark - State Cleanup)",
                    )
                )
                fig_wm.update_layout(
                    barmode="group",
                    title="Kiểm Chứng Cơ Chế Watermarking (10 Phút) Xử Lý Dữ Liệu Đến Muộn",
                    xaxis_title="Cửa sổ thời gian",
                    yaxis_title="Số sự kiện trễ (Late Events)",
                )
                st.plotly_chart(fig_wm, use_container_width=True)

            with w_col2:
                if not stream_cat_df.empty:
                    cat_ev = (
                        stream_cat_df.groupby(["category", "event_type"], as_index=False)["event_count"]
                        .sum()
                    )
                    fig_cat_ev = px.bar(
                        cat_ev,
                        x="category",
                        y="event_count",
                        color="event_type",
                        barmode="stack",
                        title="Phân Bố Sự Kiện Clickstream Theo Danh Mục & Loại Tương Tác",
                    )
                    st.plotly_chart(fig_cat_ev, use_container_width=True)

            st.markdown("#### 📋 Bảng Dữ Liệu Cửa Sổ Trượt Gần Nhất (`streaming_window_metrics`)")
            st.dataframe(stream_df.tail(15), use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 4: SQL BENCHMARK & STORAGE OPTIMIZATION (PROPOSAL SECTION IV)
    # =========================================================================
    with tab4:
        st.subheader(
            "Thực Nghiệm Đối Chứng 3 Mô Hình Lưu Trữ: (1) Raw CSV vs (2) Unpartitioned Parquet vs (3) Partitioned Parquet"
        )
        b1, b2 = st.columns(2)
        with b1:
            fig_time = px.bar(
                bench_df,
                x="query_id",
                y="execution_time_sec",
                color="storage_architecture",
                barmode="group",
                text="execution_time_sec",
                title="1. Thời Gian Thực Thi Truy Vấn - Execution Time (Giây - Thấp hơn là tốt hơn)",
                labels={"query_id": "Truy vấn Benchmark", "execution_time_sec": "Thời gian (s)"},
            )
            fig_time.update_traces(texttemplate="%{text:.3f}s", textposition="outside")
            st.plotly_chart(fig_time, use_container_width=True)

        with b2:
            fig_io = px.bar(
                bench_df,
                x="query_id",
                y="bytes_read_mb",
                color="storage_architecture",
                barmode="group",
                text="bytes_read_mb",
                title="2. Dung Lượng Dữ Liệu Quét - Scan Volume / Bytes Read (MB - Thấp hơn là tốt hơn)",
                labels={"query_id": "Truy vấn Benchmark", "bytes_read_mb": "Bytes Read (MB)"},
            )
            fig_io.update_traces(texttemplate="%{text:.2f}MB", textposition="outside")
            st.plotly_chart(fig_io, use_container_width=True)

        b3, b4 = st.columns(2)
        with b3:
            fig_res = px.bar(
                bench_df,
                x="query_id",
                y="ram_usage_mb",
                color="storage_architecture",
                barmode="group",
                title="3. Mức Tiêu Tốn Bộ Nhớ RAM Khi Thực Thi (MB)",
                labels={"query_id": "Truy vấn Benchmark", "ram_usage_mb": "RAM Usage (MB)"},
            )
            st.plotly_chart(fig_res, use_container_width=True)

        with b4:
            if not rg_df.empty:
                fig_rg = px.line(
                    rg_df,
                    x="row_group_size_mb",
                    y=["avg_query_time_sec", "write_time_sec"],
                    markers=True,
                    title="4. Thực Nghiệm Kích Thước Row Group (parquet.block.size: 32MB - 256MB)",
                    labels={"row_group_size_mb": "Row Group Size (MB)", "value": "Thời gian (s)"},
                )
                st.plotly_chart(fig_rg, use_container_width=True)

        st.markdown("#### 📊 Bảng Tổng Hợp Kết Quả Thực Nghiệm Benchmark SQL")
        st.dataframe(
            bench_df.drop(columns=["physical_plan_snippet"], errors="ignore"),
            use_container_width=True,
            hide_index=True,
        )

        if not rg_df.empty:
            st.markdown("#### 🧱 Bảng Thực Nghiệm Kích Thước Row Group (`parquet.block.size`) & Căn Chỉnh HDFS Block (128MB)")
            st.dataframe(rg_df, use_container_width=True, hide_index=True)

        st.markdown("#### 🔍 Kiểm Chứng Kế Hoạch Thực Thi Vật Lý (Spark Catalyst Physical Plan: PartitionFilters & PushedFilters)")
        for _, row in bench_df.iterrows():
            with st.expander(f"{row['query_id']} — {row['storage_architecture']} (Speedup: {row['speedup_vs_csv']}x | I/O Saved: {row['io_reduction_pct']}%)"):
                st.code(str(row["physical_plan_snippet"]), language="text")


if __name__ == "__main__":
    render_dashboard()

