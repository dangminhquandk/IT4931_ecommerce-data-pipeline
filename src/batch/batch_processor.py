"""
Batch Processing Layer - PySpark / Spark SQL & Spark MLlib (Core 2).
Reference:
- Proposal Section III.2:
  - Optimization: Spark Catalyst Optimizer & Cost-Based Optimizer (CBO) for Logical & Physical Query Plans
  - Deep Analytics: Revenue, Order Count, Top 10 Best-Selling Products on >1M historical records
- IT4931 Projects.pptx & Note 8 (Lambda Architecture):
  - Pre-computes Batch Views from the immutable Master Dataset on HDFS/Parquet
  - Applies Machine Learning (Spark MLlib K-Means RFM Customer Segmentation) to discover hidden business insights
  - Persists Batch Views to the PostgreSQL Serving Layer
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

from src.config import HDFS_CONFIG, HDFSConfig
from src.serving.db_client import ServingDatabaseClient
from src.storage.hdfs_storage_manager import build_spark_session

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Executes Batch Layer aggregations and MLlib customer segmentation over the
    Refined/Partitioned Parquet dataset and writes Batch Views to PostgreSQL.
    """

    def __init__(
        self,
        hdfs_config: HDFSConfig = HDFS_CONFIG,
        db_client: ServingDatabaseClient | None = None,
    ) -> None:
        self.hdfs_config = hdfs_config
        self.db_client = db_client or ServingDatabaseClient()

    def run_spark_batch_analytics(self, spark: Any | None = None) -> Dict[str, pd.DataFrame]:
        """
        Execute Batch Layer analytics using Spark SQL (with Catalyst & CBO) and Spark MLlib K-Means.
        """
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import StandardScaler, VectorAssembler
        from pyspark.sql import functions as F

        own_spark = False
        if spark is None:
            spark = build_spark_session("IT4931-Batch-Layer-Analytics", self.hdfs_config, enable_cbo=True)
            own_spark = True

        try:
            tx_df = spark.read.parquet(self.hdfs_config.partitioned_transactions_path)
            prod_df = spark.read.parquet(self.hdfs_config.refined_products_parquet_path)

            tx_df.createOrReplaceTempView("transactions")
            prod_df.createOrReplaceTempView("products_catalog")

            # 1. Revenue & Order Volume by Year, Month, Category (Spark SQL)
            rev_sql = """
                SELECT
                    CAST(year AS INT) AS year,
                    CAST(month AS INT) AS month,
                    Category AS category,
                    COUNT(Order_ID) AS total_orders,
                    SUM(CASE WHEN Order_Status = 'Completed' THEN 1 ELSE 0 END) AS completed_orders,
                    SUM(CASE WHEN Order_Status IN ('Cancelled', 'Returned', 'Refunded') THEN 1 ELSE 0 END) AS cancelled_orders,
                    ROUND(SUM(CASE WHEN Order_Status = 'Completed' THEN Price ELSE 0.0 END), 2) AS total_revenue,
                    ROUND(AVG(CASE WHEN Order_Status = 'Completed' THEN Price ELSE NULL END), 2) AS avg_order_value,
                    COUNT(DISTINCT Customer_ID) AS unique_customers
                FROM transactions
                GROUP BY year, month, Category
                ORDER BY year, month, total_revenue DESC
            """
            rev_pdf = spark.sql(rev_sql).toPandas()

            # 2. Top 10 Best-Selling Products (Join transactions with products_catalog via Catalyst CBO)
            top10_sql = """
                SELECT
                    t.Product_ID AS product_id,
                    p.Product_Name AS product_name,
                    p.Brand AS brand,
                    t.Category AS category,
                    COUNT(t.Order_ID) AS total_units_sold,
                    ROUND(SUM(t.Price), 2) AS total_revenue,
                    ROUND(AVG(t.Price), 2) AS avg_price,
                    ROUND(AVG((t.Price - p.Base_Cost) / t.Price * 100.0), 2) AS profit_margin_pct
                FROM transactions t
                INNER JOIN products_catalog p ON t.Product_ID = p.Product_ID
                WHERE t.Order_Status = 'Completed'
                GROUP BY t.Product_ID, p.Product_Name, p.Brand, t.Category
                ORDER BY total_revenue DESC
                LIMIT 10
            """
            top10_pdf = spark.sql(top10_sql).toPandas()
            top10_pdf.insert(0, "rank_id", range(1, len(top10_pdf) + 1))

            # 3. Order Status Summary
            status_sql = """
                SELECT
                    Order_Status AS order_status,
                    COUNT(Order_ID) AS order_count,
                    ROUND(SUM(Price), 2) AS total_value
                FROM transactions
                GROUP BY Order_Status
                ORDER BY order_count DESC
            """
            status_pdf = spark.sql(status_sql).toPandas()
            total_orders_all = max(1, int(status_pdf["order_count"].sum()))
            status_pdf["percentage"] = (status_pdf["order_count"] * 100.0 / total_orders_all).round(2)
            status_pdf = status_pdf[["order_status", "order_count", "percentage", "total_value"]]

            # 4. Spark MLlib K-Means RFM Customer Segmentation
            max_date_row = spark.sql("SELECT MAX(TO_TIMESTAMP(Timestamp)) AS max_ts FROM transactions").collect()[0]
            ref_ts = max_date_row["max_ts"]

            rfm_df = (
                tx_df.filter(F.col("Order_Status") == "Completed")
                .withColumn("order_ts", F.to_timestamp("Timestamp"))
                .groupBy("Customer_ID")
                .agg(
                    F.datediff(F.lit(ref_ts), F.max("order_ts")).cast("double").alias("recency"),
                    F.count("Order_ID").cast("double").alias("frequency"),
                    F.sum("Price").cast("double").alias("monetary"),
                )
            )

            assembler = VectorAssembler(
                inputCols=["recency", "frequency", "monetary"],
                outputCol="raw_features",
            )
            scaler = StandardScaler(
                inputCol="raw_features",
                outputCol="features",
                withStd=True,
                withMean=True,
            )
            rfm_assembled = assembler.transform(rfm_df)
            scaler_model = scaler.fit(rfm_assembled)
            rfm_scaled = scaler_model.transform(rfm_assembled)

            kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=4, seed=42, maxIter=20)
            km_model = kmeans.fit(rfm_scaled)
            clustered = km_model.transform(rfm_scaled)

            seg_pdf = (
                clustered.groupBy("prediction")
                .agg(
                    F.count("Customer_ID").alias("customer_count"),
                    F.round(F.avg("recency"), 2).alias("avg_recency_days"),
                    F.round(F.avg("frequency"), 2).alias("avg_frequency_orders"),
                    F.round(F.avg("monetary"), 2).alias("avg_monetary_spend"),
                    F.sum("monetary").alias("total_seg_revenue"),
                )
                .toPandas()
            )
            seg_pdf = self._label_rfm_segments(seg_pdf)

            views = {
                "batch_revenue_summary": rev_pdf,
                "batch_top_products": top10_pdf,
                "batch_order_status_summary": status_pdf,
                "batch_customer_segments": seg_pdf,
            }
            self.persist_batch_views(views)
            return views
        finally:
            if own_spark:
                spark.stop()

    def run_local_batch_analytics(
        self,
        clean_tx_df: pd.DataFrame,
        products_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        Execute the exact same Batch Views (Revenue Summary, Top 10 Products with Join,
        Order Status Breakdown, and K-Means RFM Customer Segmentation) using Pandas/NumPy
        and persist to the Serving Layer.
        """
        logger.info("Computing Batch Views over %d cleaned transactions...", len(clean_tx_df))
        df = clean_tx_df.copy()
        if "year" not in df.columns or "month" not in df.columns:
            ts = pd.to_datetime(df["Timestamp"], errors="coerce")
            df["year"] = ts.dt.year
            df["month"] = ts.dt.month

        df["is_completed"] = (df["Order_Status"] == "Completed").astype(int)
        df["is_cancelled"] = df["Order_Status"].isin(["Cancelled", "Returned", "Refunded"]).astype(int)
        df["completed_price"] = np.where(df["Order_Status"] == "Completed", df["Price"], 0.0)

        # 1. Monthly & Category Revenue Summary
        grouped = df.groupby(["year", "month", "Category"], as_index=False).agg(
            total_orders=("Order_ID", "count"),
            completed_orders=("is_completed", "sum"),
            cancelled_orders=("is_cancelled", "sum"),
            total_revenue=("completed_price", "sum"),
            unique_customers=("Customer_ID", "nunique"),
        )
        grouped["avg_order_value"] = np.round(
            grouped["total_revenue"] / np.maximum(1, grouped["completed_orders"]), 2
        )
        grouped["total_revenue"] = grouped["total_revenue"].round(2)
        rev_pdf = grouped.rename(columns={"Category": "category"})[
            [
                "year",
                "month",
                "category",
                "total_orders",
                "completed_orders",
                "cancelled_orders",
                "total_revenue",
                "avg_order_value",
                "unique_customers",
            ]
        ].sort_values(["year", "month", "total_revenue"], ascending=[True, True, False])

        # 2. Top 10 Best-Selling Products (Joined with products_catalog)
        completed_df = df[df["Order_Status"] == "Completed"]
        prod_agg = (
            completed_df.groupby(["Product_ID", "Category"], as_index=False)
            .agg(
                total_units_sold=("Order_ID", "count"),
                total_revenue=("Price", "sum"),
                avg_price=("Price", "mean"),
            )
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )
        top10_merged = prod_agg.merge(
            products_df[["Product_ID", "Product_Name", "Brand", "Base_Cost"]],
            on="Product_ID",
            how="left",
        )
        top10_merged["profit_margin_pct"] = (
            (top10_merged["avg_price"] - top10_merged["Base_Cost"])
            / np.maximum(0.01, top10_merged["avg_price"])
            * 100.0
        ).round(2)
        top10_merged["total_revenue"] = top10_merged["total_revenue"].round(2)
        top10_merged["avg_price"] = top10_merged["avg_price"].round(2)
        top10_merged.insert(0, "rank_id", range(1, len(top10_merged) + 1))

        top10_pdf = top10_merged.rename(
            columns={
                "Product_ID": "product_id",
                "Product_Name": "product_name",
                "Brand": "brand",
                "Category": "category",
            }
        )[
            [
                "rank_id",
                "product_id",
                "product_name",
                "brand",
                "category",
                "total_units_sold",
                "total_revenue",
                "avg_price",
                "profit_margin_pct",
            ]
        ]

        # 3. Order Status Distribution
        status_pdf = (
            df.groupby("Order_Status", as_index=False)
            .agg(order_count=("Order_ID", "count"), total_value=("Price", "sum"))
            .rename(columns={"Order_Status": "order_status"})
            .sort_values("order_count", ascending=False)
        )
        total_cnt = max(1, int(status_pdf["order_count"].sum()))
        status_pdf["percentage"] = (status_pdf["order_count"] * 100.0 / total_cnt).round(2)
        status_pdf["total_value"] = status_pdf["total_value"].round(2)
        status_pdf = status_pdf[["order_status", "order_count", "percentage", "total_value"]]

        # 4. RFM Customer Segmentation (K-Means Clustering on Recency, Frequency, Monetary)
        ts_series = pd.to_datetime(completed_df["Timestamp"], errors="coerce")
        max_ts = ts_series.max()
        completed_with_days = completed_df.assign(
            days_ago=(max_ts - ts_series).dt.days.fillna(0).astype(float)
        )
        rfm = completed_with_days.groupby("Customer_ID", as_index=False).agg(
            recency=("days_ago", "min"),
            frequency=("Order_ID", "count"),
            monetary=("Price", "sum"),
        )
        # Standardized K-Means (k=4) in NumPy
        features = rfm[["recency", "frequency", "monetary"]].to_numpy(dtype=np.float64)
        mean = features.mean(axis=0)
        std = np.where(features.std(axis=0) == 0, 1.0, features.std(axis=0))
        z = (features - mean) / std

        rng = np.random.default_rng(42)
        centroids = z[rng.choice(len(z), size=min(4, len(z)), replace=False)]
        labels = np.zeros(len(z), dtype=int)
        for _ in range(15):
            dists = np.linalg.norm(z[:, None, :] - centroids[None, :, :], axis=2)
            labels = np.argmin(dists, axis=1)
            for k in range(len(centroids)):
                mask = labels == k
                if np.any(mask):
                    centroids[k] = z[mask].mean(axis=0)

        rfm["prediction"] = labels
        seg_raw = rfm.groupby("prediction", as_index=False).agg(
            customer_count=("Customer_ID", "count"),
            avg_recency_days=("recency", "mean"),
            avg_frequency_orders=("frequency", "mean"),
            avg_monetary_spend=("monetary", "mean"),
            total_seg_revenue=("monetary", "sum"),
        )
        seg_pdf = self._label_rfm_segments(seg_raw)

        views = {
            "batch_revenue_summary": rev_pdf,
            "batch_top_products": top10_pdf,
            "batch_order_status_summary": status_pdf,
            "batch_customer_segments": seg_pdf,
        }
        self.persist_batch_views(views)
        return views

    @staticmethod
    def _label_rfm_segments(seg_df: pd.DataFrame) -> pd.DataFrame:
        """Sort RFM clusters by average monetary spend descending and assign business tier labels."""
        seg_sorted = seg_df.sort_values("avg_monetary_spend", ascending=False).reset_index(drop=True)
        tier_names = [
            "Champions (VIP High-Spend)",
            "Loyal Customers (Frequent)",
            "Potential / Promising Buyers",
            "At-Risk / Hibernating Users",
        ]
        total_rev = max(1.0, float(seg_sorted["total_seg_revenue"].sum()))
        seg_sorted["segment_id"] = range(1, len(seg_sorted) + 1)
        seg_sorted["segment_name"] = [
            tier_names[i] if i < len(tier_names) else f"Segment {i + 1}"
            for i in range(len(seg_sorted))
        ]
        seg_sorted["avg_recency_days"] = seg_sorted["avg_recency_days"].round(2)
        seg_sorted["avg_frequency_orders"] = seg_sorted["avg_frequency_orders"].round(2)
        seg_sorted["avg_monetary_spend"] = seg_sorted["avg_monetary_spend"].round(2)
        seg_sorted["share_of_revenue_pct"] = (
            seg_sorted["total_seg_revenue"] * 100.0 / total_rev
        ).round(2)
        return seg_sorted[
            [
                "segment_id",
                "segment_name",
                "customer_count",
                "avg_recency_days",
                "avg_frequency_orders",
                "avg_monetary_spend",
                "share_of_revenue_pct",
            ]
        ]

    def persist_batch_views(self, views: Dict[str, pd.DataFrame]) -> None:
        """Persist all computed Batch Views to the PostgreSQL / SQLite Serving Layer."""
        for table_name, df in views.items():
            self.db_client.write_dataframe(df, table_name, if_exists="replace")
            logger.info("Persisted Batch View '%s' (%d rows) to Serving Layer.", table_name, len(df))

