"""
Historical Batch Data Loader & Synthetic >1M E-Commerce Dataset Generator (Core 1).
Reference:
- Proposal Section II.1: Kaggle E-Commerce dataset (>1M transaction records:
  Order_ID, Customer_ID, Product_ID, Price, Category, Order_Status, Timestamp)
- IT4931 Projects.pptx & Note 1: Big Data 5V (Volume > 1M rows, Variety CSV + JSON,
  Veracity handling missing data, irregular structure, duplicates)
- IT4931 Note 3 (HDFS): Consolidating records into large files aligned with 128MB HDFS blocks
  to avoid the HDFS Small Files Problem on NameNode memory.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from src.config import LOCAL_DATA_DIR

logger = logging.getLogger(__name__)

CATEGORIES = [
    "electronics.smartphone",
    "electronics.audio.headphone",
    "electronics.video.tv",
    "computers.notebook",
    "computers.peripherals.monitor",
    "appliances.kitchen.refrigerators",
    "appliances.environment.vacuum",
    "apparel.shoes",
    "apparel.costume",
    "furniture.living_room.sofa",
    "sport.bicycle",
    "accessories.bag",
]

BRANDS_BY_CATEGORY = {
    "electronics.smartphone": ["Samsung", "Apple", "Xiaomi", "Oppo", "Vivo"],
    "electronics.audio.headphone": ["Sony", "JBL", "Bose", "Apple", "Sennheiser"],
    "electronics.video.tv": ["LG", "Samsung", "Sony", "TCL", "Panasonic"],
    "computers.notebook": ["Dell", "ASUS", "Lenovo", "HP", "Apple"],
    "computers.peripherals.monitor": ["LG", "Dell", "AOC", "Samsung", "ViewSonic"],
    "appliances.kitchen.refrigerators": ["Panasonic", "Hitachi", "Samsung", "LG", "Electrolux"],
    "appliances.environment.vacuum": ["Xiaomi", "Dyson", "Philips", "Deerma", "Roborock"],
    "apparel.shoes": ["Nike", "Adidas", "Puma", "NewBalance", "Converse"],
    "apparel.costume": ["Zara", "Uniqlo", "H&M", "Mango", "Lacoste"],
    "furniture.living_room.sofa": ["IKEA", "Ashley", "Index", "JYSK", "Uma"],
    "sport.bicycle": ["Giant", "Trek", "Specialized", "Trinx", "Merida"],
    "accessories.bag": ["Samsonite", "Coach", "Herschel", "Anello", "Tumi"],
}

PRICE_RANGES = {
    "electronics.smartphone": (180.0, 1499.0),
    "electronics.audio.headphone": (25.0, 399.0),
    "electronics.video.tv": (250.0, 2200.0),
    "computers.notebook": (450.0, 2800.0),
    "computers.peripherals.monitor": (110.0, 890.0),
    "appliances.kitchen.refrigerators": (320.0, 1850.0),
    "appliances.environment.vacuum": (65.0, 750.0),
    "apparel.shoes": (35.0, 240.0),
    "apparel.costume": (18.0, 160.0),
    "furniture.living_room.sofa": (220.0, 1600.0),
    "sport.bicycle": (150.0, 1400.0),
    "accessories.bag": (28.0, 420.0),
}

ORDER_STATUSES = ["Completed", "Processing", "Cancelled", "Returned", "Refunded"]
ORDER_STATUS_WEIGHTS = [0.76, 0.08, 0.09, 0.04, 0.03]
SUPPLIER_REGIONS = ["Asia-Pacific", "North America", "Europe", "Domestic-VN"]


def generate_products_catalog(num_products: int = 2000, seed: int = 42) -> pd.DataFrame:
    """
    Generate Product Catalog dimension table (`products_catalog.csv`)
    to support multi-table JOIN queries in the SQL Benchmark (Proposal Section IV).
    """
    rng = np.random.default_rng(seed)
    product_ids = [f"PROD_{i:05d}" for i in range(1, num_products + 1)]
    cat_weights = np.array([0.22, 0.12, 0.07, 0.14, 0.06, 0.05, 0.07, 0.09, 0.06, 0.03, 0.04, 0.05])
    cat_weights = cat_weights / cat_weights.sum()

    chosen_cats = rng.choice(CATEGORIES, size=num_products, p=cat_weights)
    brands = []
    names = []
    base_costs = []
    list_prices = []
    regions = rng.choice(SUPPLIER_REGIONS, size=num_products, p=[0.45, 0.20, 0.15, 0.20])

    for idx, cat in enumerate(chosen_cats):
        brand = rng.choice(BRANDS_BY_CATEGORY[cat])
        low, high = PRICE_RANGES[cat]
        price = round(float(rng.uniform(low, high)), 2)
        cost_ratio = float(rng.uniform(0.55, 0.82))
        base_cost = round(price * cost_ratio, 2)
        sub_name = cat.split(".")[-1].capitalize()
        brands.append(brand)
        names.append(f"{brand} {sub_name} Series-{idx + 100}")
        list_prices.append(price)
        base_costs.append(base_cost)

    return pd.DataFrame(
        {
            "Product_ID": product_ids,
            "Product_Name": names,
            "Brand": brands,
            "Category": chosen_cats,
            "List_Price": list_prices,
            "Base_Cost": base_costs,
            "Supplier_Region": regions,
        }
    )


def generate_historical_transactions(
    num_records: int = 1_050_000,
    num_customers: int = 85_000,
    products_df: pd.DataFrame | None = None,
    inject_dirty_rate: float = 0.012,
    seed: int = 42,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Generate >1,000,000 historical e-commerce transaction records matching the exact schema
    in Proposal Section II.1:
      (Order_ID, Customer_ID, Product_ID, Price, Category, Order_Status, Timestamp)
    Includes realistic Pareto product popularity, seasonal monthly volume, and a controlled
    rate of dirty data (missing Category, invalid negative Price, duplicate Order_ID) to
    demonstrate Veracity data cleaning at the HDFS Refined Layer.
    """
    rng = np.random.default_rng(seed)
    if products_df is None:
        products_df = generate_products_catalog(seed=seed)

    num_products = len(products_df)
    # Zipf/Pareto popularity weights so Top 10 best-selling products stand out clearly
    ranks = np.arange(1, num_products + 1, dtype=np.float64)
    prod_weights = 1.0 / np.power(ranks, 0.85)
    prod_weights /= prod_weights.sum()

    prod_indices = rng.choice(num_products, size=num_records, p=prod_weights)
    selected_products = products_df.iloc[prod_indices].reset_index(drop=True)

    order_ids = np.array([f"ORD_{i:08d}" for i in range(1, num_records + 1)], dtype=object)
    cust_ids = np.array(
        [f"CUST_{i:06d}" for i in rng.integers(1, num_customers + 1, size=num_records)],
        dtype=object,
    )

    # Price variation around product list price (discounts / promos)
    price_multiplier = rng.uniform(0.85, 1.10, size=num_records)
    prices = np.round(selected_products["List_Price"].to_numpy() * price_multiplier, 2)
    categories = selected_products["Category"].to_numpy(dtype=object).copy()
    statuses = rng.choice(ORDER_STATUSES, size=num_records, p=ORDER_STATUS_WEIGHTS)

    # Generate timestamps across 2025-01-01 to 2025-12-31 with Q4 holiday surge
    month_weights = np.array([0.06, 0.06, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.09, 0.10, 0.12, 0.11])
    month_weights /= month_weights.sum()
    months = rng.choice(np.arange(1, 13), size=num_records, p=month_weights)
    days = rng.integers(1, 29, size=num_records)
    hours = rng.integers(0, 24, size=num_records)
    minutes = rng.integers(0, 60, size=num_records)
    seconds = rng.integers(0, 60, size=num_records)

    base_dates = pd.to_datetime(
        {
            "year": np.full(num_records, 2025),
            "month": months,
            "day": days,
            "hour": hours,
            "minute": minutes,
            "second": seconds,
        }
    )
    timestamps = base_dates.dt.strftime("%Y-%m-%d %H:%M:%S").to_numpy(dtype=object)

    # Inject controlled Veracity issues (missing Category, negative Price, duplicate Order_ID)
    n_dirty = int(num_records * inject_dirty_rate)
    n_missing_cat = n_dirty // 3
    n_invalid_price = n_dirty // 3
    n_dup_orders = n_dirty - n_missing_cat - n_invalid_price

    if n_dirty > 0:
        dirty_indices = rng.choice(num_records, size=n_dirty, replace=False)
        cat_idx = dirty_indices[:n_missing_cat]
        price_idx = dirty_indices[n_missing_cat : n_missing_cat + n_invalid_price]
        dup_idx = dirty_indices[n_missing_cat + n_invalid_price :]

        categories[cat_idx] = None
        prices[price_idx] = -np.abs(prices[price_idx])
        order_ids[dup_idx] = order_ids[np.maximum(0, dup_idx - 1)]

    df = pd.DataFrame(
        {
            "Order_ID": order_ids,
            "Customer_ID": cust_ids,
            "Product_ID": selected_products["Product_ID"].to_numpy(),
            "Price": prices,
            "Category": categories,
            "Order_Status": statuses,
            "Timestamp": timestamps,
        }
    )

    stats = {
        "total_generated": int(num_records),
        "missing_category_injected": int(n_missing_cat),
        "invalid_price_injected": int(n_invalid_price),
        "duplicate_order_id_injected": int(n_dup_orders),
    }
    return df, stats


def convert_kaggle_csv_to_transactions(
    kaggle_csv_path: Path,
    output_csv_path: Path,
    max_rows: int | None = None,
) -> Path:
    """
    Convert raw Kaggle Multi-Category Store CSV
    (`event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session`)
    into the standardized Proposal transaction schema:
    `Order_ID,Customer_ID,Product_ID,Price,Category,Order_Status,Timestamp`.
    """
    logger.info("Converting Kaggle dataset from %s...", kaggle_csv_path)
    df = pd.read_csv(kaggle_csv_path, nrows=max_rows)
    if "Order_ID" in df.columns and "Timestamp" in df.columns:
        # Already in Proposal schema
        df.to_csv(output_csv_path, index=False)
        return output_csv_path

    status_map = {
        "purchase": "Completed",
        "cart": "Processing",
        "remove_from_cart": "Cancelled",
        "view": "Completed",
    }
    n = len(df)
    out = pd.DataFrame(
        {
            "Order_ID": [f"ORD_{i:08d}" for i in range(1, n + 1)],
            "Customer_ID": df["user_id"].astype(str).radd("CUST_"),
            "Product_ID": df["product_id"].astype(str).radd("PROD_"),
            "Price": pd.to_numeric(df["price"], errors="coerce").fillna(0.0),
            "Category": df.get("category_code", pd.Series(["electronics.smartphone"] * n)).fillna(
                "accessories.bag"
            ),
            "Order_Status": df.get("event_type", pd.Series(["purchase"] * n)).map(
                lambda x: status_map.get(str(x), "Completed")
            ),
            "Timestamp": pd.to_datetime(df["event_time"], errors="coerce")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .fillna("2025-10-15 12:00:00"),
        }
    )
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv_path, index=False)
    logger.info("Saved standardized transactions CSV (%d rows) to %s", len(out), output_csv_path)
    return output_csv_path


def prepare_local_raw_datasets(
    num_records: int = 1_050_000,
    output_dir: Path | None = None,
    force_regenerate: bool = False,
) -> Tuple[Path, Path, Dict[str, int]]:
    """
    Ensure `transactions.csv` and `products_catalog.csv` exist in `data/raw/`.
    Returns `(transactions_csv_path, products_csv_path, generation_stats)`.
    """
    raw_dir = output_dir or (LOCAL_DATA_DIR / "raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    tx_path = raw_dir / "transactions_csv" / "transactions.csv"
    prod_path = raw_dir / "products_csv" / "products_catalog.csv"
    tx_path.parent.mkdir(parents=True, exist_ok=True)
    prod_path.parent.mkdir(parents=True, exist_ok=True)

    if tx_path.exists() and prod_path.exists() and not force_regenerate:
        logger.info("Using existing raw datasets at %s", raw_dir)
        return tx_path, prod_path, {"total_generated": num_records}

    logger.info("Generating products catalog and %d historical transactions...", num_records)
    products_df = generate_products_catalog(num_products=2000)
    products_df.to_csv(prod_path, index=False)

    tx_df, stats = generate_historical_transactions(
        num_records=num_records,
        products_df=products_df,
    )
    tx_df.to_csv(tx_path, index=False)
    logger.info(
        "Generated %d transactions (%.2f MB) at %s",
        len(tx_df),
        tx_path.stat().st_size / (1024 * 1024),
        tx_path,
    )
    return tx_path, prod_path, stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Prepare Historical E-Commerce Batch Dataset")
    parser.add_argument("--rows", type=int, default=1_050_000, help="Number of transaction rows")
    parser.add_argument("--kaggle-csv", type=str, default=None, help="Optional path to Kaggle CSV")
    parser.add_argument("--force", action="store_true", help="Force regeneration")
    args = parser.parse_args()

    if args.kaggle_csv:
        target = LOCAL_DATA_DIR / "raw" / "transactions_csv" / "transactions.csv"
        convert_kaggle_csv_to_transactions(Path(args.kaggle_csv), target, max_rows=args.rows)
    else:
        prepare_local_raw_datasets(num_records=args.rows, force_regenerate=args.force)

