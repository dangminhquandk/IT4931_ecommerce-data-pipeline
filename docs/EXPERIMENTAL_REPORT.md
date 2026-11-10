# 📊 BÁO CÁO KẾT QUẢ THỰC NGHIỆM HỆ THỐNG ĐƯỜNG ỐNG DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ THỜI GIAN THỰC

- **Trường:** Đại học Bách Khoa Hà Nội — Trường Công nghệ Thông tin và Truyền thông (SoICT)
- **Học phần:** IT4931 — Lưu trữ và Xử lý Dữ liệu Lớn
- **Giảng viên hướng dẫn:** TS. Vũ Tuyết Trinh
- **Nhóm thực hiện:** Nhóm 3
  - Phạm Văn Quyết — `202416331`
  - Vũ Việt Tiến — `202416362`
  - Nguyễn Xuân Hồng Quân — `202416321`
  - Đặng Minh Quân — `202416323`

---

## I. TỔNG QUAN MÔI TRƯỜNG & DỮ LIỆU THỰC NGHIỆM

Hệ thống được triển khai theo mô hình **Lambda Architecture** gồm 4 khối cốt lõi (Thu thập & Lưu trữ HDFS/Parquet, Xử lý Khối bằng Spark SQL/MLlib, Xử lý Luồng bằng Kafka & Spark Structured Streaming, và Tầng Phục vụ/Trực quan hóa bằng PostgreSQL & Streamlit).

### 1. Tập dữ liệu sử dụng trong thực nghiệm
1. **Dữ liệu Lịch sử (Batch Dataset):**
   - Quy mô: **1,050,000 bản ghi giao dịch** (`transactions.csv`, dung lượng **95.47 MB**) liên kết với bảng danh mục **2,000 sản phẩm** (`products_catalog.csv`) và **85,000 khách hàng**.
   - Cấu trúc trường: `Order_ID`, `Customer_ID`, `Product_ID`, `Price`, `Category`, `Order_Status`, `Timestamp` (kèm các thuộc tính mở rộng từ bảng danh mục: `Product_Name`, `Brand`, `Base_Cost`, `Supplier_Region`).
2. **Dữ liệu Luồng Thời gian thực (Streaming Clickstream):**
   - Quy mô: **3,600 sự kiện tương tác** (`clickstream_events.json`) phát sinh liên tục qua topic Kafka `clickstream-events` (3 Partitions, khóa phân vùng ngữ nghĩa `key = user_id`).
   - Cấu trúc trường: `event_id`, `user_id`, `product_id`, `event_type` (`view`, `cart`, `remove_from_cart`, `purchase`), `session_id`, `timestamp`, `category`, `price`, `is_late`, `delay_seconds`.

---

## II. KẾT QUẢ TIỀN XỬ LÝ DỮ LIỆU & TỐI ƯU LƯU TRỮ HDFS / PARQUET (CORE 1)

### 1. Kiểm định Độ tin cậy Dữ liệu (Veracity — Làm sạch Raw $\rightarrow$ Refined)
Quá trình chuyển đổi từ tầng `Raw Data Layer` sang `Refined Data Layer` thực hiện kiểm tra chất lượng và lọc nhiễu tự động:

| Kiểm tra Chất lượng (Check Name) | Tổng Bản Ghi Thô | Số Bản Ghi Lỗi Phát Hiện | Số Bản Ghi Sạch Giữ Lại | Tỷ Lệ Lỗi (%) | Phương Pháp Xử Lý |
|---|---:|---:|---:|---:|---|
| **Missing Category Imputation** | `1,050,000` | `4,200` | `1,050,000` | `0.400%` | Điền khuyết `NULL Category` theo danh mục chuẩn của sản phẩm |
| **Invalid/Negative Price Filter** | `1,050,000` | `4,200` | `1,045,800` | `0.400%` | Loại bỏ các bản ghi có `Price <= 0` hoặc `NULL` |
| **Duplicate Order_ID Deduplication** | `1,050,000` | `4,172` | **`1,041,628`** | `0.397%` | Khử trùng lặp khóa `Order_ID` trước khi ghi xuống tầng Refined |

### 2. Đối chiếu Dung lượng Lưu trữ & Tỷ lệ Nén giữa 3 Tầng HDFS
Trên nền tảng lưu trữ với `dfs.blocksize = 128 MB` và `dfs.replication = 1`:

| Tầng Lưu Trữ (Storage Layer) | Định Dạng (Format) | Thuật Toán Nén (Codec) | Chiến Lược Phân Vùng | Dung Lượng (MB) | Tỷ Lệ Nén | Số Tệp | Kích Thước TB/Tệp (MB) |
|---|---|---|---|---:|---:|---:|---:|
| **1. Raw Data Layer (Baseline)** | CSV (Văn bản theo dòng) | Không nén (`NONE`) | Không phân vùng (`/data/raw/transactions_csv`) | **95.47 MB** | `1.00x` | `1` | `95.47 MB` |
| **2. Refined Data Layer** | Apache Parquet (Hướng cột) | `SNAPPY` + Dictionary | Không phân vùng (`/data/refined/...`) | **27.76 MB** | **`3.44x`** | `1` | `27.76 MB` |
| **3. Partitioned Data Layer** | Apache Parquet (Hướng cột) | `SNAPPY` + Dictionary | Phân vùng `year=/month=/` (`/data/partitioned/...`) | **34.99 MB** | **`2.73x`** | `12` | `2.92 MB` |

> **Nhận xét học thuật (theo bài giảng Note 3 - HDFS & Parquet):**
> - Định dạng cột **Parquet** kết hợp **Snappy** và **Dictionary Encoding** giúp giảm dung lượng từ **95.47 MB** xuống chỉ còn **27.76 MB** (tiết kiệm **70.9% dung lượng đĩa**).
> - Tầng **Partitioned Parquet** (`year=/month=/`) có dung lượng `34.99 MB` (nhỉnh hơn Unpartitioned Parquet `7.23 MB`) do dữ liệu được chia thành 12 thư mục tháng riêng biệt, mỗi tệp chứa Footer Metadata và từ điển riêng. Đây chính là sự đánh đổi kinh điển giữa **chi phí siêu dữ liệu phân vùng** và **tốc độ cắt tỉa phân vùng (Partition Pruning)** khi truy vấn.

### 3. Thực nghiệm Ảnh hưởng của Kích thước Row Group (`parquet.block.size`)

| Row Group Size (`parquet.block.size`) | Thời Gian Ghi (s) | Dung Lượng Lưu Trữ (MB) | Thời Gian Truy Vấn TB (s) | Lượng Quét I/O (MB) | Đỉnh RAM (MB) | Đánh Giá Tương Thích Với HDFS Block (128MB) |
|---:|---:|---:|---:|---:|---:|---|
| **32 MB** | `2.038s` | `28.76 MB` | `0.0673s` | `8.436 MB` | `584.35 MB` | 4 Row Groups / 128MB Block (Chi phí đọc Footer cao) |
| **64 MB** | `1.656s` | `27.46 MB` | **`0.0511s`** | `8.056 MB` | `659.00 MB` | 2 Row Groups / 128MB Block (Cân bằng tốt cho quét song song) |
| **128 MB** | `1.755s` | **`27.45 MB`** | `0.0981s` | **`8.052 MB`** | `661.45 MB` | **Căn chỉnh 1:1 với 128MB HDFS Block (Tối ưu Data Locality)** |
| **256 MB** | `2.245s` | `27.58 MB` | `0.1402s` | `9.193 MB` | `716.51 MB` | Vượt quá 128MB HDFS Block (Nguy cơ tách khối qua mạng giữa các DataNode) |

---

## III. KẾT QUẢ PHÂN TÍCH TẦNG XỬ LÝ KHỐI — BATCH LAYER (CORE 2)

### 1. Phân bố Trạng thái Đơn hàng (`batch_order_status_summary`)
Trên tổng số **1,041,628 giao dịch hợp lệ**:
- **Completed:** `791,701` đơn hàng (`76.01%`) — Tổng giá trị thực thu: **`$491,411,787.39`**
- **Cancelled:** `93,455` đơn hàng (`8.97%`) — Giá trị: `$58,058,173.07`
- **Processing:** `83,566` đơn hàng (`8.02%`) — Giá trị: `$52,161,859.45`
- **Returned:** `41,516` đơn hàng (`3.99%`) — Giá trị: `$25,618,601.70`
- **Refunded:** `31,390` đơn hàng (`3.01%`) — Giá trị: `$19,460,472.83`

### 2. Top 10 Sản phẩm Bán chạy nhất (`batch_top_products` — Spark SQL Join)
| Hạng | Mã SP | Tên Sản Phẩm | Thương Hiệu | Danh Mục | Số Lượng Bán | Tổng Doanh Thu ($) | Giá TB ($) | Biên Lợi Nhuận (%) |
|---:|---|---|---|---|---:|---:|---:|---:|
| **1** | `PROD_00002` | HP Notebook Series-101 | HP | `computers.notebook` | `29,639` | **`$29,379,763.96`** | `$991.25` | `28.71%` |
| **2** | `PROD_00010` | ASUS Notebook Series-109 | ASUS | `computers.notebook` | `7,583` | **`$19,007,280.12`** | `$2,506.56` | `21.54%` |
| **3** | `PROD_00011` | Samsung Tv Series-110 | Samsung | `electronics.video.tv` | `6,932` | **`$10,285,319.80`** | `$1,483.74` | `25.25%` |
| **4** | `PROD_00018` | Xiaomi Smartphone Series-117 | Xiaomi | `electronics.smartphone` | `4,599` | `$6,271,652.37` | `$1,363.70` | `30.23%` |
| **5** | `PROD_00015` | Dell Notebook Series-114 | Dell | `computers.notebook` | `5,209` | `$6,016,178.97` | `$1,154.96` | `20.02%` |
| **6** | `PROD_00001` | NewBalance Shoes Series-100 | NewBalance | `apparel.shoes` | `53,158` | `$5,606,452.37` | `$105.47` | `19.98%` |
| **7** | `PROD_00005` | Apple Smartphone Series-104 | Apple | `electronics.smartphone` | `13,696` | `$5,437,141.47` | `$396.99` | `17.81%` |
| **8** | `PROD_00013` | Hitachi Refrigerators Series-112 | Hitachi | `appliances.kitchen.refrigerators` | `5,999` | `$5,046,950.42` | `$841.30` | `25.82%` |
| **9** | `PROD_00012` | Merida Bicycle Series-111 | Merida | `sport.bicycle` | `6,487` | `$4,742,125.99` | `$731.02` | `18.49%` |
| **10** | `PROD_00041` | Lenovo Notebook Series-140 | Lenovo | `computers.notebook` | `2,258` | `$4,403,426.30` | `$1,950.14` | `28.16%` |

### 3. Phân khúc Khách hàng RFM bằng Học máy K-Means (`batch_customer_segments`)
Áp dụng thuật toán phân cụm **K-Means (`k=4`)** trên 3 chiều đặc trưng **Recency** (số ngày từ lần mua cuối), **Frequency** (tần suất đơn hàng hoàn tất), và **Monetary** (tổng chi tiêu):

| Mã Cụm | Tên Phân Khúc (RFM Tier) | Số Khách Hàng | Recency TB (Ngày) | Frequency TB (Đơn) | Chi Tiêu TB ($) | Tỷ Trọng Doanh Thu (%) |
|---:|---|---:|---:|---:|---:|---:|
| **1** | **Champions (VIP High-Spend)** | `16,217` | `16.91` | `13.45` | **`$9,549.89`** | **`31.52%`** |
| **2** | **Loyal Customers (Frequent)** | `33,682` | `19.90` | `10.06` | `$6,190.75` | **`42.43%`** |
| **3** | **Potential / Promising Buyers** | `8,630` | `97.78` | `6.56` | `$3,962.09` | `6.96%` |
| **4** | **At-Risk / Hibernating Users** | `26,458` | `22.50` | `6.73` | `$3,546.43` | `19.09%` |

---

## IV. KẾT QUẢ XỬ LÝ LUỒNG THỜI GIAN THỰC — SPEED LAYER (CORE 3)

Thực nghiệm trên luồng **3,600 sự kiện Clickstream** từ Kafka với cấu hình:
- **Sliding Window:** Độ dài cửa sổ `1 phút (60 giây)`, bước trượt `10 giây` $\rightarrow$ tạo ra **93 cửa sổ thời gian**.
- **Watermarking:** Ngưỡng trễ tối đa `10 phút (600 giây)`:
  - **165 sự kiện đến muộn** có độ trễ $60\text{s} \le \Delta t \le 600\text{s}$ được **chấp nhận** và cập nhật chính xác vào cửa sổ thời gian gốc.
  - **53 sự kiện đến muộn quá ngưỡng** ($\Delta t > 600\text{s}$) bị **loại bỏ tự động**, giúp giải phóng trạng thái cửa sổ cũ trong *State Store*.

---

## V. KẾT QUẢ THỰC NGHIỆM BENCHMARK SQL (PHẦN IV ĐỀ XUẤT)

### 1. Bảng Tổng hợp 4 Chỉ số Đánh giá Hiệu năng
| Truy Vấn | Mô Hình Lưu Trữ | Thời Gian (s) | Bytes Read (MB) | Số Dòng Quét | CPU (%) | RAM (MB) | Tăng Tốc (Speedup) | Giảm I/O (%) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| **Q1 (Partition Pruning)** | `(1) CSV thô (Baseline)` | `1.4271s` | `95.473 MB` | `1,050,000` | `44.3%` | `759.30 MB` | `1.00x` | `0.00%` |
| **Q1 (Partition Pruning)** | `(2) Unpartitioned Parquet` | `0.1433s` | `4.072 MB` | `95,166` | `47.8%` | `721.64 MB` | `9.96x` | `95.74%` |
| **Q1 (Partition Pruning)** | `(3) Partitioned Parquet` | **`0.1428s`** | **`2.146 MB`** | `95,166` | `36.9%` | `733.07 MB` | **`9.99x`** | **`97.75%`** |
| **Q2 (Predicate Pushdown)** | `(1) CSV thô (Baseline)` | `1.6433s` | `95.473 MB` | `1,050,000` | `43.0%` | `801.98 MB` | `1.00x` | `0.00%` |
| **Q2 (Predicate Pushdown)** | `(2) Unpartitioned Parquet` | **`0.1358s`** | **`2.221 MB`** | `119,675` | `50.4%` | `771.50 MB` | **`12.10x`** | **`97.67%`** |
| **Q2 (Predicate Pushdown)** | `(3) Partitioned Parquet` | `0.3340s` | `2.799 MB` | `119,675` | `57.4%` | `797.81 MB` | `4.92x` | `97.07%` |
| **Q3 (Full Group By)** | `(1) CSV thô (Baseline)` | `1.7498s` | `95.473 MB` | `1,050,000` | `48.2%` | `919.48 MB` | `1.00x` | `0.00%` |
| **Q3 (Full Group By)** | `(2) Unpartitioned Parquet` | **`0.3286s`** | **`14.065 MB`** | `791,701` | `47.0%` | `881.67 MB` | **`5.32x`** | **`85.27%`** |
| **Q3 (Full Group By)** | `(3) Partitioned Parquet` | `0.3509s` | `17.730 MB` | `791,701` | `48.4%` | `912.68 MB` | `4.99x` | `81.43%` |
| **Q4 (Join + Q4 Filter)** | `(1) CSV thô (Baseline)` | `1.2273s` | `95.473 MB` | `1,050,000` | `42.3%` | `889.28 MB` | `1.00x` | `0.00%` |
| **Q4 (Join + Q4 Filter)** | `(2) Unpartitioned Parquet` | `0.1282s` | `7.773 MB` | `104,290` | `47.7%` | `839.23 MB` | `9.57x` | `91.86%` |
| **Q4 (Join + Q4 Filter)** | `(3) Partitioned Parquet` | **`0.1085s`** | **`2.218 MB`** | `104,290` | `38.1%` | `836.70 MB` | **`11.31x`** | **`97.68%`** |

### 2. Phân tích Cơ chế Tối ưu hóa Vật lý (Predicate Pushdown & Partition Pruning)
- **Trong Truy vấn Q1 và Q4 (Có điều kiện lọc trên khóa phân vùng `year` và `month`):**
  - Mô hình **(3) Partitioned Parquet** kích hoạt đồng thời `PartitionFilters: [isnotnull(year), (year = 2025), month IN (11, 12)]` và `PushedFilters: [IsNotNull(Price), GreaterThan(Price,300.0), EqualTo(Order_Status,Completed)]`.
  - Hệ thống loại bỏ ngay 10–11 thư mục tháng không liên quan mà không cần mở tệp, giảm dung lượng đọc từ **95.473 MB** xuống chỉ còn **2.146 MB – 2.218 MB** (tiết kiệm **97.75% I/O**) và tăng tốc **11.31 lần** so với CSV thô.
- **Trong Truy vấn Q2 và Q3 (Không lọc theo `year`, `month` mà lọc theo `Price`, `Category` trên toàn bộ lịch sử):**
  - Mô hình **(2) Unpartitioned Parquet** đạt tốc độ nhanh nhất (**`0.1358s`**, nhanh hơn **12.10 lần** so với CSV) vì chỉ cần mở 1 tệp Parquet liên tục duy nhất và dùng thống kê `min/max` ở Footer để nhảy qua các Row Group không thỏa mãn (`Predicate Pushdown`), trong khi mô hình Partitioned phải mở 12 tệp phân vùng riêng lẻ. Kết quả này minh chứng hoàn hảo lý thuyết thiết kế phân vùng trong học phần **IT4931**.

