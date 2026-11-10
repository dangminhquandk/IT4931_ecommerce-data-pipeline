# 📚 Cơ Sở Lý Thuyết IT4931 & Thiết Kế Kỹ Thuật Chi Tiết

Tài liệu này tổng hợp kiến thức cốt lõi từ học phần **IT4931 - Lưu trữ và Xử lý Dữ liệu Lớn** (TS. Vũ Tuyết Trinh — Trường CNTT&TT, ĐHBK Hà Nội) và cách vận dụng trực tiếp vào **Hệ thống Đường ống Dữ liệu Thương mại Điện tử Thời gian thực (Nhóm 3)**.

---

## 1. Kiến Trúc Lambda (Lambda Architecture - Bài Giảng Note 8)

Theo bài giảng *Note 8 - Big Data Architecture*, hệ thống xử lý dữ liệu truyền thống gặp hạn chế khi **Hadoop giải quyết tốt bài toán Volume và Variety nhưng không giải quyết được Velocity (độ trễ cao)**. Do đó, hệ thống áp dụng **Lambda Architecture** với 3 tầng tách biệt (*Separation of Concerns*):

1. **Batch Layer (Tầng Xử lý Khối)**:
   - **Nguyên lý bất biến (*Data Immutability*)**: Tập dữ liệu gốc (*Master Dataset*) tại tầng `Raw Data Layer` trên HDFS chỉ cho phép ghi nối thêm (*Append-only*), không bao giờ sửa hay xóa trực tiếp.
   - **Khả năng tính toán lại (*Re-computation Capability*)**: Khi có lỗi logic trong mã nguồn (*human errors in coding*) hoặc thay đổi thuật toán phân tích, hệ thống có thể chạy lại toàn bộ tập dữ liệu lịch sử để tái tạo các *Batch Views* mà không mất mát dữ liệu.
2. **Speed Layer (Tầng Xử lý Luồng)**:
   - Bù đắp độ trễ xử lý (*processing lag*) của Batch Layer bằng cách xử lý luồng sự kiện Clickstream liên tục từ **Apache Kafka** qua **Spark Structured Streaming** theo cơ chế micro-batch với độ trễ thấp, cập nhật các *Real-time Views*.
3. **Serving Layer (Tầng Phục vụ & Mô hình Nhất quán Cuối cùng - Eventual Consistency)**:
   - **Batch Layer** cung cấp góc nhìn nhất quán mạnh (*strongly consistent*) nhưng có độ trễ lịch sử.
   - **Speed Layer** cung cấp góc nhìn cập nhật tức thời (*up-to-date*) trên luồng dữ liệu mới.
   - **Serving Layer (PostgreSQL)** lưu trữ, đánh chỉ mục và hợp nhất cả hai khung nhìn để phục vụ **Streamlit Dashboard**, đạt trạng thái **Eventual Consistency** và hiện thực hóa nguyên lý **Polyglot Persistence** (kết hợp HDFS/Parquet cho lưu trữ phân tích tuần tự và RDBMS có chỉ mục cho truy vấn độ trễ thấp).

---

## 2. Hệ Thống Tệp Phân Tán HDFS & Định Dạng Cột Parquet (Bài Giảng Note 3)

### 2.1. Nguyên Lý Thiết Kế HDFS (`note3_HDFS.pdf`)
- **Kiến trúc Master/Slave**:
  - **NameNode (Master)**: Quản lý không gian tên (*File System Namespace*), ánh xạ tên tệp thành danh sách các khối (*Blocks*) và vị trí các *DataNodes* lưu trữ chúng. Toàn bộ siêu dữ liệu (*Metadata*) được giữ trong bộ nhớ RAM chính của NameNode để truy xuất nhanh, kết hợp `FsImage` và `Transaction Log (EditLog)`.
  - **DataNode (Slave)**: Lưu trữ các khối dữ liệu thực tế trên hệ thống tệp cục bộ kèm mã kiểm tra **CRC32 Checksum** (mỗi 512 bytes) và gửi **Heartbeat** định kỳ (3 giây/lần) cùng **Block Report** về NameNode.
- **Mô hình I/O Write-Once-Read-Many (WORM)**:
  - Tối ưu hóa cho các tệp dung lượng lớn (từ hàng trăm MB đến hàng TB) và luồng đọc tuần tự thông lượng cao.
  - Cấu hình thực nghiệm trên cụm Docker: `dfs.blocksize = 134217728` (**128 MB**) và `dfs.replication = 1` (khi mở rộng lên cụm vật lý sẽ đặt `dfs.replication = 3` theo chiến lược: 1 bản sao tại nút cục bộ, 1 bản sao tại rack khác, và 1 bản sao cùng rack từ xa đó).
- **Giải quyết bài toán Tệp Nhỏ (*Small Files Problem*)**:
  - Việc lưu hàng vạn tệp kích thước vài KB lên HDFS sẽ làm cạn kiệt bộ nhớ RAM của NameNode. Trong dự án (`src/storage/hdfs_storage_manager.py`), dữ liệu trước khi ghi xuống tầng *Refined* và *Partitioned* đều được gom nhóm (`coalesce` / `repartition`) thành các tệp lớn tương thích với kích thước Block 128MB.

### 2.2. Định Dạng Cột Apache Parquet & Thuật Toán Nén Snappy (`note3_HDFS.pdf` - Slide 41-43)
Khác với định dạng dòng (`CSV`, `JSON`) phải quét toàn bộ bản ghi trên đĩa ngay cả khi truy vấn chỉ cần vài cột, **Apache Parquet** lưu trữ dữ liệu theo định dạng cột (*Column-oriented binary format*):
- **Cấu trúc nội tại của tệp Parquet**:
  - **Row Group (`parquet.block.size`)**: Chia tệp thành các nhóm dòng ngang (mặc định `128 MB`). Trong dự án, việc căn chỉnh `parquet.block.size = 128 MB` trùng khớp tỷ lệ `1:1` với `dfs.blocksize = 128 MB` của HDFS giúp mỗi Row Group nằm trọn vẹn trong một HDFS Block duy nhất, tránh hiện tượng một Row Group bị cắt ngang qua hai DataNode gây tốn băng thông mạng khi đọc!
  - **Column Chunk & Page (`parquet.page.size = 1 MB`)**: Trong mỗi Row Group, từng cột được lưu thành một *Column Chunk* liên tiếp nhau trên đĩa, bên trong chia nhỏ thành các *Pages* (1 MB).
  - **Dictionary Encoding (`parquet.enable.dictionary = true`, `parquet.dictionary.page.size = 1 MB`)**: Các cột có số lượng giá trị phân biệt thấp (*low cardinality*) như `Category`, `Order_Status`, `Brand`, `event_type` được mã hóa từ điển tự động, giúp giảm mạnh dung lượng trước khi nén khối bằng **Snappy (`parquet.compression = SNAPPY`)**.
  - **Footer Statistics & Predicate Pushdown**: Phần *Footer* của mỗi tệp và mỗi Row Group lưu trữ siêu dữ liệu thống kê (`min`, `max`, `null_count`) của từng cột. Khi thực thi câu lệnh SQL có mệnh đề `WHERE Price > 650`, Spark đẩy điều kiện lọc xuống tầng đọc Parquet (*Predicate Pushdown* / `PushedFilters`), bỏ qua hoàn toàn các Row Group có `max(Price) <= 650` mà không cần giải nén hay đọc dữ liệu!

---

## 3. Hệ Thống Nhắn Tin Phân Tán Apache Kafka (Bài Giảng Note 5)

Theo bài giảng *Note 5 - Distributed Message Queue (Apache Kafka)*:
1. **Phân tách đường ống dữ liệu (*Decoupling Data Pipelines*)**:
   - `event-generator-service` đóng vai trò **Producer** đẩy sự kiện vào topic `clickstream-events` mà không phụ thuộc vào tốc độ hay trạng thái hoạt động của **Consumer** (Spark Structured Streaming).
2. **Topic Partitions & Bảo toàn thứ tự (*Ordering Guarantee*)**:
   - Topic `clickstream-events` được chia thành **3 Partitions** để mở rộng thông lượng đọc/ghi song song.
   - Kafka **chỉ bảo đảm thứ tự thông điệp trong phạm vi một Partition đơn lẻ**. Do đó, `ClickstreamEventGenerator` sử dụng **Semantic Partitioning** với khóa bản ghi là `key = user_id` (`note5_Kafka.pdf` Slide 23): mọi sự kiện (`view -> cart -> purchase`) của cùng một người dùng luôn được định tuyến về cùng một Partition theo đúng trình tự thời gian phát sinh.
3. **Độ bền ghi (*Durable Writes*) & Consumer Offsets**:
   - Producer cấu hình `acks = 'all'` (`request.required.acks = -1`, Slide 22) để đảm bảo bản ghi chỉ được coi là *committed* khi toàn bộ các *In-Sync Replicas (ISRs)* đã ghi vào commit log.
   - Spark Structured Streaming quản lý vị trí đọc thông qua **Offset** và lưu trữ bền vững trong thư mục `checkpointLocation` trên HDFS để đảm bảo ngữ nghĩa xử lý chịu lỗi (*fault tolerance*).

---

## 4. Xử Lý Khối & Luồng Với Apache Spark (Core 2, Core 3 & Section IV)

### 4.1. Tối Ưu Hóa Truy Vấn Với Catalyst Optimizer & Cost-Based Optimizer (CBO)
- **Partition Pruning (`PartitionFilters`)**: Khi dữ liệu giao dịch được phân vùng vật lý theo thư mục `year=YYYY/month=MM/`, truy vấn lọc `WHERE year = 2025 AND month = 11` chỉ quét đúng thư mục `year=2025/month=11/` (giảm ~91.6% lượng tệp cần đọc ngay từ bước duyệt cây thư mục).
- **Predicate Pushdown (`PushedFilters`) & Column Projection**: Chỉ đọc các *Column Chunks* xuất hiện trong câu lệnh `SELECT` và lọc sớm dựa trên thống kê `min/max` của Row Group.
- **Cost-Based Optimizer (`spark.sql.cbo.enabled = true`)**: Tối ưu hóa thứ tự và chiến lược phép nối (*BroadcastHashJoin* khi nối bảng giao dịch >1M dòng với bảng danh mục `products_catalog` kích thước nhỏ).
  
### 4.2. Xử Lý Luồng Event-Time, Cửa Sổ Trượt & Watermarking
- **Sliding Window (`window_duration = 1 minute`, `slide_duration = 10 seconds`)**: Mỗi 10 giây, hệ thống tính toán các chỉ số (`total_events`, `active_users`, `purchase_count`, `gross_sales`, `conversion_rate_pct`) trong khung thời gian 1 phút gần nhất dựa trên `event_timestamp` (thời điểm sự kiện thực sự xảy ra tại thiết bị người dùng, không phải *Processing Time* khi máy chủ nhận được).
- **Watermarking (`10 minutes`)**:
  - Ngưỡng Watermark được xác định bằng:
    $$\text{Watermark}_t = \max(\text{event\textunderscore timestamp}) - 10\text{ minutes}$$
  - Các sự kiện đến muộn (*Late Data*) nhưng có `event_timestamp` $\ge \text{Watermark}_t$ vẫn được Spark chấp nhận và cập nhật chính xác vào cửa sổ tương ứng.
  - Các sự kiện trễ quá 10 phút (`event_timestamp` $< \text{Watermark}_t$) sẽ bị loại bỏ, đồng thời Spark giải phóng trạng thái của các cửa sổ cũ khỏi bộ nhớ (*State Store Cleanup*), ngăn chặn triệt để lỗi tràn bộ nhớ (`OutOfMemoryError`) khi chạy dịch vụ 24/7.
