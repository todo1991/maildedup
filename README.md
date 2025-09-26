# IMAP Dedup Batch

Script Python 3.12 giúp loại bỏ email trùng lặp trên các hộp thư IMAP lớn bằng cách:
- Băm thông tin header + kích thước thư để phát hiện trùng lặp ổn định.
- Lưu trạng thái vào SQLite để có thể resume và tránh xử lý lại thư đã giữ.
- Làm việc theo batch, tự reconnect khi lỗi, chia nhỏ STORE và EXPUNGE định kỳ.

## Yêu cầu
- Python ≥ 3.12 (thư viện chuẩn đủ dùng).
- Tài khoản IMAP có quyền ghi (STORE `\\Deleted`, EXPUNGE).
- Khả năng truy cập tới máy chủ IMAP (mặc định TLS qua cổng 993).

## Cài đặt
Clone/repo chứa `imap_dedupe_batch.py`, bảo đảm Python 3.12 có sẵn. Không cần cài thêm package.

## Cách chạy nhanh
```bash
python3 imap_dedupe_batch.py \
  -H 172.16.6.30 -P 993 \
  -u '"'"'dan@a.co'"'"' -p "$IMAP_PASS" \
  -m '"'"'cron'"'"' \
  --chunk 1500 --store-chunk 400 \
  --expunge-interval 2 \
  --sleep 0.2
```
`$IMAP_PASS` nên được đặt trong biến môi trường để tránh lộ mật khẩu.

## Tham số
| Tham số | Bắt buộc | Mặc định | Mô tả |
| --- | --- | --- | --- |
| `-H`, `--host` | ✔ | – | Máy chủ IMAP. |
| `-P`, `--port` | ✖ | `993` | Cổng IMAP. |
| `-u`, `--user` | ✔ | – | Tài khoản IMAP. |
| `-p`, `--password` | ✔ | – | Mật khẩu. |
| `-m`, `--mailbox` | ✖ | `INBOX` | Thư mục cần xử lý. |
| `--no-ssl` | ✖ | TLS bật | Kết nối plain IMAP; kết hợp `--starttls` nếu cần. |
| `--starttls` | ✖ | Tắt | Gửi STARTTLS sau khi kết nối plain IMAP. |
| `--since` | ✖ | – | Lọc thư từ ngày (YYYY-MM-DD). |
| `--before` | ✖ | – | Lọc thư trước ngày (YYYY-MM-DD). |
| `--chunk` | ✖ | `3000` | Số UID mỗi batch FETCH + xử lý. |
| `--store-chunk` | ✖ | `500` | Số UID mỗi lần STORE `\\Deleted`. |
| `--fetch-flags-chunk` | ✖ | `800` | Batch kiểm tra FLAGS khi resume. |
| `--sleep` | ✖ | `0.0` | Nghỉ giữa các batch (`time.sleep`). |
| `--timeout` | ✖ | `120` | IMAP socket timeout (giây). |
| `--db` | ✖ | `imap_dedupe.sqlite3` | File SQLite lưu digest đã giữ. |
| `--criteria` | ✖ | `msgid_first` | `msgid_first`: ưu tiên Message-ID; `composite_only`: bỏ qua Message-ID, chỉ dùng header khác + size. |
| `--dry-run` | ✖ | Tắt | Chỉ báo cáo UID trùng, không STORE/EXPUNGE. |
| `--expunge-interval` | ✖ | `5` | Gọi EXPUNGE sau N batch (`0` = chỉ EXPUNGE cuối). |

## Quy trình hoạt động
1. Kết nối IMAP (TLS hoặc STARTTLS nếu chọn), chọn mailbox.
2. Tìm tất cả UID thỏa điều kiện ngày/thư mục.
3. Chia batch (`--chunk`), FETCH header + size cho từng UID.
4. Tính digest: dùng Message-ID nếu bật `msgid_first`, nếu trống chuyển sang tổ hợp Date/From/To/Subject/Size để ổn định.
5. Tra bảng `seen_hashes` trong SQLite:
   - Digest mới ⇒ lưu UID, giữ thư.
   - Digest đã tồn tại ⇒ đánh dấu UID mới là trùng.
6. Với UID trùng:
   - `--dry-run` ⇒ chỉ in cảnh báo.
   - Mặc định ⇒ STORE `\\Deleted` theo `--store-chunk`, EXPUNGE theo chu kỳ `--expunge-interval`.
   - Nếu STORE/EXPUNGE abort ⇒ tự reconnect, lọc UID chưa `\\Deleted`, thử lại.
7. Kết thúc: EXPUNGE lần cuối (trừ khi `--dry-run`), in thống kê: số thư giữ mới/cũ và số trùng đã xoá hoặc sẽ xoá.

## Ghi chú an toàn
- Trước khi chạy thực tế hãy thử `--dry-run` để kiểm tra số lượng thư trùng.
- Sao lưu mailbox hoặc đảm bảo server hỗ trợ Undo trước khi xoá thật.
- File SQLite có thể tái sử dụng cho nhiều lần chạy trên cùng mailbox; xóa file nếu muốn xử lý lại từ đầu.
- Script tự reconnect khi thấy `imaplib.IMAP4.abort`, nhưng vẫn nên giám sát log để xử lý lỗi mạng kéo dài.

## Mẹo vận hành
- Tăng `--sleep` hoặc giảm `--chunk` khi server giới hạn tốc độ.
- Dùng `--since` / `--before` khi muốn xử lý theo phân đoạn thời gian.
- Database (`--db`) nên đặt ở ổ đĩa ổn định; có thể chia theo mailbox nếu chạy song song.
