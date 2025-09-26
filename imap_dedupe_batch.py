#!/usr/bin/env python3
# imap_dedupe_batch.py
# Batch-dedup IMAP cho hộp thư rất lớn, có retry, reconnect, chia nhỏ STORE, EXPUNGE định kỳ.
# Thư viện chuẩn Python 3.12.

import argparse
import email
from email.header import decode_header, make_header
import hashlib
import imaplib
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any, List, Tuple, Optional

# Tránh "line too long" khi FETCH nhiều header
import imaplib as _imaplib
_imaplib._MAXLINE = max(10_000_000, getattr(_imaplib, "_MAXLINE", 0))

HEADER_FIELDS = ["Message-ID", "Date", "From", "To", "Subject"]
SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_hashes (
  mailbox TEXT NOT NULL,
  digest  TEXT NOT NULL,
  keep_uid INTEGER NOT NULL,
  PRIMARY KEY (mailbox, digest)
);
CREATE INDEX IF NOT EXISTS idx_seen_hashes_mailbox ON seen_hashes(mailbox);
"""

def imap_date(d: str) -> str:
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.strftime("%d-%b-%Y")

def normalize(s: Any) -> str:
    if s is None:
        text = ""
    elif isinstance(s, (bytes, bytearray)):
        text = s.decode("utf-8", "ignore")
    else:
        text = str(s)

    try:
        # email headers may contain RFC 2047 encoded words; decode them for stable hashing
        text = str(make_header(decode_header(text)))
    except Exception:
        pass

    return " ".join(text.strip().split())

def hash_key(msgid: str, date: str, from_: str, to: str, subject: str, size: int) -> str:
    if msgid:
        basis = f"MID:{normalize(msgid).lower()}"
    else:
        basis = "|".join([
            f"F:{normalize(from_).lower()}",
            f"T:{normalize(to).lower()}",
            f"S:{normalize(subject).lower()}",
            f"D:{normalize(date)}",
            f"Z:{size}",
        ])
    return hashlib.sha256(basis.encode("utf-8", "ignore")).hexdigest()

def parse_headers(raw_bytes: bytes):
    msg = email.message_from_bytes(raw_bytes)
    g = lambda k: msg.get(k, "") or ""
    return {
        "Message-ID": g("Message-ID"),
        "Date": g("Date"),
        "From": g("From"),
        "To": g("To"),
        "Subject": g("Subject"),
    }

def chunk_iter(lst: List[int], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def safe_split_fetch_data(data):
    # imaplib trả về list hỗn hợp tuple/bytes; chuẩn hóa về các cặp (meta_str, payload_bytes)
    out = []
    i = 0
    while i < len(data):
        part = data[i]
        if isinstance(part, tuple) and len(part) >= 2:
            meta = part[0]
            payl = part[1]
            meta_str = meta.decode("utf-8", "ignore") if isinstance(meta, (bytes, bytearray)) else str(meta)
            if isinstance(payl, (bytes, bytearray)):
                out.append((meta_str, payl))
        i += 1
    return out

def extract_uid_and_size(meta_str: str) -> Tuple[Optional[int], Optional[int]]:
    uid = None
    size = None
    toks = meta_str.replace("(", " ").replace(")", " ").split()
    for j, t in enumerate(toks):
        if t == "UID" and j + 1 < len(toks):
            try:
                uid = int(toks[j + 1])
            except Exception:
                pass
        if t == "RFC822.SIZE" and j + 1 < len(toks):
            try:
                size = int(toks[j + 1])
            except Exception:
                pass
    return uid, size

class ImapSession:
    def __init__(self, args):
        self.args = args
        self.M: Optional[imaplib.IMAP4] = None
        self.connect()

    def connect(self):
        if self.M:
            try:
                self.M.logout()
            except Exception:
                pass
        if not self.args.no_ssl:
            self.M = imaplib.IMAP4_SSL(self.args.host, self.args.port, timeout=self.args.timeout)
        else:
            self.M = imaplib.IMAP4(self.args.host, self.args.port, timeout=self.args.timeout)
            if self.args.starttls:
                self.M.starttls()
        typ, _ = self.M.login(self.args.user, self.args.password)
        if typ != "OK":
            raise RuntimeError("IMAP login failed")
        self.select_mailbox(self.args.mailbox)

    def select_mailbox(self, mailbox: str):
        typ, data = self.M.select(mailbox, readonly=False)
        if typ != "OK":
            raise RuntimeError(f"Cannot select mailbox {mailbox}: {data}")

    def safe_uid(self, *cmd):
        try:
            return self.M.uid(*cmd)
        except imaplib.IMAP4.abort:
            # reconnect and retry once
            self.connect()
            return self.M.uid(*cmd)

    def noop(self):
        try:
            self.M.noop()
        except imaplib.IMAP4.abort:
            self.connect()
            self.M.noop()

    def expunge(self):
        try:
            self.noop()
            self.M.expunge()
        except imaplib.IMAP4.abort:
            self.connect()
            self.M.expunge()

def search_uids(session: ImapSession, since: str, before: str) -> List[int]:
    crit = ["ALL"]
    if since:
        crit += ["SINCE", imap_date(since)]
    if before:
        crit += ["BEFORE", imap_date(before)]
    typ, data = session.safe_uid("SEARCH", None, *crit)
    if typ != "OK":
        raise RuntimeError(f"SEARCH failed: {data}")
    raw = data[0] or b""
    return list(map(int, raw.split())) if raw else []

def fetch_headers_sizes(session: ImapSession, uids: List[int]) -> List[Tuple[int, bytes, int]]:
    if not uids:
        return []
    seq = ",".join(map(str, uids))
    session.noop()
    typ, data = session.safe_uid(
        "FETCH",
        seq,
        f'(RFC822.SIZE BODY.PEEK[HEADER.FIELDS ({" ".join(HEADER_FIELDS)})])',
    )
    if typ != "OK":
        raise RuntimeError(f"FETCH failed for {len(uids)} uids")
    out = []
    for meta_str, hdr_bytes in safe_split_fetch_data(data or []):
        uid, size = extract_uid_and_size(meta_str)
        if uid is not None and size is not None:
            out.append((uid, hdr_bytes, size))
    return out

def _chunks(uids, n=500):
    for i in range(0, len(uids), n):
        yield uids[i:i+n]

def mark_delete(session: ImapSession, uids: List[int], store_chunk: int):
    if not uids:
        return
    for part in _chunks(uids, store_chunk):
        seq = ",".join(map(str, part))
        session.noop()
        typ, _ = session.safe_uid("STORE", seq, "+FLAGS.SILENT", r"(\Deleted)")
        if typ != "OK":
            raise RuntimeError("STORE +FLAGS.SILENT \\Deleted failed")

def filter_undeleted(session: ImapSession, uids: List[int], fetch_flags_chunk: int) -> List[int]:
    # Loại các UID đã có \Deleted để tránh đánh dấu lại khi resume sau reconnect
    remain = []
    for part in _chunks(uids, fetch_flags_chunk):
        seq = ",".join(map(str, part))
        session.noop()
        typ, data = session.safe_uid("FETCH", seq, "(UID FLAGS)")
        if typ != "OK":
            continue
        it = iter(data or [])
        for item in it:
            if not isinstance(item, tuple) or len(item) < 2:
                continue
            meta = item[0].decode("utf-8", "ignore") if isinstance(item[0], (bytes, bytearray)) else str(item[0])
            uid, _ = extract_uid_and_size(meta.replace("RFC822.SIZE", "X"))  # bỏ SIZE nếu có
            flags_bytes = item[1] if isinstance(item[1], (bytes, bytearray)) else b""
            deleted = b"\\Deleted" in flags_bytes
            if uid and not deleted:
                remain.append(uid)
    return remain

def ensure_schema(db: sqlite3.Connection):
    for stmt in SQL_SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            db.execute(s + ";")
    db.commit()

def main():
    ap = argparse.ArgumentParser(description="Batch IMAP dedupe for very large mailboxes with retry/reconnect.")
    ap.add_argument("-H", "--host", required=True)
    ap.add_argument("-P", "--port", type=int, default=993)
    ap.add_argument("-u", "--user", required=True)
    ap.add_argument("-p", "--password", required=True)
    ap.add_argument("-m", "--mailbox", default="INBOX")
    ap.add_argument("--no-ssl", action="store_true")
    ap.add_argument("--starttls", action="store_true")
    ap.add_argument("--since", help="YYYY-MM-DD")
    ap.add_argument("--before", help="YYYY-MM-DD")
    ap.add_argument("--chunk", type=int, default=3000, help="Batch size for UID list processing")
    ap.add_argument("--store-chunk", type=int, default=500, help="Batch size per UID STORE command")
    ap.add_argument("--fetch-flags-chunk", type=int, default=800, help="Batch size per FLAGS check when resuming")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between batches")
    ap.add_argument("--timeout", type=int, default=120, help="Socket timeout for IMAP operations")
    ap.add_argument("--db", default="imap_dedupe.sqlite3")
    ap.add_argument("--criteria", choices=["msgid_first", "composite_only"], default="msgid_first",
                    help="msgid_first uses Message-ID when present, else composite. composite_only ignores Message-ID.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--expunge-interval", type=int, default=5, help="Expunge after N batches (0 = only at end)")
    args = ap.parse_args()

    # DB
    db = sqlite3.connect(args.db)
    ensure_schema(db)
    cur = db.cursor()

    # IMAP session
    session = ImapSession(args)

    try:
        uids = search_uids(session, args.since, args.before)
        total = len(uids)
        print(f"Found {total} messages matching criteria in {args.mailbox}")

        kept_new = 0
        kept_existing = 0
        dupes_marked = 0
        batch_idx = 0

        for batch in chunk_iter(uids, args.chunk):
            batch_idx += 1

            # FETCH headers + size
            headers = fetch_headers_sizes(session, batch)

            delete_uids = []
            for uid, hdr_bytes, size in headers:
                h = parse_headers(hdr_bytes)
                if args.criteria == "composite_only":
                    digest = hash_key("", h["Date"], h["From"], h["To"], h["Subject"], size)
                else:
                    digest = hash_key(h["Message-ID"], h["Date"], h["From"], h["To"], h["Subject"], size)

                row = cur.execute(
                    "SELECT keep_uid FROM seen_hashes WHERE mailbox=? AND digest=?",
                    (args.mailbox, digest),
                ).fetchone()

                if row is None:
                    cur.execute(
                        "INSERT OR REPLACE INTO seen_hashes(mailbox, digest, keep_uid) VALUES (?, ?, ?)",
                        (args.mailbox, digest, uid),
                    )
                    kept_new += 1
                else:
                    keep_uid = row[0]
                    if keep_uid == uid:
                        kept_existing += 1
                        continue  # this is the message we kept previously
                    delete_uids.append(uid)

            db.commit()

            if delete_uids:
                if args.dry_run:
                    print(f"[Batch {batch_idx}] Would delete {len(delete_uids)} duplicates.")
                else:
                    try:
                        mark_delete(session, delete_uids, args.store_chunk)
                        dupes_marked += len(delete_uids)
                        print(f"[Batch {batch_idx}] Marked {len(delete_uids)} duplicates as \\Deleted.")
                    except imaplib.IMAP4.abort:
                        # Reconnect và thử đánh dấu lại các UID chưa có \Deleted
                        print(f"[Batch {batch_idx}] STORE aborted. Reconnecting and resuming…")
                        session.connect()
                        to_retry = filter_undeleted(session, delete_uids, args.fetch_flags_chunk)
                        if to_retry:
                            mark_delete(session, to_retry, args.store_chunk)
                            newly = len(to_retry)
                            dupes_marked += newly
                            print(f"[Batch {batch_idx}] Resumed. Marked additional {newly} as \\Deleted.")

            if args.expunge_interval and (batch_idx % args.expunge_interval == 0) and not args.dry_run:
                print(f"[Batch {batch_idx}] EXPUNGE…")
                try:
                    session.expunge()
                except imaplib.IMAP4.abort:
                    print(f"[Batch {batch_idx}] EXPUNGE aborted. Reconnecting and retrying EXPUNGE…")
                    session.connect()
                    session.expunge()

            if args.sleep > 0:
                time.sleep(args.sleep)

        if not args.dry_run:
            print("Final EXPUNGE…")
            try:
                session.expunge()
            except imaplib.IMAP4.abort:
                session.connect()
                session.expunge()

        kept_total = kept_new + kept_existing
        if kept_existing:
            print(
                f"Kept: {kept_total} (new: {kept_new}, existing: {kept_existing}) | "
                f"Duplicates {'to delete (dry-run)' if args.dry_run else 'deleted'}: {dupes_marked}"
            )
        else:
            print(
                f"Kept: {kept_total} | "
                f"Duplicates {'to delete (dry-run)' if args.dry_run else 'deleted'}: {dupes_marked}"
            )

    finally:
        try:
            session.M.logout()
        except Exception:
            pass

if __name__ == "__main__":
    main()
