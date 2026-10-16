"""
batch_writer.py - Atomic compressed disk writer for Korean Sunrise signals.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

log = logging.getLogger("writer")


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class BatchWriter:
    """Buffers signals in RAM (gzipped) and writes sequentially to disk."""

    def __init__(self, data_root: str = "data", flush_interval_sec: float = 5.0):
        self.data_root = data_root
        self.flush_interval_sec = flush_interval_sec
        self.buf = io.BytesIO()
        self.gz = gzip.GzipFile(fileobj=self.buf, mode="wb", compresslevel=6)
        self.rows_count = 0
        self.total_written = 0
        self._flush_seq = 0
        self._lock = threading.Lock()
        self._last_flush = time.time()
        os.makedirs(os.path.join(self.data_root, "signals"), exist_ok=True)

    def add(self, signal_dict: Dict[str, Any]) -> None:
        try:
            line = json.dumps(signal_dict, default=str) + "\n"
            encoded = line.encode("utf-8")
            with self._lock:
                self.gz.write(encoded)
                self.rows_count += 1
        except Exception as e:
            log.error(f"[writer] Failed to buffer signal: {e}")

    def maybe_flush(self, now: float = 0.0) -> None:
        now = now or time.time()
        if now - self._last_flush >= self.flush_interval_sec and self.rows_count > 0:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            if self.rows_count == 0:
                return

            batch_rows = self.rows_count
            self._flush_seq += 1
            seq = self._flush_seq

            self.gz.close()
            compressed_data = self.buf.getvalue()

            day = _utc_day()
            target_dir = os.path.join(self.data_root, "signals", day)
            os.makedirs(target_dir, exist_ok=True)

            ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"sunrise_signals_{ts_str}_{seq:04d}.jsonl.gz"
            tmp_path = os.path.join(target_dir, f"{filename}.tmp")
            final_path = os.path.join(target_dir, filename)

            try:
                with open(tmp_path, "wb") as f:
                    f.write(compressed_data)
                os.replace(tmp_path, final_path)
                self.total_written += batch_rows
                log.info(
                    f"[writer] Flushed {batch_rows} signals to {filename} "
                    f"({len(compressed_data)} bytes)"
                )
            except Exception as e:
                log.error(f"[writer] Flush error: {e}")
            finally:
                self.buf = io.BytesIO()
                self.gz = gzip.GzipFile(fileobj=self.buf, mode="wb", compresslevel=6)
                self.rows_count = 0
                self._last_flush = time.time()