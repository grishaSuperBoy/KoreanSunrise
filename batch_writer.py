"""
batch_writer.py - Non-blocking compressed disk writer for Korean Sunrise signals.
Buffers raw JSON lines in RAM and compresses via gzip in batches upon flush.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

log = logging.getLogger("writer")


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class BatchWriter:
    """Buffers signals in RAM (as raw lines) and writes compressed sequentially to disk."""

    def __init__(self, data_root: str = "data", flush_interval_sec: float = 5.0):
        self.data_root = data_root
        self.flush_interval_sec = flush_interval_sec
        self._lines: List[str] = []
        self.total_written = 0
        self._flush_seq = 0
        self._lock = threading.Lock()
        self._last_flush = time.time()
        os.makedirs(os.path.join(self.data_root, "signals"), exist_ok=True)

    @property
    def rows_count(self) -> int:
        with self._lock:
            return len(self._lines)

    def add(self, signal_dict: Dict[str, Any]) -> None:
        """Non-blocking in-memory append of signal line."""
        try:
            line = json.dumps(signal_dict, default=str) + "\n"
            with self._lock:
                self._lines.append(line)
        except Exception as e:
            log.error(f"[writer] Failed to buffer signal: {e}")

    def maybe_flush(self, now: float = 0.0) -> None:
        now = now or time.time()
        with self._lock:
            need_flush = (now - self._last_flush >= self.flush_interval_sec) and bool(self._lines)
        if need_flush:
            self.flush()

    def flush(self) -> None:
        """Compresses all buffered lines in a single batch and writes atomically to disk."""
        with self._lock:
            if not self._lines:
                return
            batch_lines = self._lines
            self._lines = []
            self._last_flush = time.time()
            self._flush_seq += 1
            seq = self._flush_seq

        batch_rows = len(batch_lines)
        try:
            raw_bytes = "".join(batch_lines).encode("utf-8")
            compressed_data = gzip.compress(raw_bytes, compresslevel=6)

            day = _utc_day()
            target_dir = os.path.join(self.data_root, "signals", day)
            os.makedirs(target_dir, exist_ok=True)

            ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"sunrise_signals_{ts_str}_{seq:04d}.jsonl.gz"
            tmp_path = os.path.join(target_dir, f"{filename}.tmp")
            final_path = os.path.join(target_dir, filename)

            with open(tmp_path, "wb") as f:
                f.write(compressed_data)
            os.replace(tmp_path, final_path)
            with self._lock:
                self.total_written += batch_rows
            log.info(
                f"[writer] Flushed {batch_rows} signals to {filename} "
                f"({len(compressed_data)} bytes)"
            )
        except Exception as e:
            with self._lock:
                self._lines = batch_lines + self._lines  # Rollback lines to buffer on failure
            log.error(f"[writer] Flush error: {e}")