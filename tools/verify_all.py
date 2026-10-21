"""
tools/verify_all.py - Complete Multi-Gate Verification Suite for Korean Sunrise.

Automated 5-Gate Quality Arbiter:
  - Gate 1: Manifest Integrity (SHA256 checksums across core files).
  - Gate 2: Fee & Parameter Synchronization (Engine, Config, Calibrator, Models).
  - Gate 3: Microstructure Data Contracts & Models (OrderBookDepth5, SunriseSignal, normalize_exchange_ts).
  - Gate 4: Bytecode Compilation & Syntax Audit (all .py files in repo).
  - Gate 5: Deep Invariant & Unit Test Suite (python -m unittest).

Usage:
  python tools/verify_all.py           # Run all 5 gates
  python tools/verify_all.py --seal    # Reseal MANIFEST.sha256 with current clean hashes
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import os
import py_compile
import sys
import unittest

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
MANIFEST_FILE = os.path.join(PROJECT_ROOT, "MANIFEST.sha256")

CORE_FILES = [
    "config.py",
    "models.py",
    "korean_engine.py",
    "venues/base.py",
    "venues/manager.py",
    "venues/bithumb.py",
    "pipeline/evaluator.py",
    "pipeline/pool_state.py",
    "pipeline/profile.py",
    "tools/calibrate_thresholds.py",
    "tools/full_daily_report.py",
    "tools/pack_codebase.py",
    "tests/test_adaptive_stops_and_fixes.py",
    "tests/test_bithumb_venue.py",
    "tests/test_bithumb_lead_lag.py",
    "tests/test_cargo_cult_audit.py",
    "tests/test_korean_engine_strategy.py",
    "tests/test_pipeline_e2e.py",
    "tests/test_venues_streams_resilience.py",
]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def seal_manifest() -> None:
    print(f"[*] Sealing {len(CORE_FILES)} core files into MANIFEST.sha256...")
    lines = []
    for rel_path in sorted(CORE_FILES):
        abs_path = os.path.join(PROJECT_ROOT, rel_path)
        if not os.path.isfile(abs_path):
            print(f"    [!] Skipping missing file: {rel_path}")
            continue
        csum = sha256_file(abs_path)
        lines.append(f"{csum}  {rel_path.replace(os.sep, '/')}\n")
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"[OK] MANIFEST.sha256 successfully sealed ({len(lines)} entries).\n")


def check_gate_1_manifest() -> bool:
    print("[Gate 1] Checking Manifest Integrity (MANIFEST.sha256)...")
    if not os.path.isfile(MANIFEST_FILE):
        print("  [!] MANIFEST.sha256 not found. Run with --seal to initialize.")
        return False

    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    mismatches = []
    checked = 0
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        expected_sha, rel_path = parts[0], parts[1].replace("/", os.sep)
        abs_path = os.path.join(PROJECT_ROOT, rel_path)
        if not os.path.isfile(abs_path):
            mismatches.append(f"Missing file: {rel_path}")
            continue
        actual_sha = sha256_file(abs_path)
        if actual_sha != expected_sha:
            mismatches.append(f"Hash mismatch: {rel_path} (exp {expected_sha[:8]}..., got {actual_sha[:8]}...)")
        checked += 1

    if mismatches:
        for m in mismatches:
            print(f"  [FAIL] {m}")
        return False
    print(f"  [PASS] {checked}/{checked} core files matched manifest checksums.")
    return True


def check_gate_2_parameter_sync() -> bool:
    print("[Gate 2] Checking Fee & Parameter Synchronization...")
    import config as C
    from korean_engine import KoreanEngine

    errors = []
    # 1. Fees
    if C.TAKER_FEE_BPS != 5.0:
        errors.append(f"C.TAKER_FEE_BPS expected 5.0, got {C.TAKER_FEE_BPS}")
    if C.ROUNDTRIP_FEE_BPS != 10.0:
        errors.append(f"C.ROUNDTRIP_FEE_BPS expected 10.0, got {C.ROUNDTRIP_FEE_BPS}")

    # 2. Tolerances & Horizons
    if C.MAX_FOLLOWER_BOOK_AGE_SEC > 2.0 or C.MAX_FOLLOWER_BOOK_AGE_SEC <= 0:
        errors.append(f"C.MAX_FOLLOWER_BOOK_AGE_SEC expected in (0, 2.0]s (follower quote freshness gate), got {C.MAX_FOLLOWER_BOOK_AGE_SEC}")
    if C.MAX_SIGNAL_HOLD_SEC < 30.0:
        errors.append(f"C.MAX_SIGNAL_HOLD_SEC expected >= 30.0s, got {C.MAX_SIGNAL_HOLD_SEC}")
    if C.MIN_ORDER_USD != 10.0:
        errors.append(f"C.MIN_ORDER_USD expected 10.0, got {C.MIN_ORDER_USD}")
    if getattr(C, "TRADE_CONFIRM_LOOKBACK_SEC", 15.0) > 2.0:
        errors.append(f"C.TRADE_CONFIRM_LOOKBACK_SEC expected <= 2.0s, got {getattr(C, 'TRADE_CONFIRM_LOOKBACK_SEC', 15.0)}")

    # 3. Stop-Loss Configuration
    if not hasattr(C, "STOP_INVALIDATION_BPS") or C.STOP_INVALIDATION_BPS <= 0:
        errors.append("C.STOP_INVALIDATION_BPS missing or non-positive")
    if not hasattr(C, "STOP_ADVERSE_PNL_BPS") or C.STOP_ADVERSE_PNL_BPS <= 0:
        errors.append("C.STOP_ADVERSE_PNL_BPS missing or non-positive")
    if not hasattr(C, "ALPHA_DECAY_SEC") or C.ALPHA_DECAY_SEC <= 0:
        errors.append("C.ALPHA_DECAY_SEC missing or non-positive")

    # 4. Engine defaults reflection
    eng = KoreanEngine(emit_callback=lambda s: None)
    if eng.max_follower_book_age_sec > 2.0 or eng.max_follower_book_age_sec <= 0:
        errors.append(f"KoreanEngine.max_follower_book_age_sec expected in (0, 2.0]s, got {eng.max_follower_book_age_sec}")
    if eng.max_hold_sec < 30.0:
        errors.append(f"KoreanEngine.max_hold_sec expected >= 30.0s, got {eng.max_hold_sec}")
    if eng.min_order_usd != 10.0:
        errors.append(f"KoreanEngine.min_order_usd expected 10.0, got {eng.min_order_usd}")
    if eng.trade_confirm_lookback_sec > 2.0:
        errors.append(f"KoreanEngine.trade_confirm_lookback_sec expected <= 2.0s, got {eng.trade_confirm_lookback_sec}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print("  [PASS] Fee and engine parameter invariants verified.")
    return True


def check_gate_3_event_schemas() -> bool:
    print("[Gate 3] Checking Microstructure Data Contracts & Models...")
    from models import OrderBookDepth5, SunriseSignal, normalize_exchange_ts

    errors = []
    # 1. normalize_exchange_ts
    if normalize_exchange_ts(1792400000000000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on nanoseconds")
    if normalize_exchange_ts(1792400000000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on microseconds")
    if normalize_exchange_ts(1792400000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on milliseconds")
    if normalize_exchange_ts(1792400000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on seconds")

    # 2. SunriseSignal Stop-Loss Fields
    sig = SunriseSignal(
        id="sun_test",
        coin="SOL",
        leader_exchange="upbit",
        follower_exchange="binance",
        follower_contract="SOLUSDT",
        open_ts=1000.0,
        trigger_type="depth_drain",
        leader_mid=100.0,
        leader_best_ask=100.05,
        leader_spread_bps=5.0,
        leader_norm_spread_bps=5.0,
        leader_top5_ask_usd=1000.0,
        leader_norm_depth_usd=1000.0,
        leader_swept_usd=500.0,
        leader_imbalance_top5=0.5,
        drain_ratio=0.5,
        spread_blowout_ratio=1.0,
        follower_mid=100.0,
        follower_best_bid=99.95,
        follower_best_ask=100.05,
        follower_spread_bps=10.0,
        follower_top3_ask_usd=2000.0,
        follower_top3_bid_usd=2000.0,
        follower_imbalance_top3=0.5,
        kimchi_baseline_bps=0.0,
        pair_sigma_bps=2.0,
        raw_dislocation_bps=50.0,
        dev_z=3.0,
        gross_exec_edge_bps=35.0,
        net_exec_edge_bps=30.0,
    )
    sig_dict = sig.to_dict()
    for field_name in ("stop_hit", "stop_type", "stop_loss_bps"):
        if field_name not in sig_dict:
            errors.append(f"SunriseSignal.to_dict() missing stop-loss field: {field_name}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print("  [PASS] Data contracts and schema serialization verified.")
    return True


def check_gate_4_compilation() -> bool:
    print("[Gate 4] Auditing Bytecode Compilation across all Python files...")
    py_files = sorted(glob.glob(os.path.join(PROJECT_ROOT, "**", "*.py"), recursive=True))
    errors = []
    compiled = 0
    for pf in py_files:
        if "venv" in pf or ".git" in pf:
            continue
        try:
            py_compile.compile(pf, doraise=True)
            compiled += 1
        except py_compile.PyCompileError as e:
            errors.append(f"Compilation error in {pf}: {e}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print(f"  [PASS] {compiled} Python files compiled with zero syntax errors.")
    return True


def check_gate_5_unit_tests() -> bool:
    print("[Gate 5] Executing Complete Repository Unit Test Suite...")
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(PROJECT_ROOT, "tests"), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    if not result.wasSuccessful():
        print(f"  [FAIL] Unit tests failed: {len(result.failures)} failures, {len(result.errors)} errors.")
        return False
    print(f"  [PASS] All {result.testsRun} unit tests passed successfully.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Verification Arbiter.")
    parser.add_argument("--seal", action="store_true", help="Reseal MANIFEST.sha256 with current clean hashes.")
    args = parser.parse_args()

    print("======================================================================")
    print("      KOREAN SUNRISE - COMPREHENSIVE MULTI-GATE VERIFICATION          ")
    print("======================================================================\n")

    if args.seal:
        seal_manifest()

    gates = [
        ("Gate 1 (Manifest Integrity)", check_gate_1_manifest),
        ("Gate 2 (Parameter Synchronization)", check_gate_2_parameter_sync),
        ("Gate 3 (Data Contracts & Schemas)", check_gate_3_event_schemas),
        ("Gate 4 (Bytecode Syntax Audit)", check_gate_4_compilation),
        ("Gate 5 (Repository Unit Tests)", check_gate_5_unit_tests),
    ]

    all_passed = True
    for name, gate_func in gates:
        success = gate_func()
        print()
        if not success:
            all_passed = False
            break

    print("======================================================================")
    if all_passed:
        print("  >>> ALL 5 GATES PASSED: CODEBASE VERIFIED AND PRODUCTION-READY <<<  ")
        print("======================================================================")
        sys.exit(0)
    else:
        print("  >>> VERIFICATION FAILED: GATES REPORTED ANOMALIES <<<               ")
        print("======================================================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
