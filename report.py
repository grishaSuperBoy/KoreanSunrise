"""
report.py - Root shortcut for Korean Sunrise CLI Report Viewer.
Usage:
  python report.py           # View latest report in Markdown
  python report.py --json    # View latest report as JSON
  python report.py --generate # Generate fresh report
  python report.py --list    # List all archived reports
  python report.py --analyze # Run full multi-strategy terminal analyzer
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from tools.report import main

if __name__ == "__main__":
    main()
