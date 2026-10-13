"""Shared command-line entry point for the bronze ingesters."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from pathlib import Path

import httpx
from nemweb import BRONZE_DIR, LOG, ListingError, ReportSpec, run


def main_for(spec: ReportSpec, doc: str | None = None) -> Callable[[list[str] | None], int]:
    """Build a ``main`` function for one report."""

    def main(argv: list[str] | None = None) -> int:
        summary = (doc or "").strip().splitlines()[0] if doc else f"Ingest {spec.key}."
        parser = argparse.ArgumentParser(description=summary)
        parser.add_argument("--limit", type=int, default=None, help="ingest at most N files")
        parser.add_argument("--list-only", action="store_true", help="print pending files")
        parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR, help="output directory")
        parser.add_argument("--verbose", action="store_true", help="enable debug logging")
        args = parser.parse_args(argv)

        logging.basicConfig(
            level=logging.DEBUG if args.verbose else logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )

        try:
            run(spec, limit=args.limit, list_only=args.list_only, bronze_dir=args.bronze_dir)
        except httpx.HTTPError as exc:
            LOG.error("[%s] could not reach AEMO: %s", spec.key, exc)
            return 1
        except ListingError as exc:
            LOG.error("[%s] %s", spec.key, exc)
            return 1
        return 0

    return main
