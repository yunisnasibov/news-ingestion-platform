from __future__ import annotations

import argparse
import time

from .backfill import build_backfill_service
from .config import Settings
from .db import Database
from .process_control import ProcessController
from .service import SiteSyncService
from .sources import build_clients


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Azerbaijani news scraper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_once = subparsers.add_parser("sync-once", help="Run one scraping cycle")
    sync_once.add_argument(
        "--page-count",
        type=int,
        default=None,
        help="Override the number of listing pages or batches to scan",
    )
    sync_once.add_argument(
        "--source",
        default="all",
        help="Source to run: all, 1news.az, sonxeber.az, oxu.az, metbuat.az, report.az, azertag.az, yenixeber.az, teleqraf.az, ikisahil.az, islam.az, islamazeri.com, azerbaijan.az, axar.az, milli.az, azxeber.com, apa.az, xeberler.az, siyasetinfo.az, yeniazerbaycan.com, sia.az",
    )

    poll = subparsers.add_parser("poll", help="Run the scraper continuously")
    poll.add_argument(
        "--source",
        default="all",
        help="Source to run: all, 1news.az, sonxeber.az, oxu.az, metbuat.az, report.az, azertag.az, yenixeber.az, teleqraf.az, ikisahil.az, islam.az, islamazeri.com, azerbaijan.az, axar.az, milli.az, azxeber.com, apa.az, xeberler.az, siyasetinfo.az, yeniazerbaycan.com, sia.az",
    )

    backfill = subparsers.add_parser("backfill", help="Run one-shot historical backfill")
    backfill.add_argument("--source", required=True, help="Currently supported: azertag.az")
    backfill.add_argument("--max-pages", type=int, default=0, help="Optional hard page limit, 0 means unlimited")
    backfill.add_argument(
        "--stop-empty-pages",
        type=int,
        default=3,
        help="Stop after this many consecutive empty archive pages",
    )
    backfill.add_argument(
        "--wait-for-live-seconds",
        type=int,
        default=120,
        help="How long to wait for an in-flight live sync on the same source to finish",
    )

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Run historical backfill first, then hand off to continuous polling for the same source",
    )
    bootstrap.add_argument("--source", required=True, help="Currently supported: azertag.az")
    bootstrap.add_argument("--max-pages", type=int, default=0, help="Optional hard page limit, 0 means unlimited")
    bootstrap.add_argument(
        "--stop-empty-pages",
        type=int,
        default=3,
        help="Stop after this many consecutive empty archive pages",
    )
    bootstrap.add_argument(
        "--wait-for-live-seconds",
        type=int,
        default=120,
        help="How long to wait for an in-flight live sync on the same source to finish",
    )


    subparsers.add_parser("start", help="Start the scraper in the background")
    subparsers.add_parser("stop", help="Stop the background scraper")
    subparsers.add_parser("status", help="Show background scraper status")
    subparsers.add_parser("stats", help="Show database stats")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    settings = Settings()
    settings.ensure_paths()
    controller = ProcessController(settings)

    if args.command == "start":
        success, message = controller.start()
        print(message)
        return 0 if success else 1

    if args.command == "stop":
        success, message = controller.stop()
        print(message)
        return 0 if success else 1

    if args.command == "status":
        print(controller.status())
        return 0

    database = Database(settings)
    database.initialize()
    services = _build_services(settings, database, getattr(args, "source", "all"))

    try:
        if args.command == "sync-once":
            exit_code = 0
            page_count = args.page_count or settings.listing_page_count
            for service in services:
                summary = service.sync_once(page_count=args.page_count)
                print(service._format_summary(summary, page_count))
                for error in summary.errors:
                    print(f"error[{service.client.source_name}]: {error}")
                if summary.errors:
                    exit_code = 1
            return exit_code

        if args.command == "backfill":
            backfill_service = build_backfill_service(settings, database, args.source)
            summary = backfill_service.run(
                max_pages=args.max_pages,
                stop_after_empty_pages=args.stop_empty_pages,
                wait_for_live_seconds=args.wait_for_live_seconds,
            )
            print(_format_backfill_summary(summary))
            for error in summary.errors:
                print(f"error[{summary.source_name}]: {error}")
            return 0 if not summary.errors else 1

        if args.command == "bootstrap":
            backfill_service = build_backfill_service(settings, database, args.source)
            summary = backfill_service.run(
                max_pages=args.max_pages,
                stop_after_empty_pages=args.stop_empty_pages,
                wait_for_live_seconds=args.wait_for_live_seconds,
            )
            print(_format_backfill_summary(summary), flush=True)
            for error in summary.errors:
                print(f"error[{summary.source_name}]: {error}", flush=True)
            if summary.errors:
                return 1
            print(f"bootstrap_handoff source={args.source} mode=live_poll", flush=True)
            _poll_all_sources(settings, _build_services(settings, database, args.source))
            return 0

        if args.command == "poll":
            _poll_all_sources(settings, services)
            return 0

        if args.command == "stats":
            print(f"database={settings.database_display_name()}")
            print(f"articles={database.get_article_count()}")
            for row in database.get_article_counts_by_source():
                print(f"{row['source_name']}={row['count']}")
            return 0
    finally:
        database.close()

    return 1


def _format_backfill_summary(summary) -> str:
    error_suffix = f", errors={len(summary.errors)}" if summary.errors else ""
    return (
        "backfill_complete"
        f" source={summary.source_name}"
        f" pages_scanned={summary.pages_scanned}"
        f" last_page={summary.last_page_scanned}"
        f" listing_candidates={summary.listing_candidates}"
        f" inserted_articles={summary.inserted_articles}"
        f" updated_articles={summary.updated_articles}"
        f" skipped_existing={summary.skipped_existing_articles}"
        f" archive_errors={summary.archive_errors}"
        f" stopped_reason={summary.stopped_reason}"
        f"{error_suffix}"
    )


def _build_services(
    settings: Settings,
    database: Database,
    requested_source: str,
) -> list[SiteSyncService]:
    clients = build_clients(settings)
    if requested_source == "all":
        return [SiteSyncService(settings, database, clients[name]) for name in sorted(clients)]

    if requested_source not in clients:
        raise SystemExit(f"unknown_source={requested_source}")
    return [SiteSyncService(settings, database, clients[requested_source])]


def _poll_all_sources(settings: Settings, services: list[SiteSyncService]) -> None:
    cycle = 0
    while True:
        cycle += 1
        page_count = settings.listing_page_count
        if cycle % settings.reconcile_every_cycles == 0:
            page_count = settings.reconcile_page_count

        for service in services:
            summary = service.sync_once(page_count=page_count)
            print(service._format_summary(summary, page_count), flush=True)
            for error in summary.errors:
                print(f"error[{service.client.source_name}]: {error}", flush=True)

        time.sleep(settings.poll_interval_seconds)
