from __future__ import annotations

from news_ingestor.schemas import AuditPayload


def build_audit_payload(
    *,
    live_ids: list[str],
    db_present_ids: list[str],
    db_window_ids: list[str],
    audit_type: str,
) -> AuditPayload:
    live_latest = live_ids[0] if live_ids else ""
    db_latest = db_window_ids[0] if db_window_ids else ""
    db_present_set = set(db_present_ids)
    missing_in_db = [item_id for item_id in live_ids if item_id not in db_present_set]
    extra_in_db = [item_id for item_id in db_window_ids if item_id not in live_ids]

    status = "pass"
    if missing_in_db:
        status = "fail"
    elif extra_in_db:
        status = "warning"

    return AuditPayload(
        audit_type=audit_type,
        live_latest_item_id=live_latest,
        db_latest_item_id=db_latest,
        status=status,
        details={
            "live_ids": live_ids,
            "db_present_ids": db_present_ids,
            "db_window_ids": db_window_ids,
            "missing_in_db": missing_in_db,
            "extra_in_db": extra_in_db,
        },
    )
