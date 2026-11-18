from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from fastapi import HTTPException

from app import gating
from app.db import connect
from app.repositories import settings as settings_repo
from app.repositories import facts as fact_repo
from app.repositories import batches as batch_repo


def list_facts(business_day: str | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        day = business_day or settings_repo.get(conn, "business_day")
        return fact_repo.list_facts(conn, day)
    finally:
        conn.close()


def preview(business_day: str | None = None) -> dict[str, Any]:
    conn = connect()
    try:
        day = business_day or settings_repo.get(conn, "business_day")
        opens = fact_repo.open_facts_for_day(conn, day)
        total = round(sum(float(f["amount"]) for f in opens), 2)
        return {
            "business_day": day,
            "open_count": len(opens),
            "open_total": total,
            "already_sealed": fact_repo.day_is_sealed(conn, day),
            "facts": opens,
        }
    finally:
        conn.close()


def seal(body: dict[str, Any]) -> dict[str, Any]:
    read_conn = connect()
    try:
        day = body.get("business_day") or settings_repo.get(read_conn, "business_day")
    finally:
        read_conn.close()

    # 钉扎优先：先取业务日写锁，持锁期间两个入口的核销写一律阻塞在闸外，
    # 因此下面扫描到的 open 集合即「锁前快照」，不会再被并发核销改写。
    with gating.day_write_lock(day):
        conn = connect(immediate=True)
        try:
            if fact_repo.day_is_sealed(conn, day):
                raise HTTPException(409, f"业务日 {day} 已有 sealed 钉扎批")
            snapshot = fact_repo.open_facts_for_day(conn, day)
            if not snapshot:
                raise HTTPException(400, "无可钉扎的 open 核销事实")

            fact_ids = [int(f["id"]) for f in snapshot]
            snapshot_codes = sorted(str(f["coupon_code"]) for f in snapshot)
            snapshot_amounts = sorted(float(f["amount"]) for f in snapshot)
            total = round(sum(snapshot_amounts), 2)

            bid = batch_repo.create_batch(
                conn,
                business_day=day,
                status="sealed",
                fact_count=len(snapshot),
                total_amount=total,
                sealed_at=datetime.now().replace(microsecond=0).isoformat(),
                note=body.get("note") or "",
            )

            # 闸内停顿钩子：批头已建、事实尚未全部置 sealed，演示期间核销写仍堵在闸外。
            gating.gate_pause("SEAL")

            # 仅 open 事实可入批；任何一行状态偏离快照即抛错，整笔回滚。
            fact_repo.mark_batched(conn, fact_ids, bid)

            # 提交前对账：批头笔数/合计必须与锁前快照逐一对上，否则整笔失败。
            detail = fact_repo.facts_for_batch(conn, bid)
            detail_codes = sorted(str(f["coupon_code"]) for f in detail)
            detail_amounts = sorted(float(f["amount"]) for f in detail)
            header = batch_repo.get_batch(conn, bid)
            assert header is not None
            if (
                len(detail) != len(snapshot)
                or detail_codes != snapshot_codes
                or detail_amounts != snapshot_amounts
                or int(header["fact_count"]) != len(snapshot)
                or round(float(header["total_amount"]), 2) != total
            ):
                raise RuntimeError("钉扎对账失败：批头/明细与锁前快照不一致，整笔回滚")

            if gating.seal_failure_injected():
                # 故障注入：模拟提交前崩溃，验证批头/明细/日标记同成同败、事实仍 open。
                raise RuntimeError("VOUCHPIN_SEAL_FAIL 注入：钉扎在提交前失败")

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except sqlite3.OperationalError as e:
            conn.rollback()
            raise HTTPException(409, f"业务日 {day} 写锁竞争，钉扎被拒绝: {e}") from e
        except Exception as e:
            # 对账失败/故障注入：批头、明细、日 sealed 标记随事务整体回滚。
            conn.rollback()
            raise HTTPException(500, f"钉扎失败，整笔已回滚: {e}") from e
        finally:
            conn.close()

    return {
        "batch_id": bid,
        "business_day": day,
        "fact_count": len(snapshot),
        "total_amount": total,
        "status": "sealed",
        "snapshot": [
            {"fact_id": int(f["id"]), "coupon_code": f["coupon_code"], "amount": float(f["amount"])}
            for f in snapshot
        ],
    }


def list_batches() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_batches(conn)
    finally:
        conn.close()


def get_batch_detail(batch_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        header = batch_repo.get_batch(conn, batch_id)
        if header is None:
            raise HTTPException(404, f"批次 {batch_id} 不存在")
        detail = fact_repo.facts_for_batch(conn, batch_id)
        codes = sorted(str(f["coupon_code"]) for f in detail)
        amounts = sorted(float(f["amount"]) for f in detail)
        detail_total = round(sum(amounts), 2)
        return {
            "batch": header,
            "detail": detail,
            "reconcile": {
                "detail_count": len(detail),
                "header_count": int(header["fact_count"]),
                "detail_total": detail_total,
                "header_total": round(float(header["total_amount"]), 2),
                "coupon_codes": codes,
                "amounts": amounts,
                "matches": len(detail) == int(header["fact_count"])
                and detail_total == round(float(header["total_amount"]), 2),
            },
        }
    finally:
        conn.close()
