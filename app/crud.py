from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session

from .models import Transaction


def create_transaction(
    db: Session,
    *,
    type: str,
    date: dt.date,
    amount_pkr: int,
    category: str,
    name: str | None,
    bill_no: str | None,
    notes: str | None,
) -> Transaction:
    tx = Transaction(
        type=type,
        date=date,
        amount_pkr=amount_pkr,
        category=category,
        name=name or None,
        bill_no=bill_no or None,
        notes=notes or None,
        is_deleted=False,
    )
    db.add(tx)
    db.commit()
    db.refresh(tx)
    return tx


def get_transaction(db: Session, tx_id: int) -> Transaction | None:
    return db.execute(select(Transaction).where(Transaction.id == tx_id)).scalar_one_or_none()


def update_transaction(
    db: Session,
    tx: Transaction,
    *,
    date: dt.date,
    amount_pkr: int,
    category: str,
    name: str | None,
    bill_no: str | None,
    notes: str | None,
) -> Transaction:
    tx.date = date
    tx.amount_pkr = amount_pkr
    tx.category = category
    tx.name = name or None
    tx.bill_no = bill_no or None
    tx.notes = notes or None
    db.add(tx)
    db.commit()
    db.refresh(tx)
    return tx


def soft_delete_transaction(db: Session, tx: Transaction) -> None:
    tx.is_deleted = True
    db.add(tx)
    db.commit()


def build_filters(
    *,
    from_date: dt.date | None,
    to_date: dt.date | None,
    type: str | None,
    category: str | None,
    name: str | None,
    q: str | None,
    include_deleted: bool = False,
):
    clauses = []
    if not include_deleted:
        clauses.append(Transaction.is_deleted.is_(False))

    if from_date:
        clauses.append(Transaction.date >= from_date)
    if to_date:
        clauses.append(Transaction.date <= to_date)

    if type and type in {"incoming", "outgoing"}:
        clauses.append(Transaction.type == type)

    if category:
        clauses.append(Transaction.category == category)

    if name:
        clauses.append(func.lower(Transaction.name).like(f"%{name.lower()}%"))

    if q:
        ql = q.lower()
        clauses.append(
            or_(
                func.lower(Transaction.notes).like(f"%{ql}%"),
                func.lower(Transaction.bill_no).like(f"%{ql}%"),
                func.lower(Transaction.category).like(f"%{ql}%"),
                func.lower(Transaction.name).like(f"%{ql}%"),
            )
        )

    return and_(*clauses) if clauses else None


def list_transactions(
    db: Session,
    *,
    from_date: dt.date | None,
    to_date: dt.date | None,
    type: str | None,
    category: str | None,
    name: str | None,
    q: str | None,
    limit: int = 500,
):
    where_clause = build_filters(
        from_date=from_date,
        to_date=to_date,
        type=type,
        category=category,
        name=name,
        q=q,
    )

    stmt = select(Transaction).order_by(Transaction.date.desc(), Transaction.id.desc())
    if where_clause is not None:
        stmt = stmt.where(where_clause)
    stmt = stmt.limit(limit)

    return list(db.execute(stmt).scalars().all())


def totals(db: Session, *, from_date: dt.date | None, to_date: dt.date | None, type: str | None, category: str | None, name: str | None, q: str | None):
    where_clause = build_filters(
        from_date=from_date,
        to_date=to_date,
        type=type,
        category=category,
        name=name,
        q=q,
    )

    stmt = select(
        func.coalesce(
            func.sum(case((Transaction.type == "incoming", Transaction.amount_pkr), else_=0)),
            0,
        ).label("incoming"),
        func.coalesce(
            func.sum(case((Transaction.type == "outgoing", Transaction.amount_pkr), else_=0)),
            0,
        ).label("outgoing"),
    )
    if where_clause is not None:
        stmt = stmt.where(where_clause)

    row = db.execute(stmt).one()
    incoming = int(row.incoming or 0)
    outgoing = int(row.outgoing or 0)
    return incoming, outgoing, incoming - outgoing


def distinct_names(db: Session, *, limit: int = 200) -> list[str]:
    stmt = (
        select(func.min(Transaction.name))
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.name.is_not(None))
        .where(func.length(func.trim(Transaction.name)) > 0)
        .group_by(func.lower(Transaction.name))
        .order_by(func.lower(Transaction.name))
        .limit(limit)
    )
    rows = db.execute(stmt).all()
    return [r[0] for r in rows if r[0]]


def distinct_categories(db: Session, *, limit: int = 200) -> list[str]:
    stmt = (
        select(func.min(Transaction.category))
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.category.is_not(None))
        .where(func.length(func.trim(Transaction.category)) > 0)
        .group_by(func.lower(Transaction.category))
        .order_by(func.lower(Transaction.category))
        .limit(limit)
    )
    rows = db.execute(stmt).all()
    return [r[0] for r in rows if r[0]]
