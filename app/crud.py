from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session

from .models import Employee, Transaction, WeeklyAssignment


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
    employee_id: int | None = None,
    employee_tx_type: str | None = None,
    payment_method: str | None = None,
    assignment_id: int | None = None,
    reference: str | None = None,
) -> Transaction:
    tx = Transaction(
        type=type,
        date=date,
        amount_pkr=amount_pkr,
        category=category,
        name=name or None,
        bill_no=bill_no or None,
        notes=notes or None,
        employee_id=employee_id,
        employee_tx_type=employee_tx_type or None,
        payment_method=payment_method or None,
        assignment_id=assignment_id,
        reference=reference or None,
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
    employee_id: int | None = None,
    employee_tx_type: str | None = None,
    payment_method: str | None = None,
    assignment_id: int | None = None,
    reference: str | None = None,
) -> Transaction:
    tx.date = date
    tx.amount_pkr = amount_pkr
    tx.category = category
    tx.name = name or None
    tx.bill_no = bill_no or None
    tx.notes = notes or None
    tx.employee_id = employee_id
    tx.employee_tx_type = employee_tx_type or None
    tx.payment_method = payment_method or None
    tx.assignment_id = assignment_id
    tx.reference = reference or None
    db.add(tx)
    db.commit()
    db.refresh(tx)
    return tx


def list_employees(db: Session, *, status: str | None = None) -> list[Employee]:
    stmt = select(Employee).order_by(Employee.status.asc(), Employee.full_name.asc())
    if status:
        stmt = stmt.where(Employee.status == status)
    return list(db.execute(stmt).scalars().all())


def get_employee(db: Session, employee_id: int) -> Employee | None:
    return db.execute(select(Employee).where(Employee.id == employee_id)).scalar_one_or_none()


def create_employee(
    db: Session,
    *,
    full_name: str,
    father_name: str | None,
    cnic: str | None,
    mobile_number: str | None,
    address: str | None,
    emergency_contact: str | None,
    joining_date: dt.date,
    status: str,
    category: str,
    work_type: str,
    role_description: str | None,
    payment_rate: int | None,
    profile_image_url: str,
) -> Employee:
    emp = Employee(
        full_name=full_name,
        father_name=father_name or None,
        cnic=cnic or None,
        mobile_number=mobile_number or None,
        address=address or None,
        emergency_contact=emergency_contact or None,
        joining_date=joining_date,
        status=status,
        category=category,
        work_type=work_type,
        role_description=role_description or None,
        payment_rate=payment_rate,
        profile_image_url=profile_image_url or None,
    )
    db.add(emp)
    db.commit()
    db.refresh(emp)
    return emp


def update_employee(
    db: Session,
    emp: Employee,
    *,
    full_name: str,
    father_name: str | None,
    cnic: str | None,
    mobile_number: str | None,
    address: str | None,
    emergency_contact: str | None,
    joining_date: dt.date,
    status: str,
    category: str,
    work_type: str,
    role_description: str | None,
    payment_rate: int | None,
    profile_image_url: str,
) -> Employee:
    emp.full_name = full_name
    emp.father_name = father_name or None
    emp.cnic = cnic or None
    emp.mobile_number = mobile_number or None
    emp.address = address or None
    emp.emergency_contact = emergency_contact or None
    emp.joining_date = joining_date
    emp.status = status
    emp.category = category
    emp.work_type = work_type
    emp.role_description = role_description or None
    emp.payment_rate = payment_rate
    emp.profile_image_url = profile_image_url or None
    db.add(emp)
    db.commit()
    db.refresh(emp)
    return emp


def list_assignments_for_employee(db: Session, *, employee_id: int, limit: int = 100) -> list[WeeklyAssignment]:
    stmt = (
        select(WeeklyAssignment)
        .where(WeeklyAssignment.employee_id == employee_id)
        .order_by(WeeklyAssignment.week_start.desc(), WeeklyAssignment.id.desc())
        .limit(limit)
    )
    return list(db.execute(stmt).scalars().all())


def create_assignment(
    db: Session,
    *,
    employee_id: int,
    week_start: dt.date,
    week_end: dt.date,
    description: str,
    quantity: int | None,
    status: str,
) -> WeeklyAssignment:
    a = WeeklyAssignment(
        employee_id=employee_id,
        week_start=week_start,
        week_end=week_end,
        description=description,
        quantity=quantity,
        status=status,
        is_locked=status == "completed",
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    return a


def employee_transactions(db: Session, *, employee_id: int, limit: int = 500) -> list[Transaction]:
    emp = get_employee(db, employee_id)
    if not emp:
        return []

    legacy_name_clause = and_(
        Transaction.employee_id.is_(None),
        Transaction.name.is_not(None),
        func.lower(func.trim(Transaction.name)) == func.lower(func.trim(emp.full_name)),
    )

    stmt = (
        select(Transaction)
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.type == "outgoing")
        .where(or_(Transaction.employee_id == employee_id, legacy_name_clause))
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .limit(limit)
    )
    return list(db.execute(stmt).scalars().all())


def employee_financial_summary(db: Session, *, employee_id: int) -> dict[str, int]:
    emp = get_employee(db, employee_id)
    if not emp:
        return {"advance": 0, "paid": 0, "advance_balance": 0, "count": 0}

    legacy_name_clause = and_(
        Transaction.employee_id.is_(None),
        Transaction.name.is_not(None),
        func.lower(func.trim(Transaction.name)) == func.lower(func.trim(emp.full_name)),
    )

    stmt = (
        select(
            func.coalesce(
                func.sum(case((Transaction.employee_tx_type == "advance", Transaction.amount_pkr), else_=0)),
                0,
            ).label("advance"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            or_(
                                Transaction.employee_tx_type.in_(["salary", "per_work"]),
                                Transaction.employee_tx_type.is_(None),
                            ),
                            Transaction.amount_pkr,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("paid"),
            func.coalesce(func.count(Transaction.id), 0).label("count"),
        )
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.type == "outgoing")
        .where(or_(Transaction.employee_id == employee_id, legacy_name_clause))
    )
    row = db.execute(stmt).one()
    advance = int(row.advance or 0)
    paid = int(row.paid or 0)
    count = int(row.count or 0)
    advance_balance = max(0, advance - paid)
    return {"advance": advance, "paid": paid, "advance_balance": advance_balance, "count": count}


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
