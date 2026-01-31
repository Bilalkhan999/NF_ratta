from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session

from .models import (
    BedSize,
    Employee,
    FoamBrand,
    FoamModel,
    FoamThickness,
    FoamVariant,
    FurnitureItem,
    FurnitureVariant,
    InventoryCategory,
    StockMovement,
    Transaction,
    WeeklyAssignment,
)


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


def ensure_inventory_seed(db: Session) -> None:
    furniture_root = _upsert_category(db, type="FURNITURE", parent_id=None, name="Furniture")
    foam_root = _upsert_category(db, type="FOAM", parent_id=None, name="Foam")

    bed_sets = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Bed Set")

    for name in [
        "Single Bed",
        "Double Bed",
        "Almari",
        "Showcase",
        "Side Table",
        "Dressing Table",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name=name)

    for name in ["Cushion Bed Set", "Tahli Bed Set", "Kicker + V-Board", "Other"]:
        _upsert_category(db, type="FURNITURE", parent_id=bed_sets.id, name=name)

    _upsert_category(db, type="FOAM", parent_id=foam_root.id, name="Mattress / Foam Inventory")

    _upsert_bed_size(db, label="Single Bed (42×78)", width_in=42, length_in=78, width_ft_x100=350, length_ft_x100=650, sort_order=10)
    _upsert_bed_size(db, label="Single Slim (39×78)", width_in=39, length_in=78, width_ft_x100=325, length_ft_x100=650, sort_order=20)
    _upsert_bed_size(db, label="Single Slim (36×72)", width_in=36, length_in=72, width_ft_x100=300, length_ft_x100=600, sort_order=30)
    _upsert_bed_size(db, label="Double / Queen 1 (60×78)", width_in=60, length_in=78, width_ft_x100=500, length_ft_x100=650, sort_order=40)
    _upsert_bed_size(db, label="Super Queen / Queen 2 (66×78)", width_in=66, length_in=78, width_ft_x100=550, length_ft_x100=650, sort_order=50)
    _upsert_bed_size(db, label="King (72×78)", width_in=72, length_in=78, width_ft_x100=600, length_ft_x100=650, sort_order=60)
    _upsert_bed_size(db, label="King XL (78×84)", width_in=78, length_in=84, width_ft_x100=650, length_ft_x100=700, sort_order=70)
    _upsert_bed_size(db, label="Custom Size (manual)", width_in=0, length_in=0, width_ft_x100=None, length_ft_x100=None, sort_order=999)

    for i, inches in enumerate([4, 5, 6, 8, 10, 12], start=1):
        _upsert_thickness(db, inches=inches, sort_order=i)

    brand_ids: dict[str, int] = {}
    for b in [
        "MoltyFoam",
        "Diamond Supreme",
        "Cannon Primax",
        "Alkhair",
        "Al Shafi",
        "DuraFoam",
        "i-Foam",
        "Mehran",
        "Unifoam",
        "Other",
    ]:
        brand_ids[b] = _upsert_foam_brand(db, name=b).id

    _upsert_foam_model(db, brand_id=brand_ids["MoltyFoam"], name="Master")
    _upsert_foam_model(db, brand_id=brand_ids["MoltyFoam"], name="Celeste")
    _upsert_foam_model(db, brand_id=brand_ids["MoltyFoam"], name="Bravo")
    _upsert_foam_model(db, brand_id=brand_ids["MoltyFoam"], name="MoltyOrtho")
    _upsert_foam_model(db, brand_id=brand_ids["MoltyFoam"], name="MoltySpring")
    _upsert_foam_model(db, brand_id=brand_ids["Diamond Supreme"], name="Supreme Series")
    _upsert_foam_model(db, brand_id=brand_ids["Diamond Supreme"], name="Mr. Foam")
    _upsert_foam_model(db, brand_id=brand_ids["Unifoam"], name="Shaheen Foam")
    _upsert_foam_model(db, brand_id=brand_ids["Unifoam"], name="Dream Foam")
    _upsert_foam_model(db, brand_id=brand_ids["Cannon Primax"], name="Primax")
    _upsert_foam_model(db, brand_id=brand_ids["Cannon Primax"], name="Primax Bachat")


def _upsert_category(db: Session, *, type: str, parent_id: int | None, name: str) -> InventoryCategory:
    stmt = select(InventoryCategory).where(
        InventoryCategory.type == type,
        InventoryCategory.parent_id.is_(None) if parent_id is None else InventoryCategory.parent_id == parent_id,
        func.lower(InventoryCategory.name) == name.lower(),
    )
    c = db.execute(stmt).scalar_one_or_none()
    if c:
        if not c.is_active:
            c.is_active = True
            db.add(c)
            db.commit()
            db.refresh(c)
        return c
    c = InventoryCategory(type=type, parent_id=parent_id, name=name, is_active=True)
    db.add(c)
    db.commit()
    db.refresh(c)
    return c


def _upsert_bed_size(
    db: Session,
    *,
    label: str,
    width_in: int,
    length_in: int,
    width_ft_x100: int | None,
    length_ft_x100: int | None,
    sort_order: int,
) -> BedSize:
    stmt = select(BedSize).where(BedSize.width_in == width_in, BedSize.length_in == length_in)
    s = db.execute(stmt).scalar_one_or_none()
    if s:
        s.label = label
        s.width_ft_x100 = width_ft_x100
        s.length_ft_x100 = length_ft_x100
        s.sort_order = sort_order
        s.is_active = True
        db.add(s)
        db.commit()
        db.refresh(s)
        return s
    s = BedSize(
        label=label,
        width_in=width_in,
        length_in=length_in,
        width_ft_x100=width_ft_x100,
        length_ft_x100=length_ft_x100,
        sort_order=sort_order,
        is_active=True,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    return s


def _upsert_thickness(db: Session, *, inches: int, sort_order: int) -> FoamThickness:
    stmt = select(FoamThickness).where(FoamThickness.inches == inches)
    t = db.execute(stmt).scalar_one_or_none()
    if t:
        t.sort_order = sort_order
        t.is_active = True
        db.add(t)
        db.commit()
        db.refresh(t)
        return t
    t = FoamThickness(inches=inches, sort_order=sort_order, is_active=True)
    db.add(t)
    db.commit()
    db.refresh(t)
    return t


def _upsert_foam_brand(db: Session, *, name: str) -> FoamBrand:
    stmt = select(FoamBrand).where(func.lower(FoamBrand.name) == name.lower())
    b = db.execute(stmt).scalar_one_or_none()
    if b:
        if not b.is_active:
            b.is_active = True
            db.add(b)
            db.commit()
            db.refresh(b)
        return b
    b = FoamBrand(name=name, is_active=True)
    db.add(b)
    db.commit()
    db.refresh(b)
    return b


def _upsert_foam_model(db: Session, *, brand_id: int, name: str) -> FoamModel:
    stmt = select(FoamModel).where(FoamModel.brand_id == brand_id, func.lower(FoamModel.name) == name.lower())
    m = db.execute(stmt).scalar_one_or_none()
    if m:
        if not m.is_active:
            m.is_active = True
            db.add(m)
            db.commit()
            db.refresh(m)
        return m
    m = FoamModel(brand_id=brand_id, name=name, is_active=True)
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


def list_inventory_categories(db: Session, *, type: str, parent_id: int | None = None) -> list[InventoryCategory]:
    stmt = select(InventoryCategory).where(InventoryCategory.is_active.is_(True), InventoryCategory.type == type)
    if parent_id is None:
        stmt = stmt.where(InventoryCategory.parent_id.is_(None))
    else:
        stmt = stmt.where(InventoryCategory.parent_id == parent_id)
    stmt = stmt.order_by(InventoryCategory.name.asc())
    return list(db.execute(stmt).scalars().all())


def get_inventory_category(db: Session, *, type: str, name: str, parent_id: int | None = None) -> InventoryCategory | None:
    stmt = select(InventoryCategory).where(
        InventoryCategory.is_active.is_(True),
        InventoryCategory.type == type,
        func.lower(InventoryCategory.name) == name.lower(),
    )
    if parent_id is None:
        stmt = stmt.where(InventoryCategory.parent_id.is_(None))
    else:
        stmt = stmt.where(InventoryCategory.parent_id == parent_id)
    return db.execute(stmt).scalar_one_or_none()


def _recompute_furniture_item_status(db: Session, *, furniture_item_id: int) -> None:
    item = db.execute(select(FurnitureItem).where(FurnitureItem.id == furniture_item_id)).scalar_one_or_none()
    if not item:
        return
    if (item.status or "").upper() == "MADE_TO_ORDER":
        return
    total_qty = db.execute(
        select(func.coalesce(func.sum(FurnitureVariant.qty_on_hand), 0)).where(
            FurnitureVariant.is_active.is_(True),
            FurnitureVariant.furniture_item_id == furniture_item_id,
        )
    ).scalar_one()
    total_qty_int = int(total_qty or 0)
    item.status = "OUT_OF_STOCK" if total_qty_int <= 0 else "IN_STOCK"
    db.add(item)
    db.commit()


def list_bed_sizes(db: Session) -> list[BedSize]:
    stmt = select(BedSize).where(BedSize.is_active.is_(True)).order_by(BedSize.sort_order.asc(), BedSize.width_in.asc())
    return list(db.execute(stmt).scalars().all())


def list_thicknesses(db: Session) -> list[FoamThickness]:
    stmt = select(FoamThickness).where(FoamThickness.is_active.is_(True)).order_by(FoamThickness.sort_order.asc(), FoamThickness.inches.asc())
    return list(db.execute(stmt).scalars().all())


def list_foam_brands(db: Session) -> list[FoamBrand]:
    stmt = select(FoamBrand).where(FoamBrand.is_active.is_(True)).order_by(FoamBrand.name.asc())
    return list(db.execute(stmt).scalars().all())


def list_foam_models(db: Session, *, brand_id: int | None = None) -> list[FoamModel]:
    stmt = select(FoamModel).where(FoamModel.is_active.is_(True))
    if brand_id is not None:
        stmt = stmt.where(FoamModel.brand_id == brand_id)
    stmt = stmt.order_by(FoamModel.brand_id.asc(), FoamModel.name.asc())
    return list(db.execute(stmt).scalars().all())


def create_furniture_item(
    db: Session,
    *,
    name: str,
    sku: str,
    material_type: str,
    color_finish: str | None,
    status: str,
    category_id: int,
    sub_category_id: int | None,
    notes: str | None,
) -> FurnitureItem:
    item = FurnitureItem(
        name=name,
        sku=sku,
        material_type=material_type,
        color_finish=color_finish,
        status=status,
        category_id=category_id,
        sub_category_id=sub_category_id,
        notes=notes,
        is_active=True,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def list_furniture_items(db: Session, *, q: str | None = None, limit: int = 200) -> list[FurnitureItem]:
    stmt = select(FurnitureItem).where(FurnitureItem.is_active.is_(True)).order_by(FurnitureItem.id.desc())
    if q:
        stmt = stmt.where(func.lower(FurnitureItem.name).like(f"%{q.lower()}%"))
    stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def list_furniture_items_filtered(
    db: Session,
    *,
    q: str | None = None,
    category_id: int | None = None,
    limit: int = 200,
) -> list[FurnitureItem]:
    stmt = select(FurnitureItem).where(FurnitureItem.is_active.is_(True)).order_by(FurnitureItem.id.desc())
    if category_id is not None:
        stmt = stmt.where(FurnitureItem.category_id == category_id)
    if q:
        stmt = stmt.where(func.lower(FurnitureItem.name).like(f"%{q.lower()}%"))
    stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def furniture_cards(db: Session, *, items: list[FurnitureItem]) -> list[dict]:
    if not items:
        return []

    item_ids = [i.id for i in items]
    variants = list(
        db.execute(
            select(FurnitureVariant).where(
                FurnitureVariant.is_active.is_(True),
                FurnitureVariant.furniture_item_id.in_(item_ids),
            )
        ).scalars().all()
    )

    by_item: dict[int, list[FurnitureVariant]] = {}
    for v in variants:
        by_item.setdefault(v.furniture_item_id, []).append(v)

    out: list[dict] = []
    for it in items:
        vs = by_item.get(it.id, [])
        total_qty = sum(int(v.qty_on_hand or 0) for v in vs)
        min_cost = min((int(v.cost_price_pkr or 0) for v in vs), default=0)
        min_sale = min((int(v.sale_price_pkr or 0) for v in vs), default=0)
        any_low = False
        for v in vs:
            qty = int(v.qty_on_hand or 0)
            rl = int(v.reorder_level or 0)
            if rl > 0:
                if qty <= rl:
                    any_low = True
                    break
            else:
                if qty < 3:
                    any_low = True
                    break

        is_mto = (it.status or "").upper() == "MADE_TO_ORDER"
        is_out = (not is_mto) and total_qty <= 0
        badge = "Made to Order" if is_mto else ("Out of Stock" if is_out else ("Low Stock" if any_low else "In Stock"))

        out.append(
            {
                "item": it,
                "total_qty": total_qty,
                "min_cost": min_cost,
                "min_sale": min_sale,
                "badge": badge,
            }
        )
    return out


def foam_variant_cards(
    db: Session,
    *,
    q: str | None = None,
    brand_id: int | None = None,
    limit: int = 200,
) -> list[dict]:
    stmt = (
        select(FoamVariant, FoamModel, FoamBrand, BedSize, FoamThickness)
        .join(FoamModel, FoamModel.id == FoamVariant.foam_model_id)
        .join(FoamBrand, FoamBrand.id == FoamModel.brand_id)
        .join(BedSize, BedSize.id == FoamVariant.bed_size_id)
        .join(FoamThickness, FoamThickness.id == FoamVariant.thickness_id)
        .where(FoamVariant.is_active.is_(True), FoamModel.is_active.is_(True), FoamBrand.is_active.is_(True))
    )
    if brand_id is not None:
        stmt = stmt.where(FoamModel.brand_id == brand_id)
    if q:
        stmt = stmt.where(func.lower(FoamModel.name).like(f"%{q.lower()}%"))
    stmt = stmt.order_by(FoamVariant.qty_on_hand.asc(), FoamVariant.id.desc()).limit(limit)
    rows = list(db.execute(stmt).all())

    out: list[dict] = []
    for v, model, brand, size, thick in rows:
        qty = int(v.qty_on_hand or 0)
        rl = int(v.reorder_level or 0)
        is_out = qty <= 0
        is_low = (qty <= rl) if rl > 0 else (qty < 3)
        badge = "Out of Stock" if is_out else ("Low Stock" if is_low else "In Stock")
        out.append(
            {
                "variant": v,
                "model": model,
                "brand": brand,
                "size": size,
                "thickness": thick,
                "badge": badge,
            }
        )
    return out


def list_furniture_variants(db: Session, *, furniture_item_id: int) -> list[FurnitureVariant]:
    stmt = select(FurnitureVariant).where(FurnitureVariant.furniture_item_id == furniture_item_id, FurnitureVariant.is_active.is_(True)).order_by(FurnitureVariant.bed_size_id.asc())
    return list(db.execute(stmt).scalars().all())


def upsert_furniture_variant(
    db: Session,
    *,
    furniture_item_id: int,
    bed_size_id: int | None,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    reorder_level: int,
) -> FurnitureVariant:
    stmt = select(FurnitureVariant).where(FurnitureVariant.furniture_item_id == furniture_item_id)
    if bed_size_id is None:
        stmt = stmt.where(FurnitureVariant.bed_size_id.is_(None))
    else:
        stmt = stmt.where(FurnitureVariant.bed_size_id == bed_size_id)
    v = db.execute(stmt).scalar_one_or_none()
    if v:
        v.qty_on_hand = qty_on_hand
        v.cost_price_pkr = cost_price_pkr
        v.sale_price_pkr = sale_price_pkr
        v.reorder_level = reorder_level
        v.is_active = True
        db.add(v)
        db.commit()
        db.refresh(v)
        _recompute_furniture_item_status(db, furniture_item_id=furniture_item_id)
        return v
    v = FurnitureVariant(
        furniture_item_id=furniture_item_id,
        bed_size_id=bed_size_id,
        qty_on_hand=qty_on_hand,
        cost_price_pkr=cost_price_pkr,
        sale_price_pkr=sale_price_pkr,
        reorder_level=reorder_level,
        is_active=True,
    )
    db.add(v)
    db.commit()
    db.refresh(v)
    _recompute_furniture_item_status(db, furniture_item_id=furniture_item_id)
    return v


def create_foam_model(db: Session, *, brand_id: int, name: str, notes: str | None) -> FoamModel:
    m = FoamModel(brand_id=brand_id, name=name, notes=notes, is_active=True)
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


def list_foam_variants(db: Session, *, foam_model_id: int) -> list[FoamVariant]:
    stmt = select(FoamVariant).where(FoamVariant.foam_model_id == foam_model_id, FoamVariant.is_active.is_(True)).order_by(FoamVariant.bed_size_id.asc(), FoamVariant.thickness_id.asc())
    return list(db.execute(stmt).scalars().all())


def upsert_foam_variant(
    db: Session,
    *,
    foam_model_id: int,
    bed_size_id: int,
    thickness_id: int,
    density_type: str | None,
    qty_on_hand: int,
    purchase_cost_pkr: int,
    sale_price_pkr: int,
    reorder_level: int,
) -> FoamVariant:
    stmt = select(FoamVariant).where(
        FoamVariant.foam_model_id == foam_model_id,
        FoamVariant.bed_size_id == bed_size_id,
        FoamVariant.thickness_id == thickness_id,
    )
    v = db.execute(stmt).scalar_one_or_none()
    if v:
        v.density_type = density_type
        v.qty_on_hand = qty_on_hand
        v.purchase_cost_pkr = purchase_cost_pkr
        v.sale_price_pkr = sale_price_pkr
        v.reorder_level = reorder_level
        v.is_active = True
        db.add(v)
        db.commit()
        db.refresh(v)
        return v
    v = FoamVariant(
        foam_model_id=foam_model_id,
        bed_size_id=bed_size_id,
        thickness_id=thickness_id,
        density_type=density_type,
        qty_on_hand=qty_on_hand,
        purchase_cost_pkr=purchase_cost_pkr,
        sale_price_pkr=sale_price_pkr,
        reorder_level=reorder_level,
        is_active=True,
    )
    db.add(v)
    db.commit()
    db.refresh(v)
    return v


def adjust_stock(
    db: Session,
    *,
    inventory_type: str,
    variant_id: int,
    movement_type: str,
    qty_change: int,
    unit_cost_pkr: int | None,
    notes: str | None,
) -> StockMovement:
    mv = StockMovement(
        inventory_type=inventory_type,
        variant_id=variant_id,
        movement_type=movement_type,
        qty_change=qty_change,
        unit_cost_pkr=unit_cost_pkr,
        notes=notes,
    )
    db.add(mv)

    if inventory_type == "FURNITURE_VARIANT":
        v = db.execute(select(FurnitureVariant).where(FurnitureVariant.id == variant_id)).scalar_one()
        v.qty_on_hand = int(v.qty_on_hand or 0) + int(qty_change)
        db.add(v)
    elif inventory_type == "FOAM_VARIANT":
        v = db.execute(select(FoamVariant).where(FoamVariant.id == variant_id)).scalar_one()
        v.qty_on_hand = int(v.qty_on_hand or 0) + int(qty_change)
        db.add(v)
    db.commit()
    db.refresh(mv)

    if inventory_type == "FURNITURE_VARIANT":
        try:
            v = db.execute(select(FurnitureVariant).where(FurnitureVariant.id == variant_id)).scalar_one_or_none()
            if v:
                _recompute_furniture_item_status(db, furniture_item_id=v.furniture_item_id)
        except Exception:
            pass
    return mv


def low_stock_furniture(db: Session, *, limit: int = 200) -> list[FurnitureVariant]:
    stmt = (
        select(FurnitureVariant)
        .where(FurnitureVariant.is_active.is_(True))
        .order_by(FurnitureVariant.qty_on_hand.asc(), FurnitureVariant.id.asc())
        .limit(limit)
    )
    rows = list(db.execute(stmt).scalars().all())
    out: list[FurnitureVariant] = []
    for v in rows:
        qty = int(v.qty_on_hand or 0)
        rl = int(v.reorder_level or 0)
        if rl > 0:
            if qty <= rl:
                out.append(v)
        else:
            if qty < 3:
                out.append(v)
    return out


def list_stock_movements(db: Session, *, limit: int = 200) -> list[StockMovement]:
    stmt = select(StockMovement).order_by(StockMovement.id.desc()).limit(limit)
    return list(db.execute(stmt).scalars().all())


def low_stock_foam(db: Session, *, limit: int = 200) -> list[FoamVariant]:
    stmt = (
        select(FoamVariant)
        .where(FoamVariant.is_active.is_(True))
        .order_by(FoamVariant.qty_on_hand.asc(), FoamVariant.id.asc())
        .limit(limit)
    )
    rows = list(db.execute(stmt).scalars().all())
    out: list[FoamVariant] = []
    for v in rows:
        qty = int(v.qty_on_hand or 0)
        rl = int(v.reorder_level or 0)
        if rl > 0:
            if qty <= rl:
                out.append(v)
        else:
            if qty < 3:
                out.append(v)
    return out
