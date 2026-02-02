from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, case, func, or_, select, update as sql_update
from sqlalchemy.orm import Session

from .models import (
    BedSize,
    Employee,
    FoamBrand,
    FoamModel,
    FoamThickness,
    FoamVariant,
    HardwareMaterial,
    FurnitureItem,
    FurnitureVariant,
    InventoryCategory,
    PoshishMaterial,
    SofaItem,
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


def ensure_inventory_seed(db: Session) -> None:
    furniture_root = _upsert_category(db, type="FURNITURE", parent_id=None, name="Furniture")
    foam_root = _upsert_category(db, type="FOAM", parent_id=None, name="Foam")

    bed_sets = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Bed Set")

    sofa_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Sofa")
    hardware_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Hardware")
    poshish_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Poshish Materials")
    kapra_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Kapra")
    polish_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Polish Materials")
    wood_cat = _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name="Wood")

    for name in [
        "Single Bed",
        "Double Bed",
        "Almari",
        "Showcase",
        "Side Table",
        "Dressing Table",
        sofa_cat.name,
        hardware_cat.name,
        poshish_cat.name,
        kapra_cat.name,
        polish_cat.name,
        wood_cat.name,
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=furniture_root.id, name=name)

    for name in ["Cushion Bed Set", "Tahli Bed Set", "Kicker + V-Board", "Other"]:
        _upsert_category(db, type="FURNITURE", parent_id=bed_sets.id, name=name)

    for name in [
        "Single Seater",
        "2 Seater",
        "3 Seater",
        "L-Shaped",
        "Corner Sofa",
        "Sofa Cum Bed",
        "Deewan",
        "Recliner",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=sofa_cat.id, name=name)

    for name in [
        "Hinges",
        "Handles",
        "Locks",
        "Nails",
        "Screws",
        "Brackets",
        "Drawer Slides",
        "Latches",
        "Glue",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=hardware_cat.id, name=name)

    for name in [
        "Foam Sheet",
        "Cushion",
        "Cotton",
        "Fiber",
        "Webbing / Belt",
        "Elastic",
        "Buttons",
        "Zips",
        "Thread",
        "Staples",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=poshish_cat.id, name=name)

    for name in [
        "Thinner",
        "Lacquer",
        "Sealer",
        "Hardener",
        "Sandpaper",
        "Stain",
        "Paint",
        "Primer",
        "Polish",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=polish_cat.id, name=name)

    for name in [
        "Velvet",
        "Leatherette",
        "Cotton",
        "Jute",
        "Linen",
        "Jacquard",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=kapra_cat.id, name=name)

    for name in [
        "Tahli",
        "Deodar",
        "Kail",
        "MDF",
        "Plywood",
        "Particle Board",
        "Veneer",
        "Lamination / Sunmica",
        "Other",
    ]:
        _upsert_category(db, type="FURNITURE", parent_id=wood_cat.id, name=name)

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


def upsert_inventory_category(db: Session, *, type: str, parent_id: int | None, name: str) -> InventoryCategory:
    return _upsert_category(db, type=type, parent_id=parent_id, name=name)


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


def get_inventory_category_by_id(db: Session, *, category_id: int) -> InventoryCategory | None:
    stmt = select(InventoryCategory).where(
        InventoryCategory.is_active.is_(True),
        InventoryCategory.id == category_id,
    )
    return db.execute(stmt).scalars().first()


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


def create_sofa_item(
    db: Session,
    *,
    name: str,
    sofa_type: str,
    hardware_material: str | None,
    poshish_material: str | None,
    seating_capacity: str | None,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> SofaItem:
    it = SofaItem(
        name=name,
        sofa_type=sofa_type,
        hardware_material=hardware_material or None,
        poshish_material=poshish_material or None,
        seating_capacity=seating_capacity or None,
        qty_on_hand=int(qty_on_hand or 0),
        reorder_level=0,
        cost_price_pkr=int(cost_price_pkr or 0),
        sale_price_pkr=int(sale_price_pkr or 0),
        notes=notes,
        is_active=True,
    )
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def update_sofa_item(
    db: Session,
    *,
    item_id: int,
    name: str,
    sofa_type: str,
    hardware_material: str | None,
    poshish_material: str | None,
    seating_capacity: str | None,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> SofaItem | None:
    it = db.execute(select(SofaItem).where(SofaItem.id == item_id)).scalar_one_or_none()
    if not it:
        return None
    it.name = name
    it.sofa_type = sofa_type
    it.hardware_material = hardware_material or None
    it.poshish_material = poshish_material or None
    it.seating_capacity = seating_capacity or None
    it.qty_on_hand = int(qty_on_hand or 0)
    it.cost_price_pkr = int(cost_price_pkr or 0)
    it.sale_price_pkr = int(sale_price_pkr or 0)
    it.notes = notes
    it.is_active = True
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def list_sofa_items(db: Session, *, q: str | None = None, sofa_type: str | None = None, limit: int = 500) -> list[SofaItem]:
    stmt = select(SofaItem).where(SofaItem.is_active.is_(True)).order_by(SofaItem.id.desc())
    if sofa_type:
        stmt = stmt.where(SofaItem.sofa_type == sofa_type)
    if q:
        stmt = stmt.where(func.lower(SofaItem.name).like(f"%{q.lower()}%"))
    stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def soft_delete_sofa_item(db: Session, *, item_id: int) -> None:
    it = db.execute(select(SofaItem).where(SofaItem.id == item_id)).scalar_one_or_none()
    if not it:
        return
    it.is_active = False
    db.add(it)
    db.commit()


def sofa_cards(db: Session, *, items: list[SofaItem]) -> list[dict]:
    out: list[dict] = []
    for it in items:
        qty = int(it.qty_on_hand or 0)
        rl = int(it.reorder_level or 0)
        is_out = qty <= 0
        is_low = (qty <= rl) if rl > 0 else (qty < 3)
        badge = "Out of Stock" if is_out else ("Low Stock" if is_low else "In Stock")
        out.append({"item": it, "badge": badge})
    return out


def create_hardware_material(
    db: Session,
    *,
    name: str,
    unit: str,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> HardwareMaterial:
    it = HardwareMaterial(
        name=name,
        unit=unit or "pieces",
        qty_on_hand=int(qty_on_hand or 0),
        reorder_level=0,
        cost_price_pkr=int(cost_price_pkr or 0),
        sale_price_pkr=int(sale_price_pkr or 0),
        notes=notes,
        is_active=True,
    )
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def update_hardware_material(
    db: Session,
    *,
    item_id: int,
    name: str,
    unit: str,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> HardwareMaterial | None:
    it = db.execute(select(HardwareMaterial).where(HardwareMaterial.id == item_id)).scalar_one_or_none()
    if not it:
        return None
    it.name = name
    it.unit = unit or "pieces"
    it.qty_on_hand = int(qty_on_hand or 0)
    it.cost_price_pkr = int(cost_price_pkr or 0)
    it.sale_price_pkr = int(sale_price_pkr or 0)
    it.notes = notes
    it.is_active = True
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def list_hardware_materials(db: Session, *, q: str | None = None, limit: int = 500) -> list[HardwareMaterial]:
    stmt = select(HardwareMaterial).where(HardwareMaterial.is_active.is_(True)).order_by(HardwareMaterial.id.desc())
    if q:
        stmt = stmt.where(func.lower(HardwareMaterial.name).like(f"%{q.lower()}%"))
    stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def soft_delete_hardware_material(db: Session, *, item_id: int) -> None:
    it = db.execute(select(HardwareMaterial).where(HardwareMaterial.id == item_id)).scalar_one_or_none()
    if not it:
        return
    it.is_active = False
    db.add(it)
    db.commit()


def hardware_cards(db: Session, *, items: list[HardwareMaterial]) -> list[dict]:
    out: list[dict] = []
    for it in items:
        qty = int(it.qty_on_hand or 0)
        rl = int(it.reorder_level or 0)
        is_out = qty <= 0
        is_low = (qty <= rl) if rl > 0 else (qty < 3)
        badge = "Out of Stock" if is_out else ("Low Stock" if is_low else "In Stock")
        out.append({"item": it, "badge": badge})
    return out


def create_poshish_material(
    db: Session,
    *,
    name: str,
    color: str | None,
    unit: str,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> PoshishMaterial:
    it = PoshishMaterial(
        name=name,
        color=color or None,
        unit=unit or "meters",
        qty_on_hand=int(qty_on_hand or 0),
        reorder_level=0,
        cost_price_pkr=int(cost_price_pkr or 0),
        sale_price_pkr=int(sale_price_pkr or 0),
        notes=notes,
        is_active=True,
    )
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def update_poshish_material(
    db: Session,
    *,
    item_id: int,
    name: str,
    color: str | None,
    unit: str,
    qty_on_hand: int,
    cost_price_pkr: int,
    sale_price_pkr: int,
    notes: str | None,
) -> PoshishMaterial | None:
    it = db.execute(select(PoshishMaterial).where(PoshishMaterial.id == item_id)).scalar_one_or_none()
    if not it:
        return None
    it.name = name
    it.color = color or None
    it.unit = unit or "meters"
    it.qty_on_hand = int(qty_on_hand or 0)
    it.cost_price_pkr = int(cost_price_pkr or 0)
    it.sale_price_pkr = int(sale_price_pkr or 0)
    it.notes = notes
    it.is_active = True
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def list_poshish_materials(db: Session, *, q: str | None = None, limit: int = 500) -> list[PoshishMaterial]:
    stmt = select(PoshishMaterial).where(PoshishMaterial.is_active.is_(True)).order_by(PoshishMaterial.id.desc())
    if q:
        stmt = stmt.where(func.lower(PoshishMaterial.name).like(f"%{q.lower()}%"))
    stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def soft_delete_poshish_material(db: Session, *, item_id: int) -> None:
    it = db.execute(select(PoshishMaterial).where(PoshishMaterial.id == item_id)).scalar_one_or_none()
    if not it:
        return
    it.is_active = False
    db.add(it)
    db.commit()


def poshish_cards(db: Session, *, items: list[PoshishMaterial]) -> list[dict]:
    out: list[dict] = []
    for it in items:
        qty = int(it.qty_on_hand or 0)
        rl = int(it.reorder_level or 0)
        is_out = qty <= 0
        is_low = (qty <= rl) if rl > 0 else (qty < 3)
        badge = "Out of Stock" if is_out else ("Low Stock" if is_low else "In Stock")
        out.append({"item": it, "badge": badge})
    return out


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

    bed_sizes = list_bed_sizes(db)
    bed_size_by_id = {s.id: s for s in bed_sizes}

    out: list[dict] = []
    for it in items:
        vs = by_item.get(it.id, [])
        primary_variant_id: int | None = None
        primary_bed_size_id: int | None = None
        primary_qty_on_hand: int = 0
        primary_cost_price_pkr: int = 0
        primary_sale_price_pkr: int = 0
        if vs:
            try:
                primary = sorted(vs, key=lambda v: (v.bed_size_id is None, v.bed_size_id or 0, v.id))[0]
                primary_variant_id = primary.id
                primary_bed_size_id = primary.bed_size_id
                primary_qty_on_hand = int(primary.qty_on_hand or 0)
                primary_cost_price_pkr = int(primary.cost_price_pkr or 0)
                primary_sale_price_pkr = int(primary.sale_price_pkr or 0)
            except Exception:
                primary = vs[0]
                primary_variant_id = primary.id
                primary_bed_size_id = primary.bed_size_id
                primary_qty_on_hand = int(primary.qty_on_hand or 0)
                primary_cost_price_pkr = int(primary.cost_price_pkr or 0)
                primary_sale_price_pkr = int(primary.sale_price_pkr or 0)
        total_qty = sum(int(v.qty_on_hand or 0) for v in vs)
        min_cost = min((int(v.cost_price_pkr or 0) for v in vs), default=0)
        min_sale = min((int(v.sale_price_pkr or 0) for v in vs), default=0)

        size_label = "Custom Size"
        size_ids = sorted({v.bed_size_id for v in vs if v.bed_size_id is not None})
        has_custom = any(v.bed_size_id is None for v in vs)
        if len(size_ids) == 1 and not has_custom:
            s = bed_size_by_id.get(size_ids[0])
            size_label = s.label if s else "Custom Size"
        elif len(size_ids) > 1 and not has_custom:
            size_label = "Multiple Sizes"

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
                "size_label": size_label,
                "min_cost": min_cost,
                "min_sale": min_sale,
                "badge": badge,
                "primary_variant_id": primary_variant_id,
                "primary_bed_size_id": primary_bed_size_id,
                "primary_qty_on_hand": primary_qty_on_hand,
                "primary_cost_price_pkr": primary_cost_price_pkr,
                "primary_sale_price_pkr": primary_sale_price_pkr,
            }
        )
    return out


def update_furniture_item(
    db: Session,
    *,
    item_id: int,
    name: str,
    material_type: str,
    status: str,
    category_id: int,
    sub_category_id: int | None,
    notes: str | None,
) -> FurnitureItem | None:
    item = db.execute(select(FurnitureItem).where(FurnitureItem.id == item_id)).scalar_one_or_none()
    if not item:
        return None
    item.name = name
    item.material_type = material_type
    item.status = status
    item.category_id = category_id
    item.sub_category_id = sub_category_id
    item.notes = notes
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def soft_delete_furniture_item(db: Session, *, item_id: int) -> None:
    item = db.execute(select(FurnitureItem).where(FurnitureItem.id == item_id)).scalar_one_or_none()
    if not item:
        return
    item.is_active = False
    db.add(item)
    db.execute(
        sql_update(FurnitureVariant)
        .where(FurnitureVariant.furniture_item_id == item_id)
        .values(is_active=False)
    )
    db.commit()


def soft_delete_foam_model(db: Session, *, model_id: int) -> None:
    m = db.execute(select(FoamModel).where(FoamModel.id == model_id)).scalar_one_or_none()
    if not m:
        return
    m.is_active = False
    db.add(m)
    db.execute(
        sql_update(FoamVariant)
        .where(FoamVariant.foam_model_id == model_id)
        .values(is_active=False)
    )
    db.commit()


def inventory_dashboard_stats(db: Session) -> dict:
    furniture_items = list_furniture_items_filtered(db, q=None, category_id=None, limit=5000)
    furniture_cards_data = furniture_cards(db, items=furniture_items)

    foam_cards_data = foam_variant_cards(db, q=None, brand_id=None, limit=5000)

    sofa_items = list_sofa_items(db, q=None, sofa_type=None, limit=5000)
    sofa_cards_data = sofa_cards(db, items=sofa_items)

    hardware_items = list_hardware_materials(db, q=None, limit=5000)
    hardware_cards_data = hardware_cards(db, items=hardware_items)

    poshish_items = list_poshish_materials(db, q=None, limit=5000)
    poshish_cards_data = poshish_cards(db, items=poshish_items)

    total_furniture = len(furniture_items)
    total_foam = len(foam_cards_data)
    total_sofas = len(sofa_items)
    total_hardware = len(hardware_items)
    total_poshish = len(poshish_items)

    low_stock = 0
    out_of_stock = 0

    low_stock_furniture = 0
    out_of_stock_furniture = 0
    low_stock_foam = 0
    out_of_stock_foam = 0
    low_stock_sofas = 0
    out_of_stock_sofas = 0
    low_stock_hardware = 0
    out_of_stock_hardware = 0
    low_stock_poshish = 0
    out_of_stock_poshish = 0

    furniture_value = 0
    for c in furniture_cards_data:
        badge = c.get("badge")
        if badge == "Low Stock":
            low_stock += 1
            low_stock_furniture += 1
        elif badge == "Out of Stock":
            out_of_stock += 1
            out_of_stock_furniture += 1
        if badge != "Made to Order":
            furniture_value += int(c.get("total_qty") or 0) * int(c.get("min_sale") or 0)

    foam_value = 0
    for c in foam_cards_data:
        badge = c.get("badge")
        if badge == "Low Stock":
            low_stock += 1
            low_stock_foam += 1
        elif badge == "Out of Stock":
            out_of_stock += 1
            out_of_stock_foam += 1
        v = c.get("variant")
        if v:
            foam_value += int(getattr(v, "qty_on_hand", 0) or 0) * int(getattr(v, "sale_price_pkr", 0) or 0)

    sofa_value = 0
    for c in sofa_cards_data:
        badge = c.get("badge")
        if badge == "Low Stock":
            low_stock += 1
            low_stock_sofas += 1
        elif badge == "Out of Stock":
            out_of_stock += 1
            out_of_stock_sofas += 1
        it = c.get("item")
        if it:
            sofa_value += int(getattr(it, "qty_on_hand", 0) or 0) * int(getattr(it, "sale_price_pkr", 0) or 0)

    hardware_value = 0
    for c in hardware_cards_data:
        badge = c.get("badge")
        if badge == "Low Stock":
            low_stock += 1
            low_stock_hardware += 1
        elif badge == "Out of Stock":
            out_of_stock += 1
            out_of_stock_hardware += 1
        it = c.get("item")
        if it:
            hardware_value += int(getattr(it, "qty_on_hand", 0) or 0) * int(getattr(it, "sale_price_pkr", 0) or 0)

    poshish_value = 0
    for c in poshish_cards_data:
        badge = c.get("badge")
        if badge == "Low Stock":
            low_stock += 1
            low_stock_poshish += 1
        elif badge == "Out of Stock":
            out_of_stock += 1
            out_of_stock_poshish += 1
        it = c.get("item")
        if it:
            poshish_value += int(getattr(it, "qty_on_hand", 0) or 0) * int(getattr(it, "sale_price_pkr", 0) or 0)

    total_items = total_furniture + total_foam + total_sofas + total_hardware + total_poshish
    in_stock_items = max(total_items - low_stock - out_of_stock, 0)
    stock_health_pct = int(round((in_stock_items / total_items) * 100)) if total_items > 0 else 0

    return {
        "total_furniture": total_furniture,
        "total_foam": total_foam,
        "total_sofas": total_sofas,
        "total_hardware": total_hardware,
        "total_poshish": total_poshish,
        "low_stock_furniture": low_stock_furniture,
        "out_of_stock_furniture": out_of_stock_furniture,
        "low_stock_foam": low_stock_foam,
        "out_of_stock_foam": out_of_stock_foam,
        "low_stock_sofas": low_stock_sofas,
        "out_of_stock_sofas": out_of_stock_sofas,
        "low_stock_hardware": low_stock_hardware,
        "out_of_stock_hardware": out_of_stock_hardware,
        "low_stock_poshish": low_stock_poshish,
        "out_of_stock_poshish": out_of_stock_poshish,
        "low_stock": low_stock,
        "out_of_stock": out_of_stock,
        "total_inventory_value": furniture_value + foam_value + sofa_value + hardware_value + poshish_value,
        "total_items": total_items,
        "stock_health_pct": stock_health_pct,
    }


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
    elif inventory_type == "SOFA_ITEM":
        it = db.execute(select(SofaItem).where(SofaItem.id == variant_id)).scalar_one()
        it.qty_on_hand = int(it.qty_on_hand or 0) + int(qty_change)
        db.add(it)
    elif inventory_type == "HARDWARE_MATERIAL":
        it = db.execute(select(HardwareMaterial).where(HardwareMaterial.id == variant_id)).scalar_one()
        it.qty_on_hand = int(it.qty_on_hand or 0) + int(qty_change)
        db.add(it)
    elif inventory_type == "POSHISH_MATERIAL":
        it = db.execute(select(PoshishMaterial).where(PoshishMaterial.id == variant_id)).scalar_one()
        it.qty_on_hand = int(it.qty_on_hand or 0) + int(qty_change)
        db.add(it)
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


def stock_movement_cards(db: Session, *, limit: int = 500) -> list[dict]:
    moves = list_stock_movements(db, limit=limit)
    if not moves:
        return []

    furniture_variant_ids: set[int] = set()
    foam_variant_ids: set[int] = set()
    sofa_ids: set[int] = set()
    hardware_ids: set[int] = set()
    poshish_ids: set[int] = set()

    for m in moves:
        t = (m.inventory_type or "").upper()
        if t == "FURNITURE_VARIANT":
            furniture_variant_ids.add(int(m.variant_id))
        elif t == "FOAM_VARIANT":
            foam_variant_ids.add(int(m.variant_id))
        elif t == "SOFA_ITEM":
            sofa_ids.add(int(m.variant_id))
        elif t == "HARDWARE_MATERIAL":
            hardware_ids.add(int(m.variant_id))
        elif t == "POSHISH_MATERIAL":
            poshish_ids.add(int(m.variant_id))

    furniture_name_by_variant_id: dict[int, str] = {}
    if furniture_variant_ids:
        fvs = list(
            db.execute(select(FurnitureVariant).where(FurnitureVariant.id.in_(sorted(furniture_variant_ids)))).scalars().all()
        )
        f_item_ids = {v.furniture_item_id for v in fvs}
        items = []
        if f_item_ids:
            items = list(db.execute(select(FurnitureItem).where(FurnitureItem.id.in_(sorted(f_item_ids)))).scalars().all())
        item_name_by_id = {i.id: i.name for i in items}
        for v in fvs:
            furniture_name_by_variant_id[v.id] = item_name_by_id.get(v.furniture_item_id, f"Furniture #{v.furniture_item_id}")

    foam_name_by_variant_id: dict[int, str] = {}
    if foam_variant_ids:
        fvs = list(db.execute(select(FoamVariant).where(FoamVariant.id.in_(sorted(foam_variant_ids)))).scalars().all())
        model_ids = {v.foam_model_id for v in fvs}
        models = []
        if model_ids:
            models = list(db.execute(select(FoamModel).where(FoamModel.id.in_(sorted(model_ids)))).scalars().all())
        model_name_by_id = {m.id: m.name for m in models}
        for v in fvs:
            foam_name_by_variant_id[v.id] = model_name_by_id.get(v.foam_model_id, f"Foam #{v.foam_model_id}")

    sofa_name_by_id: dict[int, str] = {}
    if sofa_ids:
        sofas = list(db.execute(select(SofaItem).where(SofaItem.id.in_(sorted(sofa_ids)))).scalars().all())
        sofa_name_by_id = {s.id: s.name for s in sofas}

    hardware_name_by_id: dict[int, str] = {}
    if hardware_ids:
        mats = list(db.execute(select(HardwareMaterial).where(HardwareMaterial.id.in_(sorted(hardware_ids)))).scalars().all())
        hardware_name_by_id = {m.id: m.name for m in mats}

    poshish_name_by_id: dict[int, str] = {}
    if poshish_ids:
        mats = list(db.execute(select(PoshishMaterial).where(PoshishMaterial.id.in_(sorted(poshish_ids)))).scalars().all())
        poshish_name_by_id = {m.id: m.name for m in mats}

    out: list[dict] = []
    for m in moves:
        t = (m.inventory_type or "").upper()
        name = ""
        label = t
        if t == "FURNITURE_VARIANT":
            label = "Furniture"
            name = furniture_name_by_variant_id.get(int(m.variant_id), f"Furniture Variant #{m.variant_id}")
        elif t == "FOAM_VARIANT":
            label = "Foam"
            name = foam_name_by_variant_id.get(int(m.variant_id), f"Foam Variant #{m.variant_id}")
        elif t == "SOFA_ITEM":
            label = "Sofa"
            name = sofa_name_by_id.get(int(m.variant_id), f"Sofa #{m.variant_id}")
        elif t == "HARDWARE_MATERIAL":
            label = "Hardware"
            name = hardware_name_by_id.get(int(m.variant_id), f"Hardware #{m.variant_id}")
        elif t == "POSHISH_MATERIAL":
            label = "Poshish"
            name = poshish_name_by_id.get(int(m.variant_id), f"Poshish #{m.variant_id}")

        out.append(
            {
                "movement": m,
                "label": label,
                "item_name": name,
            }
        )
    return out


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
