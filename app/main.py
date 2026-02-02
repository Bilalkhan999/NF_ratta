from __future__ import annotations

import csv
import datetime as dt
import io
import os
import base64
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Flowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.charts.legends import Legend
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from . import crud
from .db import Base, IS_SQLITE, SessionLocal, engine
from .models import BedSize, Employee, FoamBrand, FoamModel, FoamThickness, InventoryCategory, Transaction, WeeklyAssignment
from .utils import (
    EMPLOYEE_CATEGORIES,
    EMPLOYEE_WORK_TYPES,
    EMPLOYEE_TX_TYPES,
    INCOMING_CATEGORIES,
    OUTGOING_CATEGORIES,
    PAYMENT_METHODS,
    clamp_date_range,
    parse_date,
    pkr_format,
    sat_thu_week_range,
)


MAX_IMAGE_UPLOAD_BYTES = int(os.getenv("MAX_IMAGE_UPLOAD_BYTES", "800000"))

_CLOUDINARY_URL = os.getenv("CLOUDINARY_URL")
if _CLOUDINARY_URL:
    import cloudinary
    import cloudinary.uploader

    cloudinary.config(cloudinary_url=_CLOUDINARY_URL)


def _decode_data_url(data_url: str) -> tuple[str, bytes] | None:
    if not data_url:
        return None
    if not data_url.startswith("data:"):
        return None
    try:
        header, b64 = data_url.split(",", 1)
        content_type = header.split(":", 1)[1].split(";", 1)[0] or "application/octet-stream"
        raw = base64.b64decode(b64)
        return content_type, raw
    except Exception:
        return None


def _upload_image_to_cloudinary(*, raw: bytes, folder: str, public_id: str) -> str:
    if not _CLOUDINARY_URL:
        raise HTTPException(status_code=500, detail="Cloudinary is not configured")
    try:
        res = cloudinary.uploader.upload(
            raw,
            folder=folder,
            public_id=public_id,
            overwrite=True,
            resource_type="image",
        )
        url = (res or {}).get("secure_url") or (res or {}).get("url")
        if not url:
            raise Exception("Missing URL")
        return str(url)
    except Exception:
        raise HTTPException(status_code=500, detail="Image upload failed")


def _employee_outgoing_category(emp: Employee) -> str:
    c = (emp.category or "").lower()
    if "karkhan" in c or "factory" in c:
        return "Karkhanay Wala"
    if "polish" in c:
        return "Polish Wala"
    if "poshish" in c or "upholstery" in c:
        return "Poshish Wala"
    return "Employee"

app = FastAPI(title="Nusrat Furniture Payments")

SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/static"):
            return await call_next(request)
        if path in {"/login", "/logout"}:
            return await call_next(request)
        if not _is_logged_in(request):
            return RedirectResponse(url="/login", status_code=303)
        return await call_next(request)


# Order matters: SessionMiddleware must run BEFORE auth so request.session works.
app.add_middleware(AuthMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=os.getenv("VERCEL") is not None,
)

BASE_DIR = __import__("pathlib").Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def on_startup() -> None:
    auto_create = os.getenv("AUTO_CREATE_DB")
    if IS_SQLITE or (auto_create is not None and auto_create.strip() == "1"):
        try:
            Base.metadata.create_all(bind=engine)
        except Exception:
            return

        if not IS_SQLITE:
            try:
                insp = inspect(engine)
                cols = {c["name"] for c in insp.get_columns("transactions")}

                alter_stmts: list[str] = []
                if "employee_id" not in cols:
                    alter_stmts.append("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS employee_id INTEGER")
                if "employee_tx_type" not in cols:
                    alter_stmts.append("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS employee_tx_type VARCHAR(32)")
                if "payment_method" not in cols:
                    alter_stmts.append("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payment_method VARCHAR(32)")
                if "assignment_id" not in cols:
                    alter_stmts.append("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS assignment_id INTEGER")
                if "reference" not in cols:
                    alter_stmts.append("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS reference VARCHAR(256)")

                if alter_stmts:
                    with engine.begin() as conn:
                        for stmt in alter_stmts:
                            conn.execute(text(stmt))
            except Exception:
                pass

            try:
                emp_cols = {c["name"] for c in insp.get_columns("employees")}
                emp_alter: list[str] = []
                if "profile_image_url" not in emp_cols:
                    emp_alter.append("ALTER TABLE employees ADD COLUMN IF NOT EXISTS profile_image_url VARCHAR(512)")
                if "cnic_image_url" not in emp_cols:
                    emp_alter.append("ALTER TABLE employees ADD COLUMN IF NOT EXISTS cnic_image_url VARCHAR(512)")
                if "profile_image_data" not in emp_cols:
                    emp_alter.append("ALTER TABLE employees ADD COLUMN IF NOT EXISTS profile_image_data TEXT")
                if "cnic_image_data" not in emp_cols:
                    emp_alter.append("ALTER TABLE employees ADD COLUMN IF NOT EXISTS cnic_image_data TEXT")
                if emp_alter:
                    with engine.begin() as conn:
                        for stmt in emp_alter:
                            conn.execute(text(stmt))
            except Exception:
                pass


def _is_logged_in(request: Request) -> bool:
    try:
        return bool(request.session.get("user"))
    except Exception:
        return False


def common_context(request: Request):
    return {
        "request": request,
        "is_logged_in": _is_logged_in(request),
        "incoming_categories": INCOMING_CATEGORIES,
        "outgoing_categories": OUTGOING_CATEGORIES,
        "employee_categories": EMPLOYEE_CATEGORIES,
        "employee_work_types": EMPLOYEE_WORK_TYPES,
        "employee_tx_types": EMPLOYEE_TX_TYPES,
        "payment_methods": PAYMENT_METHODS,
        "all_categories": sorted(set(INCOMING_CATEGORIES + OUTGOING_CATEGORIES)),
        "pkr_format": pkr_format,
        "today": dt.date.today().isoformat(),
    }


@app.get("/employees", response_class=HTMLResponse)
def employees(request: Request, db: Session = Depends(get_db), status: str | None = None):
    _backfill_employees_from_transactions(db)
    items = crud.list_employees(db, status=status)
    ctx = common_context(request)
    ctx.update({"items": items, "status": status or ""})
    return TEMPLATES.TemplateResponse("employees.html", ctx)


@app.get("/admin/transactions-unique.csv")
def transactions_unique_csv(db: Session = Depends(get_db)):
    stmt = (
        select(
            func.trim(Transaction.name).label("name"),
            Transaction.category.label("category"),
            func.count(Transaction.id).label("tx_count"),
        )
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.name.is_not(None))
        .where(func.trim(Transaction.name) != "")
        .group_by(func.lower(func.trim(Transaction.name)), Transaction.category)
        .order_by(func.count(Transaction.id).desc(), func.lower(func.trim(Transaction.name)).asc(), Transaction.category.asc())
    )
    rows = db.execute(stmt).all()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["name", "category", "tx_count"])
    for r in rows:
        w.writerow([r.name, r.category, int(r.tx_count or 0)])

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=transactions_unique.csv"},
    )


@app.get("/employees/new", response_class=HTMLResponse)
def employee_new(request: Request):
    ctx = common_context(request)
    ctx.update({"mode": "create", "emp": None, "errors": {}})
    return TEMPLATES.TemplateResponse("employee_form.html", ctx)


@app.post("/employees/new", response_class=HTMLResponse)
def employee_new_post(
    request: Request,
    db: Session = Depends(get_db),
    full_name: str = Form(...),
    father_name: str | None = Form(None),
    cnic: str | None = Form(None),
    mobile_number: str | None = Form(None),
    address: str | None = Form(None),
    emergency_contact: str | None = Form(None),
    joining_date: str = Form(...),
    status: str = Form("active"),
    category: str = Form(...),
    work_type: str = Form(...),
    role_description: str | None = Form(None),
    payment_rate: int | None = Form(None),
    profile_image_url: str | None = Form(None),
    profile_image: UploadFile | None = File(None),
    cnic_image: UploadFile | None = File(None),
):
    jd = parse_date(joining_date) or dt.date.today()
    errors: dict[str, str] = {}
    if not full_name.strip():
        errors["full_name"] = "Full name is required."
    if category not in EMPLOYEE_CATEGORIES:
        errors["category"] = "Invalid category."
    if work_type not in EMPLOYEE_WORK_TYPES:
        errors["work_type"] = "Invalid work type."
    if status not in {"active", "inactive"}:
        errors["status"] = "Invalid status."
    if not profile_image and not (profile_image_url or "").strip():
        errors["profile_image_url"] = "Profile image is required."
    if errors:
        ctx = common_context(request)
        ctx.update(
            {
                "mode": "create",
                "emp": {
                    "full_name": full_name,
                    "father_name": father_name,
                    "cnic": cnic,
                    "mobile_number": mobile_number,
                    "address": address,
                    "emergency_contact": emergency_contact,
                    "joining_date": jd,
                    "status": status,
                    "category": category,
                    "work_type": work_type,
                    "role_description": role_description,
                    "payment_rate": payment_rate,
                    "profile_image_url": profile_image_url,
                },
                "errors": errors,
            }
        )
        return TEMPLATES.TemplateResponse("employee_form.html", ctx, status_code=400)

    profile_url = (profile_image_url or "").strip() or None
    if profile_image is not None:
        raw = profile_image.file.read()
        if raw and len(raw) > MAX_IMAGE_UPLOAD_BYTES:
            errors["profile_image_url"] = f"Profile image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw:
            profile_url = _upload_image_to_cloudinary(raw=raw, folder="nf_employees", public_id=f"profile_{int(dt.datetime.utcnow().timestamp())}")

    cnic_url = None
    if cnic_image is not None:
        raw2 = cnic_image.file.read()
        if raw2 and len(raw2) > MAX_IMAGE_UPLOAD_BYTES:
            errors["cnic_image"] = f"CNIC image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw2:
            cnic_url = _upload_image_to_cloudinary(raw=raw2, folder="nf_employees", public_id=f"cnic_{int(dt.datetime.utcnow().timestamp())}")

    if errors:
        ctx = common_context(request)
        ctx.update({"mode": "new", "emp": None, "errors": errors})
        return TEMPLATES.TemplateResponse("employee_form.html", ctx, status_code=400)

    emp = crud.create_employee(
        db,
        full_name=full_name.strip(),
        father_name=father_name,
        cnic=cnic,
        mobile_number=mobile_number,
        address=address,
        emergency_contact=emergency_contact,
        joining_date=jd,
        status=status,
        category=category,
        work_type=work_type,
        role_description=role_description,
        payment_rate=payment_rate,
        profile_image_url=profile_url,
    )
    if cnic_url:
        emp.cnic_image_url = cnic_url
        db.add(emp)
        db.commit()
    return RedirectResponse(url=f"/employees/{emp.id}", status_code=303)


@app.get("/employees/{employee_id}", response_class=HTMLResponse)
def employee_profile(request: Request, employee_id: int, db: Session = Depends(get_db)):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")

    txs = crud.employee_transactions(db, employee_id=employee_id, limit=1000)
    summary = crud.employee_financial_summary(db, employee_id=employee_id)
    assignments = crud.list_assignments_for_employee(db, employee_id=employee_id)

    ledger = []
    running = 0
    for txx in txs:
        debit = 0
        credit = 0
        if (txx.employee_tx_type or "") == "advance":
            debit = int(txx.amount_pkr)
            running += debit
        elif (txx.employee_tx_type or "") in {"salary", "per_work"} or txx.employee_tx_type is None:
            credit = int(txx.amount_pkr)
            running -= credit
        ledger.append({"tx": txx, "debit": debit, "credit": credit, "balance": running})

    ctx = common_context(request)
    ctx.update({"emp": emp, "summary": summary, "ledger": ledger, "assignments": assignments})
    return TEMPLATES.TemplateResponse("employee_profile.html", ctx)


@app.get("/employees/{employee_id}/profile-image")
def employee_profile_image(employee_id: int, db: Session = Depends(get_db)):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")

    decoded = _decode_data_url(emp.profile_image_data or "")
    if not decoded:
        raise HTTPException(status_code=404, detail="No image")
    content_type, raw = decoded
    return Response(content=raw, media_type=content_type)


@app.get("/employees/{employee_id}/cnic-image")
def employee_cnic_image(employee_id: int, db: Session = Depends(get_db)):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")

    decoded = _decode_data_url(emp.cnic_image_data or "")
    if not decoded:
        raise HTTPException(status_code=404, detail="No image")
    content_type, raw = decoded
    return Response(content=raw, media_type=content_type)


@app.get("/employees/{employee_id}/edit", response_class=HTMLResponse)
def employee_edit(request: Request, employee_id: int, db: Session = Depends(get_db)):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")
    ctx = common_context(request)
    ctx.update({"mode": "edit", "emp": emp, "errors": {}})
    return TEMPLATES.TemplateResponse("employee_form.html", ctx)


@app.post("/employees/{employee_id}/edit", response_class=HTMLResponse)
def employee_edit_post(
    request: Request,
    employee_id: int,
    db: Session = Depends(get_db),
    full_name: str = Form(...),
    father_name: str | None = Form(None),
    cnic: str | None = Form(None),
    mobile_number: str | None = Form(None),
    address: str | None = Form(None),
    emergency_contact: str | None = Form(None),
    joining_date: str = Form(...),
    status: str = Form("active"),
    category: str = Form(...),
    work_type: str = Form(...),
    role_description: str | None = Form(None),
    payment_rate: int | None = Form(None),
    profile_image_url: str | None = Form(None),
    profile_image: UploadFile | None = File(None),
    cnic_image: UploadFile | None = File(None),
):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")
    jd = parse_date(joining_date) or dt.date.today()
    errors: dict[str, str] = {}
    if not full_name.strip():
        errors["full_name"] = "Full name is required."
    if category not in EMPLOYEE_CATEGORIES:
        errors["category"] = "Invalid category."
    if work_type not in EMPLOYEE_WORK_TYPES:
        errors["work_type"] = "Invalid work type."
    if status not in {"active", "inactive"}:
        errors["status"] = "Invalid status."
    if not profile_image and not (profile_image_url or "").strip() and not (emp.profile_image_data or emp.profile_image_url):
        errors["profile_image_url"] = "Profile image is required."
    if errors:
        ctx = common_context(request)
        ctx.update({"mode": "edit", "emp": emp, "errors": errors})
        return TEMPLATES.TemplateResponse("employee_form.html", ctx, status_code=400)

    crud.update_employee(
        db,
        emp,
        full_name=full_name.strip(),
        father_name=father_name,
        cnic=cnic,
        mobile_number=mobile_number,
        address=address,
        emergency_contact=emergency_contact,
        joining_date=jd,
        status=status,
        category=category,
        work_type=work_type,
        role_description=role_description,
        payment_rate=payment_rate,
        profile_image_url=(profile_image_url or "").strip(),
    )

    if profile_image is not None:
        raw = profile_image.file.read()
        if raw and len(raw) > MAX_IMAGE_UPLOAD_BYTES:
            errors["profile_image_url"] = f"Profile image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw:
            emp.profile_image_url = _upload_image_to_cloudinary(raw=raw, folder="nf_employees", public_id=f"profile_{emp.id}")
            emp.profile_image_data = None
    if cnic_image is not None:
        raw2 = cnic_image.file.read()
        if raw2 and len(raw2) > MAX_IMAGE_UPLOAD_BYTES:
            errors["cnic_image"] = f"CNIC image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw2:
            emp.cnic_image_url = _upload_image_to_cloudinary(raw=raw2, folder="nf_employees", public_id=f"cnic_{emp.id}")
            emp.cnic_image_data = None

    if errors:
        ctx = common_context(request)
        ctx.update({"mode": "edit", "emp": emp, "errors": errors})
        return TEMPLATES.TemplateResponse("employee_form.html", ctx, status_code=400)
    db.add(emp)
    db.commit()
    return RedirectResponse(url=f"/employees/{employee_id}", status_code=303)


@app.post("/employees/{employee_id}/assignments/new")
def assignment_new(
    employee_id: int,
    db: Session = Depends(get_db),
    week_start: str = Form(...),
    week_end: str = Form(...),
    description: str = Form(...),
    quantity: int | None = Form(None),
    status: str = Form("pending"),
):
    emp = crud.get_employee(db, employee_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Not found")
    ws = parse_date(week_start) or dt.date.today()
    we = parse_date(week_end) or ws
    if status not in {"pending", "in_progress", "completed"}:
        status = "pending"
    crud.create_assignment(db, employee_id=employee_id, week_start=ws, week_end=we, description=description, quantity=quantity, status=status)
    return RedirectResponse(url=f"/employees/{employee_id}", status_code=303)


def filter_context(db: Session):
    names = crud.distinct_names(db, limit=200)
    categories = sorted(set(INCOMING_CATEGORIES + OUTGOING_CATEGORIES + crud.distinct_categories(db, limit=200)))
    return {"suggested_names": names, "filter_categories": categories}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)
    ctx = common_context(request)
    ctx.update({"error": None})
    return TEMPLATES.TemplateResponse("login.html", ctx)


@app.post("/login")
def login_submit(request: Request, username: str = Form(""), password: str = Form("")):
    if username == ADMIN_USER and password == ADMIN_PASSWORD:
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    ctx = common_context(request)
    ctx.update({"error": "Invalid username or password."})
    return TEMPLATES.TemplateResponse("login.html", ctx)


@app.get("/logout")
def logout(request: Request):
    try:
        request.session.clear()
    except Exception:
        pass
    return RedirectResponse(url="/login", status_code=303)


def _seed_is_enabled() -> bool:
    return (os.getenv("ENABLE_SEED") or "").strip() == "1"


def _require_seed_token(token: str | None) -> None:
    if not _seed_is_enabled():
        raise HTTPException(status_code=404, detail="Not found")
    expected = (os.getenv("SEED_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="SEED_TOKEN is not configured")
    if (token or "").strip() != expected:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/admin/seed", response_class=HTMLResponse)
def admin_seed(request: Request):
    if not _seed_is_enabled():
        raise HTTPException(status_code=404, detail="Not found")
    html = """
    <html><head><title>Seed Test Data</title></head>
    <body style='font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; padding:24px;'>
      <h2 style='margin:0 0 8px 0;'>Seed Test Data</h2>
      <div style='color:#666; margin:0 0 16px 0;'>Temporary: creates sample employees and transactions</div>
      <form method='post' action='/admin/seed/run' style='max-width:420px;'>
        <label style='display:block; margin:0 0 6px 0;'>Seed Token</label>
        <input name='token' placeholder='Enter SEED_TOKEN' style='width:100%; padding:10px; border:1px solid #ccc; border-radius:8px; margin:0 0 12px 0;' />
        <button type='submit' style='padding:10px 14px; border-radius:10px; border:0; background:#2563eb; color:#fff; font-weight:600;'>Create Test Data</button>
      </form>
      <div style='color:#666; margin-top:14px; font-size:13px;'>Requires env: ENABLE_SEED=1 and SEED_TOKEN.</div>
      <div style='margin-top:14px; font-size:13px;'><a href='/employees'>Employees</a> | <a href='/transactions'>Transactions</a></div>
    </body></html>
    """
    return HTMLResponse(content=html)


@app.post("/admin/seed/run", response_class=HTMLResponse)
def admin_seed_run(request: Request, db: Session = Depends(get_db), token: str | None = Form(None)):
    _require_seed_token(token)

    marker = "seed_v1"
    created_employees: list[str] = []
    created_transactions = 0

    seed_employees = [
        {"full_name": "Murtaza", "category": "Supervisor / Office Staff", "work_type": "contract"},
        {"full_name": "Waseem", "category": "Polish Worker", "work_type": "daily"},
        {"full_name": "Razaq", "category": "Upholstery / Poshish Worker", "work_type": "daily"},
        {"full_name": "Yaseen", "category": "Helper / Mazdoor", "work_type": "daily"},
    ]

    by_name: dict[str, Employee] = {}
    for e in seed_employees:
        name = e["full_name"]
        emp = db.execute(select(Employee).where(Employee.full_name.ilike(name))).scalar_one_or_none()
        if not emp:
            emp = crud.create_employee(
                db,
                full_name=name,
                father_name=None,
                cnic=None,
                mobile_number=None,
                address=None,
                emergency_contact=None,
                joining_date=dt.date.today(),
                status="active",
                category=e["category"],
                work_type=e["work_type"],
                role_description=None,
                payment_rate=None,
                profile_image_url="",
            )
            created_employees.append(name)
        by_name[name.lower()] = emp

    existing_seed = db.execute(
        select(func.count(Transaction.id))
        .where(Transaction.is_deleted.is_(False))
        .where(Transaction.reference == marker)
    ).scalar_one()
    if int(existing_seed or 0) == 0:
        today = dt.date.today()

        crud.create_transaction(
            db,
            type="incoming",
            date=today,
            amount_pkr=250000,
            category=INCOMING_CATEGORIES[0],
            name="Customer",
            bill_no="SEED-1",
            notes="seed",
            reference=marker,
        )
        created_transactions += 1

        for nm, amt, cat, tx_type in [
            ("waseem", 9000, "Polish Wala", "salary"),
            ("razaq", 7000, "Poshish Wala", "salary"),
            ("yaseen", 1500, "Employee", "advance"),
        ]:
            emp = by_name.get(nm)
            crud.create_transaction(
                db,
                type="outgoing",
                date=today,
                amount_pkr=int(amt),
                category=cat,
                name=emp.full_name if emp else nm,
                bill_no=None,
                notes="seed",
                employee_id=emp.id if emp else None,
                employee_tx_type=tx_type,
                payment_method=PAYMENT_METHODS[0] if PAYMENT_METHODS else None,
                reference=marker,
            )
            created_transactions += 1

    _backfill_employees_from_transactions(db)

    items = "".join([f"<li>{n}</li>" for n in created_employees])
    html = f"""
    <html><head><title>Seed Result</title></head>
    <body style='font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; padding:24px;'>
      <h2 style='margin:0 0 8px 0;'>Seed Test Data</h2>
      <div style='color:#666; margin:0 0 16px 0;'>Completed</div>
      <div style='margin:0 0 12px 0;'><b>Marker:</b> {marker}</div>
      <div style='margin:0 0 12px 0;'><b>Employees created:</b> {len(created_employees)}</div>
      {'<ul style="margin:0 0 12px 18px;">' + items + '</ul>' if created_employees else ''}
      <div style='margin:0 0 12px 0;'><b>Transactions created:</b> {created_transactions}</div>
      <div style='margin-top:14px; font-size:13px;'><a href='/employees'>Employees</a> | <a href='/transactions'>Transactions</a> | <a href='/admin/seed'>Back</a></div>
    </body></html>
    """
    return HTMLResponse(content=html)


@app.get("/admin/tx-names-debug", response_class=HTMLResponse)
def tx_names_debug(request: Request, db: Session = Depends(get_db)):
    # Show all distinct transaction names and categories
    stmt = select(Transaction.name, Transaction.category).where(Transaction.is_deleted.is_(False)).where(Transaction.name.is_not(None)).distinct()
    rows = db.execute(stmt).all()
    # Show all current employees
    emp_rows = db.execute(select(Employee.full_name, Employee.category)).all()
    ctx = common_context(request)
    ctx.update({"tx_rows": rows, "emp_rows": emp_rows})
    return TEMPLATES.TemplateResponse("admin_tx_names_debug.html", ctx)


@app.get("/admin/sync-employees-from-transactions", response_class=HTMLResponse)
def sync_employees_from_transactions(request: Request, db: Session = Depends(get_db)):
    stmt = select(Transaction.name, Transaction.category).where(Transaction.is_deleted.is_(False)).where(Transaction.name.is_not(None)).distinct()
    rows = db.execute(stmt).all()
    # Debug: log all distinct name+category
    print("DEBUG: distinct name+category rows:", rows)

    created = []
    linked = 0
    errors: list[str] = []
    for name, category in rows:
        try:
            if not name or not name.strip():
                continue
            name = name.strip()
            print(f"DEBUG: processing name='{name}' category='{category}'")

            # Special case: ensure two distinct employees for Murtaza if both categories exist
            if name.lower() == "murtaza" and category in {"Employee", "Karkhanay Wala"}:
                # Try to find existing employee with matching name and category mapping
                emp = None
                existing = db.execute(select(Employee).where(Employee.full_name.ilike(name))).scalars().all()
                print(f"DEBUG: Murtaza existing employees: {existing}")
                for e in existing:
                    emp_cat = _employee_outgoing_category(e)
                    if emp_cat == category:
                        emp = e
                        break
                if not emp:
                    emp = crud.create_employee(
                        db,
                        full_name=name,
                        father_name=None,
                        cnic=None,
                        mobile_number=None,
                        address=None,
                        emergency_contact=None,
                        joining_date=dt.date.today(),
                        status="active",
                        category=_map_category_to_employee_category(category),
                        work_type="daily",
                        role_description=None,
                        payment_rate=None,
                        profile_image_url="",
                    )
                    created.append(f"{name} ({category})")
                    print(f"DEBUG: created Murtaza employee for category '{category}'")
            else:
                # General case: one employee per name
                emp = db.execute(select(Employee).where(Employee.full_name.ilike(name))).scalar_one_or_none()
                print(f"DEBUG: existing employee for '{name}': {emp}")
                if not emp:
                    emp = crud.create_employee(
                        db,
                        full_name=name,
                        father_name=None,
                        cnic=None,
                        mobile_number=None,
                        address=None,
                        emergency_contact=None,
                        joining_date=dt.date.today(),
                        status="active",
                        category=_map_category_to_employee_category(category),
                        work_type="daily",
                        role_description=None,
                        payment_rate=None,
                        profile_image_url="",
                    )
                    created.append(name)
                    print(f"DEBUG: created employee for '{name}' category '{category}'")

            # Link all transactions for this name+category to the employee
            txs = db.execute(
                select(Transaction)
                .where(Transaction.is_deleted.is_(False))
                .where(Transaction.name.ilike(name))
                .where(Transaction.category == category)
                .where(Transaction.employee_id.is_(None))
            ).scalars().all()
            print(f"DEBUG: linking {len(txs)} transactions for '{name}' category '{category}'")
            for tx in txs:
                tx.employee_id = emp.id
                if not tx.employee_tx_type:
                    tx.employee_tx_type = "salary"
            linked += len(txs)
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            errors.append(f"{name} / {category}: {e}")

    db.commit()
    ctx = common_context(request)
    ctx.update({"created": created, "linked": linked, "debug_rows": rows, "errors": errors})
    return TEMPLATES.TemplateResponse("admin_sync_result.html", ctx)


def _map_category_to_employee_category(tx_category: str) -> str:
    """Map outgoing transaction category to an employee profile category."""
    cat = tx_category.lower()
    if "karkhan" in cat or "factory" in cat:
        return "Factory Worker (Karkhanay Wala)"
    if "polish" in cat:
        return "Polish Worker"
    if "poshish" in cat or "upholstery" in cat:
        return "Upholstery / Poshish Worker"
    return "Helper / Mazdoor"


def _backfill_employees_from_transactions(db: Session) -> None:
    try:
        rows = db.execute(
            select(Transaction.name, Transaction.category)
            .where(Transaction.is_deleted.is_(False))
            .where(Transaction.type == "outgoing")
            .where(Transaction.name.is_not(None))
            .distinct()
        ).all()
    except Exception:
        return

    for raw_name, tx_category in rows:
        if not raw_name:
            continue
        name = raw_name.strip()
        if not name:
            continue

        # Create or find employee
        emp = db.execute(select(Employee).where(Employee.full_name.ilike(name))).scalar_one_or_none()
        if not emp:
            try:
                emp = crud.create_employee(
                    db,
                    full_name=name,
                    father_name=None,
                    cnic=None,
                    mobile_number=None,
                    address=None,
                    emergency_contact=None,
                    joining_date=dt.date.today(),
                    status="active",
                    category=_map_category_to_employee_category(tx_category or ""),
                    work_type="daily",
                    role_description=None,
                    payment_rate=None,
                    profile_image_url="",
                )
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
                continue

        # Link transactions for this name to the employee (only if not already linked)
        try:
            txs = db.execute(
                select(Transaction)
                .where(Transaction.is_deleted.is_(False))
                .where(Transaction.type == "outgoing")
                .where(Transaction.name.ilike(name))
                .where(Transaction.employee_id.is_(None))
            ).scalars().all()
        except Exception:
            continue

        for tx in txs:
            tx.employee_id = emp.id
            if not tx.employee_tx_type:
                tx.employee_tx_type = "salary"

    try:
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    ctx = common_context(request)
    return TEMPLATES.TemplateResponse("home.html", ctx)


@app.get("/daily", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    today = dt.date.today()
    incoming, outgoing, net = crud.totals(db, from_date=today, to_date=today, type=None, category=None, name=None, q=None)

    week = sat_thu_week_range(today)
    w_in, w_out, w_net = crud.totals(db, from_date=week.start, to_date=week.end, type=None, category=None, name=None, q=None)

    recent = crud.list_transactions(db, from_date=None, to_date=None, type=None, category=None, name=None, q=None, limit=10)

    ctx = common_context(request)
    ctx.update(
        {
            "today_incoming": incoming,
            "today_outgoing": outgoing,
            "today_net": net,
            "week_start": week.start,
            "week_end": week.end,
            "week_incoming": w_in,
            "week_outgoing": w_out,
            "week_net": w_net,
            "recent": recent,
        }
    )
    return TEMPLATES.TemplateResponse("dashboard.html", ctx)


@app.get("/daily-in-out")
def daily_in_out():
    return RedirectResponse(url="/transactions", status_code=303)


@app.get("/coming-soon/{feature}", response_class=HTMLResponse)
def coming_soon(request: Request, feature: str):
    ctx = common_context(request)
    ctx.update({"feature": feature})
    return TEMPLATES.TemplateResponse("coming_soon.html", ctx)


def _inventory_badge_class(badge: str) -> str:
    if badge == "In Stock":
        return "text-bg-success"
    if badge == "Low Stock":
        return "text-bg-warning"
    if badge == "Out of Stock":
        return "text-bg-danger"
    if badge == "Made to Order":
        return "text-bg-secondary"
    return "text-bg-secondary"


def _ensure_inventory_seeded(db: Session) -> None:
    try:
        crud.ensure_inventory_seed(db)
    except Exception:
        return


@app.get("/inventory", response_class=HTMLResponse)
def inventory_index(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)
    ctx = common_context(request)
    ctx.update({"stats": crud.inventory_dashboard_stats(db)})
    return TEMPLATES.TemplateResponse("inventory_index.html", ctx)


@app.get("/inventory/sofas", response_class=HTMLResponse)
def inventory_sofas(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)

    roots = crud.list_inventory_categories(db, type="FURNITURE", parent_id=None)
    root_id = next((c.id for c in roots if (c.name or "").lower() == "furniture"), None)
    sofa_types: list[str] = []
    if root_id is not None:
        cats = crud.list_inventory_categories(db, type="FURNITURE", parent_id=root_id)
        sofa_cat = next((c for c in cats if (c.name or "").lower() == "sofa"), None)
        if sofa_cat is not None:
            sofa_types = [c.name for c in crud.list_inventory_categories(db, type="FURNITURE", parent_id=sofa_cat.id)]

    items = crud.list_sofa_items(db, q=None, sofa_type=None, limit=500)
    cards = crud.sofa_cards(db, items=items)
    for c in cards:
        c["badge_class"] = _inventory_badge_class(str(c.get("badge") or ""))

    ctx = common_context(request)
    ctx.update({"cards": cards, "sofa_types": sofa_types})
    return TEMPLATES.TemplateResponse("inventory_sofas.html", ctx)


@app.post("/inventory/sofas", response_class=HTMLResponse)
def inventory_sofas_create(
    request: Request,
    db: Session = Depends(get_db),
    item_id: str | None = Form(None),
    name: str = Form(...),
    sofa_type: str = Form(...),
    hardware_material: str | None = Form(None),
    poshish_material: str | None = Form(None),
    seating_capacity: str | None = Form(None),
    qty_on_hand: int = Form(0),
    cost_price_pkr: int = Form(0),
    sale_price_pkr: int = Form(0),
    notes: str | None = Form(None),
):
    _ensure_inventory_seeded(db)
    edit_id: int | None = None
    try:
        if item_id and str(item_id).strip():
            edit_id = int(str(item_id).strip())
    except Exception:
        edit_id = None

    it = None
    if edit_id is not None:
        it = crud.update_sofa_item(
            db,
            item_id=edit_id,
            name=name,
            sofa_type=sofa_type,
            hardware_material=hardware_material,
            poshish_material=poshish_material,
            seating_capacity=seating_capacity,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    if it is None:
        crud.create_sofa_item(
            db,
            name=name,
            sofa_type=sofa_type,
            hardware_material=hardware_material,
            poshish_material=poshish_material,
            seating_capacity=seating_capacity,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    return RedirectResponse(url="/inventory/sofas", status_code=303)


@app.post("/inventory/sofas/{item_id}/delete", response_class=HTMLResponse)
def inventory_sofas_delete(item_id: int, db: Session = Depends(get_db)):
    crud.soft_delete_sofa_item(db, item_id=item_id)
    return RedirectResponse(url="/inventory/sofas", status_code=303)


@app.get("/inventory/hardware", response_class=HTMLResponse)
def inventory_hardware(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)
    items = crud.list_hardware_materials(db, q=None, limit=500)
    cards = crud.hardware_cards(db, items=items)
    for c in cards:
        c["badge_class"] = _inventory_badge_class(str(c.get("badge") or ""))
    ctx = common_context(request)
    ctx.update({"cards": cards})
    return TEMPLATES.TemplateResponse("inventory_hardware.html", ctx)


@app.post("/inventory/hardware", response_class=HTMLResponse)
def inventory_hardware_create(
    request: Request,
    db: Session = Depends(get_db),
    item_id: str | None = Form(None),
    name: str = Form(...),
    unit: str = Form("pieces"),
    qty_on_hand: int = Form(0),
    cost_price_pkr: int = Form(0),
    sale_price_pkr: int = Form(0),
    notes: str | None = Form(None),
):
    _ensure_inventory_seeded(db)
    edit_id: int | None = None
    try:
        if item_id and str(item_id).strip():
            edit_id = int(str(item_id).strip())
    except Exception:
        edit_id = None

    it = None
    if edit_id is not None:
        it = crud.update_hardware_material(
            db,
            item_id=edit_id,
            name=name,
            unit=unit,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    if it is None:
        crud.create_hardware_material(
            db,
            name=name,
            unit=unit,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    return RedirectResponse(url="/inventory/hardware", status_code=303)


@app.post("/inventory/hardware/{item_id}/delete", response_class=HTMLResponse)
def inventory_hardware_delete(item_id: int, db: Session = Depends(get_db)):
    crud.soft_delete_hardware_material(db, item_id=item_id)
    return RedirectResponse(url="/inventory/hardware", status_code=303)


@app.get("/inventory/poshish", response_class=HTMLResponse)
def inventory_poshish(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)
    items = crud.list_poshish_materials(db, q=None, limit=500)
    cards = crud.poshish_cards(db, items=items)
    for c in cards:
        c["badge_class"] = _inventory_badge_class(str(c.get("badge") or ""))
    ctx = common_context(request)
    ctx.update({"cards": cards})
    return TEMPLATES.TemplateResponse("inventory_poshish.html", ctx)


@app.post("/inventory/poshish", response_class=HTMLResponse)
def inventory_poshish_create(
    request: Request,
    db: Session = Depends(get_db),
    item_id: str | None = Form(None),
    name: str = Form(...),
    color: str | None = Form(None),
    unit: str = Form("meters"),
    qty_on_hand: int = Form(0),
    cost_price_pkr: int = Form(0),
    sale_price_pkr: int = Form(0),
    notes: str | None = Form(None),
):
    _ensure_inventory_seeded(db)
    edit_id: int | None = None
    try:
        if item_id and str(item_id).strip():
            edit_id = int(str(item_id).strip())
    except Exception:
        edit_id = None

    it = None
    if edit_id is not None:
        it = crud.update_poshish_material(
            db,
            item_id=edit_id,
            name=name,
            color=color,
            unit=unit,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    if it is None:
        crud.create_poshish_material(
            db,
            name=name,
            color=color,
            unit=unit,
            qty_on_hand=int(qty_on_hand or 0),
            cost_price_pkr=int(cost_price_pkr or 0),
            sale_price_pkr=int(sale_price_pkr or 0),
            notes=notes,
        )
    return RedirectResponse(url="/inventory/poshish", status_code=303)


@app.post("/inventory/poshish/{item_id}/delete", response_class=HTMLResponse)
def inventory_poshish_delete(item_id: int, db: Session = Depends(get_db)):
    crud.soft_delete_poshish_material(db, item_id=item_id)
    return RedirectResponse(url="/inventory/poshish", status_code=303)


@app.get("/inventory/stock/transactions", response_class=HTMLResponse)
def inventory_stock_transactions(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)
    cards = crud.stock_movement_cards(db, limit=500)
    ctx = common_context(request)
    ctx.update({"cards": cards})
    return TEMPLATES.TemplateResponse("inventory_stock_transactions.html", ctx)


@app.get("/inventory/furniture", response_class=HTMLResponse)
def inventory_furniture(request: Request, db: Session = Depends(get_db), category: str | None = None):
    _ensure_inventory_seeded(db)

    roots = crud.list_inventory_categories(db, type="FURNITURE", parent_id=None)
    root_id = next((c.id for c in roots if (c.name or "").lower() == "furniture"), None)

    furniture_categories: list[InventoryCategory] = []
    furniture_subcategories: list[InventoryCategory] = []
    if root_id is not None:
        furniture_categories = crud.list_inventory_categories(db, type="FURNITURE", parent_id=root_id)
        for cat in furniture_categories:
            furniture_subcategories.extend(crud.list_inventory_categories(db, type="FURNITURE", parent_id=cat.id))

    preset_category_id: int | None = None
    if category:
        ckey = category.strip().lower()
        if ckey in {"sofa", "hardware"}:
            for c in furniture_categories:
                n = (c.name or "").lower()
                if ckey == "sofa" and n == "sofa":
                    preset_category_id = c.id
                    break
                if ckey == "hardware" and n == "hardware":
                    preset_category_id = c.id
                    break

    items = crud.list_furniture_items_filtered(db, q=None, category_id=None, limit=500)
    cards = crud.furniture_cards(db, items=items)

    cat_name_by_id = {c.id: c.name for c in furniture_categories}
    subcat_name_by_id = {c.id: c.name for c in furniture_subcategories}

    for c in cards:
        it = c.get("item")
        if it is None:
            continue
        c["badge_class"] = _inventory_badge_class(str(c.get("badge") or ""))
        c["category_name"] = cat_name_by_id.get(getattr(it, "category_id", None), "")
        sc_id = getattr(it, "sub_category_id", None)
        c["sub_category_name"] = subcat_name_by_id.get(sc_id) if sc_id else ""

    ctx = common_context(request)
    ctx.update(
        {
            "cards": cards,
            "furniture_categories": furniture_categories,
            "furniture_subcategories": furniture_subcategories,
            "bed_sizes": crud.list_bed_sizes(db),
            "preset_category_id": preset_category_id,
        }
    )
    return TEMPLATES.TemplateResponse("inventory_furniture.html", ctx)


@app.post("/inventory/furniture", response_class=HTMLResponse)
def inventory_furniture_create(
    request: Request,
    db: Session = Depends(get_db),
    item_id: str | None = Form(None),
    name: str = Form(...),
    category_id: int = Form(...),
    sub_category_id: str | None = Form(None),
    bed_size_id: str = Form(...),
    material_type: str = Form("Wood"),
    qty_on_hand: int = Form(0),
    cost_price_pkr: int = Form(0),
    sale_price_pkr: int = Form(0),
    notes: str | None = Form(None),
):
    _ensure_inventory_seeded(db)

    edit_item_id: int | None = None
    try:
        if item_id and str(item_id).strip():
            edit_item_id = int(str(item_id).strip())
    except Exception:
        edit_item_id = None

    sub_id: int | None = None
    try:
        if sub_category_id and sub_category_id.strip():
            sub_id = int(sub_category_id)
    except Exception:
        sub_id = None

    item = None
    if edit_item_id is not None:
        item = crud.update_furniture_item(
            db,
            item_id=edit_item_id,
            name=name,
            material_type=material_type or "Wood",
            status="IN_STOCK",
            category_id=category_id,
            sub_category_id=sub_id,
            notes=notes,
        )

    if item is None:
        sku = f"FUR-{int(dt.datetime.utcnow().timestamp())}"
        item = crud.create_furniture_item(
            db,
            name=name,
            sku=sku,
            material_type=material_type or "Wood",
            color_finish=None,
            status="IN_STOCK",
            category_id=category_id,
            sub_category_id=sub_id,
            notes=notes,
        )

    bs_id: int | None = None
    try:
        if bed_size_id != "custom":
            bs_id = int(bed_size_id)
    except Exception:
        bs_id = None

    crud.upsert_furniture_variant(
        db,
        furniture_item_id=item.id,
        bed_size_id=bs_id,
        qty_on_hand=int(qty_on_hand or 0),
        cost_price_pkr=int(cost_price_pkr or 0),
        sale_price_pkr=int(sale_price_pkr or 0),
        reorder_level=0,
    )
    return RedirectResponse(url="/inventory/furniture", status_code=303)


@app.post("/inventory/furniture/{item_id}/delete", response_class=HTMLResponse)
def inventory_furniture_delete(item_id: int, db: Session = Depends(get_db)):
    crud.soft_delete_furniture_item(db, item_id=item_id)
    return RedirectResponse(url="/inventory/furniture", status_code=303)


@app.get("/inventory/foam", response_class=HTMLResponse)
def inventory_foam(request: Request, db: Session = Depends(get_db)):
    _ensure_inventory_seeded(db)

    cards = crud.foam_variant_cards(db, q=None, brand_id=None, limit=500)
    for c in cards:
        c["badge_class"] = _inventory_badge_class(str(c.get("badge") or ""))

    ctx = common_context(request)
    ctx.update(
        {
            "cards": cards,
            "foam_brands": crud.list_foam_brands(db),
            "bed_sizes": crud.list_bed_sizes(db),
            "thicknesses": crud.list_thicknesses(db),
        }
    )
    return TEMPLATES.TemplateResponse("inventory_foam.html", ctx)


@app.post("/inventory/foam", response_class=HTMLResponse)
def inventory_foam_create(
    request: Request,
    db: Session = Depends(get_db),
    brand_id: int = Form(...),
    model_name: str = Form(...),
    bed_size_id: int = Form(...),
    thickness_id: int = Form(...),
    qty_on_hand: int = Form(0),
    purchase_cost_pkr: int = Form(0),
    sale_price_pkr: int = Form(0),
    notes: str | None = Form(None),
):
    _ensure_inventory_seeded(db)

    model = crud.create_foam_model(db, brand_id=brand_id, name=model_name, notes=notes)
    crud.upsert_foam_variant(
        db,
        foam_model_id=model.id,
        bed_size_id=bed_size_id,
        thickness_id=thickness_id,
        density_type=None,
        qty_on_hand=int(qty_on_hand or 0),
        purchase_cost_pkr=int(purchase_cost_pkr or 0),
        sale_price_pkr=int(sale_price_pkr or 0),
        reorder_level=0,
    )
    return RedirectResponse(url="/inventory/foam", status_code=303)


@app.post("/inventory/foam/{model_id}/delete", response_class=HTMLResponse)
def inventory_foam_delete(model_id: int, db: Session = Depends(get_db)):
    crud.soft_delete_foam_model(db, model_id=model_id)
    return RedirectResponse(url="/inventory/foam", status_code=303)


@app.post("/inventory/stock/adjust", response_class=HTMLResponse)
def inventory_stock_adjust(
    inventory_type: str = Form(...),
    variant_id: int = Form(...),
    transaction_type: str = Form("in"),
    quantity: int = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
):
    qty = int(quantity or 0)
    if qty <= 0:
        return RedirectResponse(url="/inventory", status_code=303)
    delta = qty if transaction_type == "in" else -qty
    movement = "Stock In" if transaction_type == "in" else "Stock Out"
    crud.adjust_stock(
        db,
        inventory_type=inventory_type,
        variant_id=variant_id,
        movement_type=movement,
        qty_change=delta,
        unit_cost_pkr=None,
        notes=notes,
    )
    if inventory_type == "FOAM_VARIANT":
        return RedirectResponse(url="/inventory/foam", status_code=303)
    if inventory_type == "SOFA_ITEM":
        return RedirectResponse(url="/inventory/sofas", status_code=303)
    if inventory_type == "HARDWARE_MATERIAL":
        return RedirectResponse(url="/inventory/hardware", status_code=303)
    if inventory_type == "POSHISH_MATERIAL":
        return RedirectResponse(url="/inventory/poshish", status_code=303)
    return RedirectResponse(url="/inventory/furniture", status_code=303)


@app.get("/add", response_class=HTMLResponse)
def add_payment(request: Request, db: Session = Depends(get_db), type: str = "incoming"):
    if type not in {"incoming", "outgoing"}:
        type = "incoming"

    ctx = common_context(request)
    employees = crud.list_employees(db, status="active") if type == "outgoing" else []
    ctx.update(
        {
            "mode": "create",
            "type": type,
            "tx": None,
            "errors": {},
            "employees": employees,
        }
    )
    return TEMPLATES.TemplateResponse("payment_form.html", ctx)


def validate_form(type: str, category: str, bill_no: str | None, amount_pkr: int):
    errors: dict[str, str] = {}

    if type not in {"incoming", "outgoing"}:
        errors["type"] = "Invalid type."

    if amount_pkr <= 0:
        errors["amount_pkr"] = "Amount must be greater than 0."

    if type == "incoming":
        if category not in INCOMING_CATEGORIES:
            errors["category"] = "Invalid incoming source."
        if category == "Client" and not (bill_no or "").strip():
            errors["bill_no"] = "Bill Number is required for Client payments."

    if type == "outgoing":
        if category not in OUTGOING_CATEGORIES:
            errors["category"] = "Invalid outgoing category."

    return errors


@app.post("/add", response_class=HTMLResponse)
def add_payment_post(
    request: Request,
    db: Session = Depends(get_db),
    type: str = Form(...),
    date: str = Form(...),
    amount_pkr: int = Form(...),
    category: str = Form(...),
    name: str | None = Form(None),
    bill_no: str | None = Form(None),
    notes: str | None = Form(None),
    employee_id: str | None = Form(None),
    employee_tx_type: str | None = Form(None),
    payment_method: str | None = Form(None),
    reference: str | None = Form(None),
):
    parsed_date = parse_date(date)
    if not parsed_date:
        parsed_date = dt.date.today()

    parsed_employee_id: int | None = None
    try:
        if employee_id is not None and str(employee_id).strip():
            parsed_employee_id = int(str(employee_id).strip())
    except Exception:
        parsed_employee_id = None

    emp = None
    if type == "outgoing" and parsed_employee_id:
        emp = crud.get_employee(db, parsed_employee_id)
        if emp:
            category = _employee_outgoing_category(emp)
            if not (name or "").strip():
                name = emp.full_name

    errors = validate_form(type, category, bill_no, amount_pkr)
    if type == "outgoing" and parsed_employee_id:
        if not emp:
            errors["employee_id"] = "Invalid employee."
    if errors:
        ctx = common_context(request)
        employees = crud.list_employees(db, status="active") if type == "outgoing" else []
        ctx.update(
            {
                "mode": "create",
                "type": type,
                "tx": {
                    "date": parsed_date,
                    "amount_pkr": amount_pkr,
                    "category": category,
                    "name": name,
                    "bill_no": bill_no,
                    "notes": notes,
                    "employee_id": parsed_employee_id,
                    "employee_tx_type": employee_tx_type,
                    "payment_method": payment_method,
                    "reference": reference,
                },
                "errors": errors,
                "employees": employees,
            }
        )
        return TEMPLATES.TemplateResponse("payment_form.html", ctx, status_code=400)

    crud.create_transaction(
        db,
        type=type,
        date=parsed_date,
        amount_pkr=int(amount_pkr),
        category=category,
        name=name,
        bill_no=bill_no,
        notes=notes,
        employee_id=parsed_employee_id if type == "outgoing" else None,
        employee_tx_type=employee_tx_type if type == "outgoing" else None,
        payment_method=payment_method if type == "outgoing" else None,
        reference=reference if type == "outgoing" else None,
    )
    return RedirectResponse(url="/transactions", status_code=303)


@app.get("/transactions", response_class=HTMLResponse)
def transactions(
    request: Request,
    db: Session = Depends(get_db),
    from_date: str | None = None,
    to_date: str | None = None,
    type: str | None = None,
    category: str | None = None,
    name: str | None = None,
    q: str | None = None,
):
    f = parse_date(from_date)
    t = parse_date(to_date)
    f, t = clamp_date_range(f, t)

    items = crud.list_transactions(db, from_date=f, to_date=t, type=type, category=category, name=name, q=q, limit=500)
    incoming, outgoing, net = crud.totals(db, from_date=f, to_date=t, type=type, category=category, name=name, q=q)

    ctx = common_context(request)
    ctx.update(filter_context(db))
    ctx.update(
        {
            "items": items,
            "filters": {
                "from_date": f.isoformat() if f else "",
                "to_date": t.isoformat() if t else "",
                "type": type or "",
                "category": category or "",
                "name": name or "",
                "q": q or "",
            },
            "totals": {"incoming": incoming, "outgoing": outgoing, "net": net},
        }
    )

    return TEMPLATES.TemplateResponse("transactions.html", ctx)


@app.get("/edit/{tx_id}", response_class=HTMLResponse)
def edit_payment(request: Request, tx_id: int, db: Session = Depends(get_db)):
    tx = crud.get_transaction(db, tx_id)
    if not tx or tx.is_deleted:
        raise HTTPException(status_code=404, detail="Not found")

    ctx = common_context(request)
    employees = crud.list_employees(db, status="active") if tx.type == "outgoing" else []
    ctx.update({"mode": "edit", "type": tx.type, "tx": tx, "errors": {}, "employees": employees})
    return TEMPLATES.TemplateResponse("payment_form.html", ctx)


@app.post("/edit/{tx_id}", response_class=HTMLResponse)
def edit_payment_post(
    request: Request,
    tx_id: int,
    db: Session = Depends(get_db),
    date: str = Form(...),
    amount_pkr: int = Form(...),
    category: str = Form(...),
    name: str | None = Form(None),
    bill_no: str | None = Form(None),
    notes: str | None = Form(None),
    employee_id: str | None = Form(None),
    employee_tx_type: str | None = Form(None),
    payment_method: str | None = Form(None),
    reference: str | None = Form(None),
):
    tx = crud.get_transaction(db, tx_id)
    if not tx or tx.is_deleted:
        raise HTTPException(status_code=404, detail="Not found")

    parsed_date = parse_date(date)
    if not parsed_date:
        parsed_date = dt.date.today()

    parsed_employee_id: int | None = None
    try:
        if employee_id is not None and str(employee_id).strip():
            parsed_employee_id = int(str(employee_id).strip())
    except Exception:
        parsed_employee_id = None

    emp = None
    if tx.type == "outgoing" and parsed_employee_id:
        emp = crud.get_employee(db, parsed_employee_id)
        if emp:
            category = _employee_outgoing_category(emp)
            if not (name or "").strip():
                name = emp.full_name

    errors = validate_form(tx.type, category, bill_no, int(amount_pkr))
    if tx.type == "outgoing" and parsed_employee_id:
        if not emp:
            errors["employee_id"] = "Invalid employee."
    if errors:
        ctx = common_context(request)
        employees = crud.list_employees(db, status="active") if tx.type == "outgoing" else []
        ctx.update(
            {
                "mode": "edit",
                "type": tx.type,
                "tx": {
                    "id": tx.id,
                    "type": tx.type,
                    "date": parsed_date,
                    "amount_pkr": amount_pkr,
                    "category": category,
                    "name": name,
                    "bill_no": bill_no,
                    "notes": notes,
                    "employee_id": parsed_employee_id,
                    "employee_tx_type": employee_tx_type,
                    "payment_method": payment_method,
                    "reference": reference,
                },
                "errors": errors,
                "employees": employees,
            }
        )
        return TEMPLATES.TemplateResponse("payment_form.html", ctx, status_code=400)

    crud.update_transaction(
        db,
        tx,
        date=parsed_date,
        amount_pkr=int(amount_pkr),
        category=category,
        name=name,
        bill_no=bill_no,
        notes=notes,
        employee_id=parsed_employee_id if tx.type == "outgoing" else None,
        employee_tx_type=employee_tx_type if tx.type == "outgoing" else None,
        payment_method=payment_method if tx.type == "outgoing" else None,
        reference=reference if tx.type == "outgoing" else None,
    )

    return RedirectResponse(url="/transactions", status_code=303)


@app.post("/delete/{tx_id}")
def delete_payment(tx_id: int, db: Session = Depends(get_db)):
    tx = crud.get_transaction(db, tx_id)
    if not tx or tx.is_deleted:
        raise HTTPException(status_code=404, detail="Not found")

    crud.soft_delete_transaction(db, tx)
    return RedirectResponse(url="/transactions", status_code=303)


def period_range(period: str, anchor: dt.date) -> tuple[dt.date, dt.date]:
    if period == "daily":
        return anchor, anchor
    if period == "weekly":
        wr = sat_thu_week_range(anchor)
        return wr.start, wr.end
    if period == "monthly":
        start = anchor.replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - dt.timedelta(days=1)
        return start, end
    return anchor, anchor


@app.get("/reports", response_class=HTMLResponse)
def reports(
    request: Request,
    db: Session = Depends(get_db),
    period: str = "daily",
    anchor: str | None = None,
    type: str | None = None,
    category: str | None = None,
    name: str | None = None,
    q: str | None = None,
):
    if period not in {"daily", "weekly", "monthly"}:
        period = "daily"

    anchor_date = parse_date(anchor) or dt.date.today()
    start, end = period_range(period, anchor_date)

    items = crud.list_transactions(db, from_date=start, to_date=end, type=type, category=category, name=name, q=q, limit=2000)
    incoming, outgoing, net = crud.totals(db, from_date=start, to_date=end, type=type, category=category, name=name, q=q)

    by_day: dict[str, dict[str, int]] = {}
    outgoing_by_cat: dict[str, int] = {}
    for tx in items:
        k = tx.date.isoformat()
        by_day.setdefault(k, {"incoming": 0, "outgoing": 0})
        by_day[k][tx.type] += int(tx.amount_pkr)
        if tx.type == "outgoing":
            outgoing_by_cat[tx.category] = outgoing_by_cat.get(tx.category, 0) + int(tx.amount_pkr)

    labels = sorted(by_day.keys())
    incoming_series = [by_day[d]["incoming"] for d in labels]
    outgoing_series = [by_day[d]["outgoing"] for d in labels]

    net_series = []
    running = 0
    for inc, out in zip(incoming_series, outgoing_series):
        running += inc - out
        net_series.append(running)

    if len(labels) > 31:
        labels = labels[-31:]
        incoming_series = incoming_series[-31:]
        outgoing_series = outgoing_series[-31:]
        net_series = net_series[-31:]

    ctx = common_context(request)
    ctx.update(filter_context(db))
    ctx.update(
        {
            "period": period,
            "anchor": anchor_date,
            "start": start,
            "end": end,
            "items": items,
            "filters": {"type": type or "", "category": category or "", "name": name or "", "q": q or ""},
            "totals": {"incoming": incoming, "outgoing": outgoing, "net": net},
            "chart": {
                "labels": labels,
                "incoming": incoming_series,
                "outgoing": outgoing_series,
                "outgoing_by_cat": outgoing_by_cat,
                "cumulative_net": net_series,
            },
        }
    )

    return TEMPLATES.TemplateResponse("reports.html", ctx)


def to_dataframe(items: list[Transaction]):
    try:
        import pandas as pd
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export requires pandas. Install pandas or disable export. ({e})")

    rows = []
    for t in items:
        rows.append(
            {
                "ID": t.id,
                "Date": t.date.isoformat(),
                "Type": t.type,
                "Category": t.category,
                "Name": t.name or "",
                "Bill No": t.bill_no or "",
                "Amount (PKR)": int(t.amount_pkr),
                "Notes": t.notes or "",
            }
        )
    return pd.DataFrame(rows)


@app.get("/export/xlsx")
def export_xlsx(
    request: Request,
    db: Session = Depends(get_db),
    from_date: str | None = None,
    to_date: str | None = None,
    type: str | None = None,
    category: str | None = None,
    name: str | None = None,
    q: str | None = None,
):
    params = {
        "from_date": from_date or "",
        "to_date": to_date or "",
        "type": type or "",
        "category": category or "",
        "name": name or "",
        "q": q or "",
    }
    from urllib.parse import urlencode

    url = "/export/pdf?" + urlencode(params)
    return RedirectResponse(url=url, status_code=303)


@app.get("/export/pdf")
def export_pdf(
    db: Session = Depends(get_db),
    from_date: str | None = None,
    to_date: str | None = None,
    type: str | None = None,
    category: str | None = None,
    name: str | None = None,
    q: str | None = None,
):
    f = parse_date(from_date)
    t = parse_date(to_date)
    f, t = clamp_date_range(f, t)

    items = crud.list_transactions(db, from_date=f, to_date=t, type=type, category=category, name=name, q=q, limit=3000)
    incoming, outgoing, net = crud.totals(db, from_date=f, to_date=t, type=type, category=category, name=name, q=q)

    by_day: dict[str, dict[str, int]] = {}
    outgoing_by_cat: dict[str, int] = {}
    for tx in items:
        k = tx.date.isoformat()
        by_day.setdefault(k, {"incoming": 0, "outgoing": 0})
        by_day[k][tx.type] += int(tx.amount_pkr)
        if tx.type == "outgoing":
            outgoing_by_cat[tx.category] = outgoing_by_cat.get(tx.category, 0) + int(tx.amount_pkr)

    labels = sorted(by_day.keys())
    incoming_series = [by_day[d]["incoming"] for d in labels]
    outgoing_series = [by_day[d]["outgoing"] for d in labels]
    cumulative_net = []
    running = 0
    for inc, out in zip(incoming_series, outgoing_series):
        running += inc - out
        cumulative_net.append(running)

    chart_labels = labels[-14:] if len(labels) > 14 else labels
    chart_incoming = incoming_series[-14:] if len(incoming_series) > 14 else incoming_series
    chart_outgoing = outgoing_series[-14:] if len(outgoing_series) > 14 else outgoing_series
    chart_cum_net = cumulative_net[-14:] if len(cumulative_net) > 14 else cumulative_net

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, title="Nusrat Furniture Report", leftMargin=1.2 * cm, rightMargin=1.2 * cm, topMargin=1.2 * cm, bottomMargin=1.2 * cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("nf_title", parent=styles["Title"], alignment=TA_CENTER)
    small_style = ParagraphStyle("nf_small", parent=styles["Normal"], fontSize=9, textColor=colors.HexColor("#4b5563"))

    period_txt = "All dates" if (not f and not t) else f"From {f.isoformat() if f else '...'} to {t.isoformat() if t else '...'}"

    story: list[Flowable] = []
    story.append(Paragraph("Nusrat Furniture — Dashboard Report", title_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(period_txt, small_style))
    applied = []
    if type:
        applied.append(f"Type={type}")
    if category:
        applied.append(f"Category={category}")
    if name:
        applied.append(f"Name={name}")
    if q:
        applied.append(f"Search={q}")
    if applied:
        story.append(Spacer(1, 2))
        story.append(Paragraph("Filters: " + ", ".join(applied), small_style))
    story.append(Spacer(1, 10))

    kpi = Table(
        [["Total Incoming", "Total Outgoing", "Net"], [pkr_format(incoming), pkr_format(outgoing), pkr_format(net)]],
        colWidths=[(A4[0] - 2.4 * cm) / 3.0] * 3,
    )
    kpi.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d6efd")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTSIZE", (0, 1), (-1, 1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
            ]
        )
    )
    story.append(kpi)
    story.append(Spacer(1, 10))

    chart_width = A4[0] - 2.4 * cm

    if chart_labels:
        bar_d = Drawing(chart_width, 170)
        bc = VerticalBarChart()
        bc.x = 0
        bc.y = 20
        bc.height = 140
        bc.width = chart_width
        bc.data = [chart_incoming, chart_outgoing]
        bc.categoryAxis.categoryNames = chart_labels
        bc.categoryAxis.labels.angle = 45
        bc.categoryAxis.labels.dy = -8
        bc.valueAxis.valueMin = 0
        bc.bars[0].fillColor = colors.HexColor("#198754")
        bc.bars[1].fillColor = colors.HexColor("#dc3545")
        bar_d.add(bc)
        story.append(Paragraph("Incoming vs Outgoing (last days)", styles["Heading3"]))
        story.append(bar_d)
        story.append(Spacer(1, 8))

        line_d = Drawing(chart_width, 170)
        lc = HorizontalLineChart()
        lc.x = 0
        lc.y = 20
        lc.height = 140
        lc.width = chart_width
        lc.data = [chart_cum_net]
        lc.categoryAxis.categoryNames = chart_labels
        lc.categoryAxis.labels.angle = 45
        lc.categoryAxis.labels.dy = -8
        lc.lines[0].strokeColor = colors.HexColor("#0d6efd")
        line_d.add(lc)
        story.append(Paragraph("Cash Flow (cumulative net)", styles["Heading3"]))
        story.append(line_d)
        story.append(Spacer(1, 8))

    if outgoing_by_cat:
        pie_d = Drawing(chart_width, 220)
        pie = Pie()
        pie.x = 40
        pie.y = 20
        pie.width = 160
        pie.height = 160
        top = sorted(outgoing_by_cat.items(), key=lambda kv: kv[1], reverse=True)[:6]
        other_sum = sum(v for _, v in sorted(outgoing_by_cat.items(), key=lambda kv: kv[1], reverse=True)[6:])
        labels_pie = [k for k, _ in top]
        values_pie = [v for _, v in top]
        if other_sum > 0:
            labels_pie.append("Other")
            values_pie.append(other_sum)
        pie.data = values_pie
        pie.labels = None

        palette = [
            colors.HexColor("#0d6efd"),
            colors.HexColor("#198754"),
            colors.HexColor("#dc3545"),
            colors.HexColor("#fd7e14"),
            colors.HexColor("#6f42c1"),
            colors.HexColor("#20c997"),
            colors.HexColor("#0dcaf0"),
        ]
        for i in range(len(values_pie)):
            pie.slices[i].fillColor = palette[i % len(palette)]

        pie_d.add(pie)

        legend = Legend()
        legend.x = 220
        legend.y = 170
        legend.alignment = "right"
        legend.colorNamePairs = [(pie.slices[i].fillColor, labels_pie[i]) for i in range(len(labels_pie))]
        pie_d.add(legend)
        story.append(Paragraph("Expense Breakdown", styles["Heading3"]))
        story.append(pie_d)
        story.append(Spacer(1, 10))

    data = [["Date", "Type", "Category", "Name", "Bill", "Amount (PKR)"]]
    for txx in items[:120]:
        data.append(
            [
                txx.date.isoformat(),
                txx.type,
                txx.category,
                (txx.name or "")[:20],
                (txx.bill_no or "")[:12],
                f"{int(txx.amount_pkr):,}",
            ]
        )

    table = Table(data, repeatRows=1, colWidths=[2.0 * cm, 2.0 * cm, 4.2 * cm, 3.0 * cm, 2.2 * cm, 3.0 * cm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d6efd")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#f1f5f9")]),
                ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
            ]
        )
    )

    story.append(Paragraph("Transactions (sample)", styles["Heading3"]))
    story.append(table)
    doc.build(story)

    buf.seek(0)
    filename = "nusrat-furniture-report.pdf"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return Response(buf.getvalue(), media_type="application/pdf", headers=headers)


@app.get("/analytics", response_class=HTMLResponse)
def analytics(
    request: Request,
    db: Session = Depends(get_db),
    from_date: str | None = None,
    to_date: str | None = None,
    type: str | None = None,
    category: str | None = None,
    name: str | None = None,
    q: str | None = None,
):
    f = parse_date(from_date)
    t = parse_date(to_date)
    f, t = clamp_date_range(f, t)

    items = crud.list_transactions(db, from_date=f, to_date=t, type=type, category=category, name=name, q=q, limit=10000)

    by_day: dict[str, dict[str, int]] = {}
    outgoing_by_cat: dict[str, int] = {}

    running_dates = sorted({tx.date for tx in items})

    for tx in items:
        k = tx.date.isoformat()
        by_day.setdefault(k, {"incoming": 0, "outgoing": 0})
        by_day[k][tx.type] += int(tx.amount_pkr)
        if tx.type == "outgoing":
            outgoing_by_cat[tx.category] = outgoing_by_cat.get(tx.category, 0) + int(tx.amount_pkr)

    labels = sorted(by_day.keys())
    incoming_series = [by_day[d]["incoming"] for d in labels]
    outgoing_series = [by_day[d]["outgoing"] for d in labels]

    net_series = []
    running = 0
    for inc, out in zip(incoming_series, outgoing_series):
        running += inc - out
        net_series.append(running)

    ctx = common_context(request)
    ctx.update(filter_context(db))
    ctx.update(
        {
            "filters": {
                "from_date": f.isoformat() if f else "",
                "to_date": t.isoformat() if t else "",
                "type": type or "",
                "category": category or "",
                "name": name or "",
                "q": q or "",
            },
            "chart": {
                "labels": labels,
                "incoming": incoming_series,
                "outgoing": outgoing_series,
                "outgoing_by_cat": outgoing_by_cat,
                "cumulative_net": net_series,
            },
        }
    )

    return TEMPLATES.TemplateResponse("analytics.html", ctx)
