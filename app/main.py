from __future__ import annotations

import datetime as dt
import io
import os
import base64

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
from .models import Employee, Transaction, WeeklyAssignment
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
        Base.metadata.create_all(bind=engine)

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
    items = crud.list_employees(db, status=status)
    ctx = common_context(request)
    ctx.update({"items": items, "status": status or ""})
    return TEMPLATES.TemplateResponse("employees.html", ctx)


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

    profile_data = None
    if profile_image is not None:
        raw = profile_image.file.read()
        if raw and len(raw) > MAX_IMAGE_UPLOAD_BYTES:
            errors["profile_image_url"] = f"Profile image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw:
            b64 = base64.b64encode(raw).decode("ascii")
            profile_data = f"data:{profile_image.content_type or 'application/octet-stream'};base64,{b64}"

    cnic_data = None
    if cnic_image is not None:
        raw2 = cnic_image.file.read()
        if raw2 and len(raw2) > MAX_IMAGE_UPLOAD_BYTES:
            errors["cnic_image"] = f"CNIC image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw2:
            b64_2 = base64.b64encode(raw2).decode("ascii")
            cnic_data = f"data:{cnic_image.content_type or 'application/octet-stream'};base64,{b64_2}"

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
        profile_image_url=(profile_image_url or "").strip(),
    )
    if profile_data or cnic_data:
        emp.profile_image_data = profile_data
        emp.cnic_image_data = cnic_data
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
            b64 = base64.b64encode(raw).decode("ascii")
            emp.profile_image_data = f"data:{profile_image.content_type or 'application/octet-stream'};base64,{b64}"
    if cnic_image is not None:
        raw2 = cnic_image.file.read()
        if raw2 and len(raw2) > MAX_IMAGE_UPLOAD_BYTES:
            errors["cnic_image"] = f"CNIC image is too large. Max {MAX_IMAGE_UPLOAD_BYTES // 1000}KB."
        elif raw2:
            b64_2 = base64.b64encode(raw2).decode("ascii")
            emp.cnic_image_data = f"data:{cnic_image.content_type or 'application/octet-stream'};base64,{b64_2}"

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
    employee_id: int | None = Form(None),
    employee_tx_type: str | None = Form(None),
    payment_method: str | None = Form(None),
    reference: str | None = Form(None),
):
    parsed_date = parse_date(date)
    if not parsed_date:
        parsed_date = dt.date.today()

    errors = validate_form(type, category, bill_no, amount_pkr)
    if type == "outgoing" and employee_id:
        emp = crud.get_employee(db, employee_id)
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
                    "employee_id": employee_id,
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
        employee_id=employee_id if type == "outgoing" else None,
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
    employee_id: int | None = Form(None),
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

    errors = validate_form(tx.type, category, bill_no, int(amount_pkr))
    if tx.type == "outgoing" and employee_id:
        emp = crud.get_employee(db, employee_id)
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
                    "employee_id": employee_id,
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
        employee_id=employee_id if tx.type == "outgoing" else None,
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


def to_dataframe(items: list[Transaction]) -> pd.DataFrame:
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
