from __future__ import annotations

import datetime as dt

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from .db import Base


class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, index=True)

    full_name = Column(String(128), nullable=False, index=True)
    father_name = Column(String(128), nullable=True)
    cnic = Column(String(32), nullable=True)
    mobile_number = Column(String(32), nullable=True)
    address = Column(Text, nullable=True)
    emergency_contact = Column(String(64), nullable=True)

    joining_date = Column(Date, nullable=False, index=True)
    status = Column(String(16), nullable=False, default="active", index=True)

    category = Column(String(64), nullable=False, index=True)
    work_type = Column(String(32), nullable=False)
    role_description = Column(String(256), nullable=True)
    payment_rate = Column(Integer, nullable=True)

    profile_image_url = Column(String(512), nullable=True)
    profile_image_data = Column(Text, nullable=True)
    cnic_image_data = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class WeeklyAssignment(Base):
    __tablename__ = "weekly_assignments"

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, nullable=False, index=True)

    week_start = Column(Date, nullable=False, index=True)
    week_end = Column(Date, nullable=False, index=True)

    description = Column(Text, nullable=False)
    quantity = Column(Integer, nullable=True)
    status = Column(String(16), nullable=False, default="pending", index=True)
    is_locked = Column(Boolean, nullable=False, default=False, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)

    type = Column(String(16), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    amount_pkr = Column(Integer, nullable=False)

    category = Column(String(64), nullable=False, index=True)
    name = Column(String(128), nullable=True, index=True)
    bill_no = Column(String(64), nullable=True, index=True)
    notes = Column(Text, nullable=True)

    employee_id = Column(Integer, nullable=True, index=True)
    employee_tx_type = Column(String(32), nullable=True, index=True)
    payment_method = Column(String(32), nullable=True, index=True)
    assignment_id = Column(Integer, nullable=True, index=True)
    reference = Column(String(256), nullable=True)

    is_deleted = Column(Boolean, nullable=False, default=False, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<Transaction id={self.id} type={self.type} date={self.date} amount_pkr={self.amount_pkr}>"
