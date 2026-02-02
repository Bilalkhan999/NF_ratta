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
    cnic_image_url = Column(String(512), nullable=True)
    profile_image_data = Column(Text, nullable=True)
    cnic_image_data = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class SofaItem(Base):
    __tablename__ = "sofa_items"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(256), nullable=False, index=True)
    sofa_type = Column(String(128), nullable=False, index=True)
    hardware_material = Column(String(128), nullable=True, index=True)
    poshish_material = Column(String(128), nullable=True, index=True)
    seating_capacity = Column(String(64), nullable=True)

    qty_on_hand = Column(Integer, nullable=False, default=0)
    reorder_level = Column(Integer, nullable=False, default=0)
    cost_price_pkr = Column(Integer, nullable=False, default=0)
    sale_price_pkr = Column(Integer, nullable=False, default=0)

    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class HardwareMaterial(Base):
    __tablename__ = "hardware_materials"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(256), nullable=False, index=True)
    unit = Column(String(32), nullable=False, default="pieces")

    qty_on_hand = Column(Integer, nullable=False, default=0)
    reorder_level = Column(Integer, nullable=False, default=0)
    cost_price_pkr = Column(Integer, nullable=False, default=0)
    sale_price_pkr = Column(Integer, nullable=False, default=0)

    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class PoshishMaterial(Base):
    __tablename__ = "poshish_materials"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(256), nullable=False, index=True)
    color = Column(String(128), nullable=True)
    unit = Column(String(32), nullable=False, default="meters")

    qty_on_hand = Column(Integer, nullable=False, default=0)
    reorder_level = Column(Integer, nullable=False, default=0)
    cost_price_pkr = Column(Integer, nullable=False, default=0)
    sale_price_pkr = Column(Integer, nullable=False, default=0)

    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

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


class InventoryCategory(Base):
    __tablename__ = "inventory_categories"

    id = Column(Integer, primary_key=True, index=True)

    type = Column(String(16), nullable=False, index=True)
    name = Column(String(128), nullable=False, index=True)
    parent_id = Column(Integer, nullable=True, index=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class BedSize(Base):
    __tablename__ = "bed_sizes"

    id = Column(Integer, primary_key=True, index=True)

    label = Column(String(128), nullable=False)
    width_in = Column(Integer, nullable=False)
    length_in = Column(Integer, nullable=False)
    width_ft_x100 = Column(Integer, nullable=True)
    length_ft_x100 = Column(Integer, nullable=True)
    sort_order = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FurnitureItem(Base):
    __tablename__ = "furniture_items"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(256), nullable=False, index=True)
    sku = Column(String(64), nullable=False, index=True)
    material_type = Column(String(32), nullable=False, default="Wood", index=True)
    color_finish = Column(String(128), nullable=True)
    status = Column(String(32), nullable=False, default="IN_STOCK", index=True)

    category_id = Column(Integer, nullable=False, index=True)
    sub_category_id = Column(Integer, nullable=True, index=True)

    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FurnitureVariant(Base):
    __tablename__ = "furniture_variants"

    id = Column(Integer, primary_key=True, index=True)

    furniture_item_id = Column(Integer, nullable=False, index=True)
    bed_size_id = Column(Integer, nullable=True, index=True)

    qty_on_hand = Column(Integer, nullable=False, default=0)
    reorder_level = Column(Integer, nullable=False, default=0)
    cost_price_pkr = Column(Integer, nullable=False, default=0)
    sale_price_pkr = Column(Integer, nullable=False, default=0)

    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FoamBrand(Base):
    __tablename__ = "foam_brands"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(128), nullable=False, index=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FoamModel(Base):
    __tablename__ = "foam_models"

    id = Column(Integer, primary_key=True, index=True)

    brand_id = Column(Integer, nullable=False, index=True)
    name = Column(String(128), nullable=False, index=True)
    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FoamThickness(Base):
    __tablename__ = "foam_thicknesses"

    id = Column(Integer, primary_key=True, index=True)

    inches = Column(Integer, nullable=False, index=True)
    sort_order = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class FoamVariant(Base):
    __tablename__ = "foam_variants"

    id = Column(Integer, primary_key=True, index=True)

    foam_model_id = Column(Integer, nullable=False, index=True)
    bed_size_id = Column(Integer, nullable=False, index=True)
    thickness_id = Column(Integer, nullable=False, index=True)

    density_type = Column(String(64), nullable=True)

    qty_on_hand = Column(Integer, nullable=False, default=0)
    reorder_level = Column(Integer, nullable=False, default=0)
    purchase_cost_pkr = Column(Integer, nullable=False, default=0)
    sale_price_pkr = Column(Integer, nullable=False, default=0)

    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class StockMovement(Base):
    __tablename__ = "stock_movements"

    id = Column(Integer, primary_key=True, index=True)

    inventory_type = Column(String(32), nullable=False, index=True)
    variant_id = Column(Integer, nullable=False, index=True)
    movement_type = Column(String(32), nullable=False, index=True)
    qty_change = Column(Integer, nullable=False)
    unit_cost_pkr = Column(Integer, nullable=True)

    reference_type = Column(String(64), nullable=True)
    reference_id = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
