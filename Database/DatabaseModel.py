import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


#


class Base(DeclarativeBase):
    pass


class State(Base):
    __tablename__ = "state"

    param: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str]


class Delivery(Base):
    __tablename__ = "deliveries"
    uuid_em_infra: Mapped[UUID] = mapped_column(primary_key=True, index=True)
    uuid_davie: Mapped[Optional[UUID]] = mapped_column(index=True)
    referentie: Mapped[Optional[str]] = mapped_column(index=True)
    status: Mapped[Optional[str]]


class DeliveryAsset(Base):
    __tablename__ = "deliveries_assets"
    uuid_delivery_em_infra: Mapped[UUID] = mapped_column(index=True)
    uuid_asset: Mapped[UUID] = mapped_column(index=True)
    last_updated: Mapped[datetime.datetime]

    __table_args__ = (
        PrimaryKeyConstraint("uuid_delivery_em_infra", "uuid_asset"),
    )


class Asset(Base):
    __tablename__ = "assets"
    uuid: Mapped[UUID] = mapped_column(primary_key=True, index=True)
    naampad: Mapped[str]
    installatie: Mapped[str] = mapped_column(index=True)
