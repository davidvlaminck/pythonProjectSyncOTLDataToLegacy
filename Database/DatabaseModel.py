import datetime
from typing import Optional, List
from uuid import UUID

from sqlalchemy import PrimaryKeyConstraint, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship
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
    final: Mapped[Optional[bool]]
    deliveries_assets: Mapped[List["DeliveryAsset"]] = relationship(back_populates="delivery")


class DeliveryAsset(Base):
    __tablename__ = "deliveries_assets"
    uuid_delivery_em_infra: Mapped[UUID] = mapped_column(ForeignKey("deliveries.uuid_em_infra"), index=True)
    uuid_asset: Mapped[UUID] = mapped_column(ForeignKey("assets.uuid"), index=True)
    last_updated: Mapped[datetime.datetime]
    delivery: Mapped["Delivery"] = relationship(back_populates="deliveries_assets")

    __table_args__ = (
        PrimaryKeyConstraint("uuid_delivery_em_infra", "uuid_asset"),
    )


class Asset(Base):
    __tablename__ = "assets"
    uuid: Mapped[UUID] = mapped_column(primary_key=True, index=True, nullable=False)
    naampad: Mapped[Optional[str]]
    installatie: Mapped[Optional[str]] = mapped_column(index=True)
