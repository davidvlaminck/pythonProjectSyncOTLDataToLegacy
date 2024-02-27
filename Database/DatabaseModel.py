from typing import Optional
from uuid import UUID

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

#
# class DeliveryAsset(Base):
#     __tablename__ = "deliveries_assets"
#     uuid_em_infra: Mapped[UUID] = mapped_column(index=True)
#     uuid_asset: Mapped[UUID] = mapped_column(index=True)
