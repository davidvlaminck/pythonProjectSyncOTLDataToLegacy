import datetime
from pathlib import Path
from typing import Generator, Iterator
from uuid import UUID

import sqlalchemy
from sqlalchemy import Table, select, update, delete
from sqlalchemy.orm import sessionmaker, joinedload

from Database.DatabaseModel import Base, State, Delivery, DeliveryAsset, Asset
from Domain.Enums import AanleveringStatus


class DbManager:
    def __init__(self, state_db_path: Path):
        self.state_table: Table | None = None
        self.state_db_path = state_db_path
        self.db_engine = sqlalchemy.create_engine(f'sqlite:///{state_db_path}')

        self.create_tables_if_not_exist()
        self.session_maker = sessionmaker(bind=self.db_engine, expire_on_commit=False)

    def set_state_variable(self, variable_name: str, value: str):
        with self.session_maker.begin() as session:
            result = session.scalar(select(State).filter_by(param=variable_name))

            if result is None:
                state = State(param=variable_name, value=value)
                session.add(state)
            else:
                result.value = value

            session.commit()

    def get_state_variable(self, variable_name: str) -> object:
        with self.session_maker.begin() as session:
            query = session.query(State.value).filter(State.param == variable_name)
            return query.scalar()

    def create_tables_if_not_exist(self):
        Base.metadata.create_all(self.db_engine)

    def get_a_delivery_uuid_without_reference(self) -> UUID:
        with self.session_maker.begin() as session:
            query = session.query(Delivery.uuid_em_infra).filter(Delivery.referentie.is_(None)).limit(1)
            return query.scalar()

    def get_a_delivery_by_em_infra_uuid(self, em_infra_uuid) -> UUID:
        em_infra_guid = UUID(em_infra_uuid)
        with self.session_maker.begin() as session:
            query = session.query(Delivery.uuid_em_infra).filter(Delivery.uuid_em_infra == em_infra_guid)
            return query.scalar()

    def get_a_delivery_without_davie_uuid(self) -> Delivery:
        with self.session_maker.begin() as session:
            query = session.query(Delivery).filter(Delivery.uuid_davie.is_(None)).limit(1)
            return query.scalar()

    def update_delivery_description(self, em_infra_uuid: UUID, description: str):
        with self.session_maker.begin() as session:
            query = update(Delivery).where(Delivery.uuid_em_infra == em_infra_uuid).values(referentie=description)
            session.execute(query)
            session.commit()

    def update_delivery_davie_uuid(self, em_infra_uuid: UUID, davie_uuid: str):
        with self.session_maker.begin() as session:
            query = update(Delivery).where(Delivery.uuid_em_infra == em_infra_uuid).values(uuid_davie=UUID(davie_uuid))
            session.execute(query)
            session.commit()

    def delete_delivery_by_uuid(self, em_infra_uuid: str):
        with self.session_maker.begin() as session:
            query = delete(Delivery).where(Delivery.uuid_em_infra == em_infra_uuid)
            session.execute(query)
            session.commit()

    def update_delivery_status(self, davie_uuid: str, status: AanleveringStatus):
        with self.session_maker.begin() as session:
            query = update(Delivery).where(Delivery.uuid_davie == UUID(davie_uuid)).values(status=status.value)
            session.execute(query)
            session.commit()

    def add_delivery(self, em_infra_uuid: str):
        with self.session_maker.begin() as session:
            session.add(Delivery(uuid_em_infra=UUID(em_infra_uuid)))
            session.commit()

    def upsert_assets_delivery(self, delivery_em_infra_uuid: str, asset_timestamp_dict: {str: datetime.datetime}):
        with self.session_maker.begin() as session:
            for asset_uuid, timestamp in asset_timestamp_dict.items():
                session.merge(Asset(uuid=UUID(asset_uuid)))
                session.merge(DeliveryAsset(
                    uuid_delivery_em_infra=UUID(delivery_em_infra_uuid),
                    uuid_asset=UUID(asset_uuid),
                    last_updated=timestamp
                ))
            session.commit()

    def get_asset_uuids_from_specific_deliveries(self, delivery_references: [str]) -> Iterator[str]:
        with self.session_maker.begin() as session:
            for d in delivery_references:
                # filter such that the delivery reference starts with the given string
                yield from [str(s) for s in session.scalars(
                    select(DeliveryAsset.uuid_asset).
                    join(DeliveryAsset.delivery).
                    where(Delivery.referentie.startswith(d))).all()]

    def get_asset_uuids_from_final_deliveries(self):
        with self.session_maker.begin() as session:
            return [str(s) for s in session.scalars(select(DeliveryAsset.uuid_asset)).all()]  # TODO add filter

    def get_deliveries_by_asset_uuid(self, asset_uuid: str) -> [Delivery]:
        with self.session_maker.begin() as session:
            query = session.query(DeliveryAsset).filter(DeliveryAsset.uuid_asset == UUID(asset_uuid))
            return [a.delivery for a in query.all()]

    def clear_delivery(self, context_string):
        with self.session_maker.begin() as session:
            delivery = session.query(Delivery).filter(Delivery.referentie.startswith(context_string)).first()
            if delivery is None:
                return
            delivery_assets = delivery.deliveries_assets
            for delivery_asset in delivery_assets:
                asset = session.query(Asset).filter(Asset.uuid == delivery_asset.uuid_asset).first()
                if asset is not None:
                    session.delete(asset)
                session.delete(delivery_asset)
            session.delete(delivery)
            session.commit()