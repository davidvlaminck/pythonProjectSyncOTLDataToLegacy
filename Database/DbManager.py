from pathlib import Path
from uuid import UUID

import sqlalchemy
from sqlalchemy import Table, select, update, delete
from sqlalchemy.orm import sessionmaker

from Database.DatabaseModel import Base, State, Delivery


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



