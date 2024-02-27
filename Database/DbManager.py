import sqlalchemy
from pathlib import Path

from sqlalchemy import MetaData, Table, Column, Integer, String, select, insert, update
from sqlalchemy.orm import sessionmaker

from Database.DatabaseModel import Base, State


class DbManager:
    def __init__(self, state_db_path: Path):
        self.state_table: Table | None = None
        self.state_db_path = state_db_path
        self.db_engine = sqlalchemy.create_engine(f'sqlite:///{state_db_path}')

        self.create_tables_if_not_exist()
        self.session_maker = sessionmaker(bind=self.db_engine)

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
