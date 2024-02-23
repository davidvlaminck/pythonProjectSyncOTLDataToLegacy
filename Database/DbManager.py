import sqlalchemy
from pathlib import Path

from sqlalchemy import MetaData, Table, Column, Integer, String, select, insert, update


class DbManager:
    def __init__(self, state_db_path: Path):
        self.state_table: Table | None = None
        self.state_db_path = state_db_path
        self.db_engine = sqlalchemy.create_engine(f'sqlite:///{state_db_path}')

        self.create_tables_if_not_exist()

    def set_state_variable(self, variable_name: str, value: str):
        if self.get_state_variable(variable_name) is None:
            stmt = insert(self.state_table).values(param=variable_name, value=value)
        else:
            stmt = (
                update(self.state_table)
                .where(self.state_table.c.param == variable_name)
                .values(value=value))
        with self.db_engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()

    def get_state_variable(self, variable_name: str) -> object:
        stmt = (select(self.state_table.c.value).
                where(self.state_table.c.param == variable_name))

        with self.db_engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
            connection.commit()

        return None if result is None else result[0]

    def create_tables_if_not_exist(self):
        metadata = MetaData()

        self.state_table = Table(
            'state',
            metadata,
            Column('param', String, primary_key=True, nullable=False),
            Column('value', String, nullable=False))

        # Create the table if it doesn't exist
        metadata.create_all(bind=self.db_engine)
