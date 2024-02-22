import sqlite3
from pathlib import Path


class DbManager:
    def __init__(self, state_db_path: Path):
        self.state_db_path = state_db_path
        self.connection = sqlite3.connect(self.state_db_path, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.connection.execute('pragma journal_mode=wal')

        self.create_tables_if_not_exist()

    def execute_write_query(self, query: str):
        return self.cursor.execute(query)

    def execute_read_query_fetchall(self, query: str) -> list:
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_read_query_fetchone(self, query: str) -> object:
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def set_state_variable(self, variable_name: str, value: str):
        if self.get_state_variable(variable_name) is None:
            return self.execute_write_query(f'INSERT INTO state (param, value) VALUES ("{variable_name}", "{value}");')
        else:
            return self.execute_write_query(f'UPDATE state SET value = "{value}" WHERE param = "{variable_name}";')

    def get_state_variable(self, variable_name: str) -> object:
        result = self.execute_read_query_fetchone(f'SELECT value FROM state WHERE param = "{variable_name}";')
        if result is None:
            return None
        return result[0]

    def create_tables_if_not_exist(self):
        tables = self.execute_read_query_fetchall("SELECT name FROM sqlite_master WHERE type='table';")
        if ('state',) not in tables:
            self.execute_write_query('CREATE TABLE state (param TEXT PRIMARY KEY, value TEXT);')
