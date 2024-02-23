import os
import sqlite3
from pathlib import Path

from Database.DbManager import DbManager


def test_get_state_variable():
    manager = DbManager(state_db_path=Path('test.db'))
    manager.set_state_variable(variable_name='feedproxy_page', value='1')

    result_page = manager.get_state_variable('feedproxy_page')

    assert result_page == '1'

    os.unlink(Path('test.db'))


def test_set_state_variable():
    manager = DbManager(state_db_path=Path('test.db'))
    manager.set_state_variable(variable_name='feedproxy_page', value='2')
    result_page = manager.get_state_variable('feedproxy_page')
    assert result_page == '2'
    manager.set_state_variable(variable_name='feedproxy_page', value='3')
    result_page = manager.get_state_variable('feedproxy_page')
    assert result_page == '3'

    os.unlink(Path('test.db'))
