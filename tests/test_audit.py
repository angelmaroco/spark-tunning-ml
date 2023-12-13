from __future__ import annotations

import os

import pytest

from spark_tunning_ml.audit import Audit


def test_add_app_id():
    audit = Audit(db_name='test_database.db')
    audit.add_app_id('app_001', 1, 2, 3, 4, 5, 6)
    df = audit.get_audit_data()
    assert not df.empty
    assert df['app_id'].iloc[0] == 'app_001'


def test_delete_app_id():
    audit = Audit(db_name='test_database.db')
    audit.add_app_id('app_002', 1, 2, 3, 4, 5, 6)
    audit.delete_app_id('app_002')
    df = audit.get_audit_data()
    assert df.empty


def test_query_app_id():
    audit = Audit(db_name='test_database.db')
    audit.add_app_id('app_003', 1, 2, 3, 4, 5, 6)
    assert audit.query_app_id('app_003')
    assert not audit.query_app_id('nonexistent_app')


# Clean up the test database after tests
def pytest_sessionfinish(session, exitstatus):
    os.remove('test_database.db')
