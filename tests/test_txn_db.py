import os
import tempfile

import pytest

from src.txn_db import TransactionalKVStore, TransactionError


def test_basic_commit_and_read(tmp_path):
    wal = tmp_path / "wal.log"
    store = TransactionalKVStore(wal_path=str(wal))

    txn = store.begin()
    txn.set("a", 1)
    txn.commit()

    txn2 = store.begin()
    assert txn2.get("a") == 1


def test_snapshot_isolation(tmp_path):
    wal = tmp_path / "wal.log"
    store = TransactionalKVStore(wal_path=str(wal))

    txn1 = store.begin()
    txn1.set("x", "first")

    txn2 = store.begin()
    assert txn2.get("x") is None

    txn1.commit()
    assert txn2.get("x") is None  # still sees old snapshot

    txn2.commit()

    txn3 = store.begin()
    assert txn3.get("x") == "first"


def test_conflict_detection(tmp_path):
    wal = tmp_path / "wal.log"
    store = TransactionalKVStore(wal_path=str(wal))

    txn1 = store.begin()
    txn1.set("k", 10)
    txn1.commit()

    txn2 = store.begin()
    txn3 = store.begin()

    txn2.set("k", 20)
    txn3.set("k", 30)

    txn2.commit()

    with pytest.raises(TransactionError):
        txn3.commit()


def test_recovery_from_wal(tmp_path):
    wal = tmp_path / "wal.log"
    store = TransactionalKVStore(wal_path=str(wal))

    txn = store.begin()
    txn.set("z", 99)
    txn.commit()

    # Create a new instance, which should recover state
    store2 = TransactionalKVStore(wal_path=str(wal))
    txn2 = store2.begin()
    assert txn2.get("z") == 99
