"""Transactions, end to end against a live daemon.

Two constraints shape every test here, and both come from the protocol rather than the
driver: a ``SELECT`` reads committed state (queries carry no transaction id), and a
transaction is scoped to one table.
"""

from __future__ import annotations

import pytest

import htcondordb


def rowcount(connection, table) -> int:
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0]


class TestAutocommitMode:
    def test_default_is_autocommit(self, connection):
        assert connection.autocommit is True
        assert connection.in_transaction is False

    def test_commit_is_a_noop(self, connection):
        connection.commit()  # PEP 249: must work even with no transaction open

    def test_rollback_explains_how_to_enable_transactions(self, connection):
        with pytest.raises(htcondordb.NotSupportedError) as excinfo:
            connection.rollback()
        assert "autocommit = False" in str(excinfo.value)

    def test_writes_land_immediately(self, connection, table):
        connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')").close()
        assert rowcount(connection, table) == 1


class TestExplicitTransactions:
    def test_commit_applies(self, connection, table):
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j2', 'bob')")
        connection.commit()
        assert rowcount(connection, table) == 2

    def test_rollback_discards(self, connection, table):
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
        connection.rollback()
        assert rowcount(connection, table) == 0

    def test_reads_see_committed_state_only(self, connection, table):
        # The transaction's own uncommitted writes are invisible to a query. Pinned so a
        # future protocol change that makes reads transaction-aware shows up here.
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
        assert rowcount(connection, table) == 0
        connection.commit()
        assert rowcount(connection, table) == 1

    def test_a_new_transaction_opens_after_commit(self, connection, table):
        connection.autocommit = False
        assert connection.in_transaction
        connection.commit()
        assert connection.in_transaction  # still in non-autocommit mode

        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
        connection.rollback()
        assert rowcount(connection, table) == 0

    def test_returning_to_autocommit_commits_pending_work(self, connection, table):
        connection.autocommit = False
        connection.cursor().execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'a')")
        connection.autocommit = True
        assert connection.in_transaction is False
        assert rowcount(connection, table) == 1

    def test_delete_is_rolled_back(self, connection, table):
        # DELETE has a fast server-side bulk path that commits on its own; inside a
        # transaction it must take the staged path instead, or a rollback would silently
        # fail to restore the rows.
        connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')").close()
        connection.autocommit = False
        connection.cursor().execute(f"DELETE FROM {table} WHERE Key = ?", ("j1",))
        connection.rollback()
        assert rowcount(connection, table) == 1

    def test_update_is_rolled_back(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, JobStatus) VALUES ('j1', 1)"
        ).close()
        connection.autocommit = False
        connection.cursor().execute(f"UPDATE {table} SET JobStatus = 5 WHERE Key = ?", ("j1",))
        connection.rollback()
        cursor = connection.cursor()
        cursor.execute(f"SELECT JobStatus FROM {table} WHERE Key = ?", ("j1",))
        assert cursor.fetchone() == (1,)

    def test_executemany_is_atomic_in_a_transaction(self, connection, table):
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.executemany(
            f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)",
            [("j1", "alice"), ("j2", "bob"), ("j3", "carol")],
        )
        assert cursor.rowcount == 3
        assert rowcount(connection, table) == 0  # nothing applied yet
        connection.rollback()
        assert rowcount(connection, table) == 0


class TestTransactionContextManager:
    def test_commits_on_success(self, connection, table):
        with connection.transaction():
            connection.cursor().execute(
                f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')"
            )
        assert rowcount(connection, table) == 1
        assert connection.in_transaction is False

    def test_rolls_back_on_exception(self, connection, table):
        class Boom(Exception):
            pass

        with pytest.raises(Boom):
            with connection.transaction():
                connection.cursor().execute(
                    f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')"
                )
                raise Boom
        assert rowcount(connection, table) == 0
        assert connection.in_transaction is False

    def test_rolls_back_on_a_failed_statement(self, connection, table):
        with pytest.raises(htcondordb.ProgrammingError):
            with connection.transaction():
                connection.cursor().execute(
                    f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')"
                )
                connection.cursor().execute("NOT SQL AT ALL")
        assert rowcount(connection, table) == 0

    def test_restores_the_previous_autocommit_setting(self, connection, table):
        assert connection.autocommit is True
        with connection.transaction():
            assert connection.autocommit is False
        assert connection.autocommit is True

    def test_nesting_is_refused(self, connection):
        with connection.transaction():
            with pytest.raises(htcondordb.ProgrammingError, match="already open"):
                with connection.transaction():
                    pass


class TestTransactionLimits:
    def test_cannot_span_tables(self, connection, table):
        other = table + "_b"
        connection.execute(f"CREATE TABLE {other}").close()
        try:
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
            with pytest.raises(htcondordb.DatabaseError) as excinfo:
                cursor.execute(f"INSERT INTO {other} (Key, Owner) VALUES ('j2', 'bob')")
            message = str(excinfo.value)
            assert "cannot span tables" in message
            assert table in message and other in message

            # The transaction survives the refusal and still works for its own table.
            assert connection.in_transaction
            cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j3', 'carol')")
            connection.commit()
            assert rowcount(connection, table) == 2
        finally:
            connection.autocommit = True
            connection.execute(f"DROP TABLE {other}").close()

    def test_nested_begin_is_refused(self, connection):
        connection.autocommit = False
        with pytest.raises(htcondordb.DatabaseError, match="already open"):
            connection.cursor().execute("BEGIN")


class TestTransactionOnClose:
    def test_close_rolls_back(self, daemon_address, table, connection):
        # `table` is created on `connection`; a second connection writes into it and then
        # closes without committing.
        other = htcondordb.connect(daemon_address, autocommit=False)
        other.cursor().execute(f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')")
        other.close()
        assert rowcount(connection, table) == 0

    def test_connect_with_autocommit_false(self, daemon_address):
        conn = htcondordb.connect(daemon_address, autocommit=False)
        try:
            assert conn.autocommit is False
            assert conn.in_transaction is True
        finally:
            conn.close()
