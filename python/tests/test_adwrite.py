"""Writing whole ClassAds, in batches, from an iterator."""

from __future__ import annotations

import pytest

import htcondordb

classad2 = pytest.importorskip("classad2")


def slot(name, cpus=4, **extra):
    ad = {"Name": name, "Cpus": cpus, "Requirements": classad2.ExprTree("Cpus > 2")}
    ad.update(extra)
    return classad2.ClassAd(ad)


def rowcount(connection, table):
    cur = connection.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


class TestWriteAds:
    def test_writes_and_keys_by_name(self, connection, table):
        res = connection.write_ads(table, [slot("a"), slot("b")])
        assert res.written == 2 and not res.rejects and bool(res)
        assert rowcount(connection, table) == 2

    def test_consumes_an_iterator_lazily(self, connection, table):
        # A generator, never a list: the loader must not materialize its input.
        consumed = []

        def gen():
            for i in range(30):
                consumed.append(i)
                yield slot(f"s{i}")

        res = connection.write_ads(table, gen(), chunk=10)
        assert res.written == 30
        assert len(consumed) == 30
        assert rowcount(connection, table) == 30

    def test_spans_several_chunks(self, connection, table):
        res = connection.write_ads(table, (slot(f"s{i}") for i in range(25)), chunk=10)
        assert res.written == 25
        assert rowcount(connection, table) == 25

    def test_upsert_replaces(self, connection, table):
        connection.write_ads(table, [slot("a", cpus=4, Extra="keep")])
        connection.write_ads(table, [slot("a", cpus=64)])
        assert rowcount(connection, table) == 1
        (ad,) = list(connection.ads(f"SELECT * FROM {table}"))
        assert ad.eval("Cpus") == 64
        # Replace, not merge: the omitted attribute is gone.
        assert "Extra" not in ad

    def test_expressions_survive(self, connection, table):
        connection.write_ads(table, [slot("a", cpus=8)])
        (ad,) = list(connection.ads(f"SELECT * FROM {table}"))
        assert "Cpus" in str(ad["Requirements"])
        assert ad.eval("Requirements") is True

    def test_read_modify_write(self, connection, table):
        connection.write_ads(table, [slot("a", cpus=8), slot("b", cpus=2)])
        updated = []
        for ad in connection.ads(f"SELECT * FROM {table} WHERE Cpus > 4"):
            ad["Checked"] = True
            updated.append(ad)
        res = connection.write_ads(table, updated)
        assert res.written == 1
        cur = connection.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE Checked")
        assert cur.fetchone()[0] == 1

    def test_custom_key_attribute(self, connection, table):
        connection.write_ads(table, [classad2.ClassAd({"Slot": "x", "Cpus": 1})], key="Slot")
        assert rowcount(connection, table) == 1

    def test_callable_key(self, connection, table):
        connection.write_ads(
            table, [slot("a"), slot("b")], key=lambda ad: f"custom-{ad['Name']}"
        )
        assert rowcount(connection, table) == 2

    def test_missing_key_attribute_is_a_clear_error(self, connection, table):
        with pytest.raises(htcondordb.ProgrammingError, match="Name"):
            connection.write_ads(table, [classad2.ClassAd({"Cpus": 1})])

    def test_dicts_are_accepted(self, connection, table):
        res = connection.write_ads(table, [{"Name": "a", "Cpus": 1}])
        assert res.written == 1

    def test_empty_input(self, connection, table):
        res = connection.write_ads(table, [])
        assert res.written == 0 and bool(res)


class TestRejects:
    def test_a_bad_ad_does_not_lose_the_batch(self, connection, table):
        res = connection.write_ads(table, [
            slot("good1"),
            classad2.ClassAd({"Name": "bad", "Note": "line1\nline2"}),
            slot("good2"),
        ])
        assert res.written == 2
        assert [r.index for r in res.rejects] == [1]
        assert "newline" in res.rejects[0].reason
        assert not res, "a result with rejects must be falsy"
        assert rowcount(connection, table) == 2

    def test_reject_indices_are_the_callers_across_chunks(self, connection, table):
        ads = [slot(f"s{i}") for i in range(10)]
        ads[7] = classad2.ClassAd({"Name": "bad", "Note": "a\nb"})
        res = connection.write_ads(table, ads, chunk=3)
        assert [r.index for r in res.rejects] == [7], "index must be the position in the input"
        assert res.written == 9


class TestTransactions:
    def test_stages_into_an_open_transaction(self, connection, table):
        connection.autocommit = False
        connection.write_ads(table, [slot("a")])
        assert rowcount(connection, table) == 1  # visible to the transaction
        connection.rollback()
        assert rowcount(connection, table) == 0  # and to nobody else

    def test_committing_makes_it_durable(self, connection, table):
        connection.autocommit = False
        connection.write_ads(table, [slot("a"), slot("b")])
        connection.commit()
        connection.autocommit = True
        assert rowcount(connection, table) == 2
