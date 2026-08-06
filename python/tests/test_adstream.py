"""Iterating results as ClassAds.

The point of this path is that an attribute holding an expression stays an expression,
which a result cell cannot carry. Needs HTCondor's classad2 bindings; skipped without them.
"""

from __future__ import annotations

import pytest

import htcondordb

classad2 = pytest.importorskip("classad2")


@pytest.fixture
def machines(connection, table):
    """A few slot-shaped ads whose Requirements is an expression over siblings."""
    cursor = connection.cursor()
    for name, cpus, start in [("slot1", 4, "true"), ("slot2", 2, "false"), ("slot3", 16, "true")]:
        cursor.execute(
            f"INSERT INTO {table} (Key, Name, Cpus, Start, WithinResourceLimits, Requirements) "
            f"VALUES (?, ?, ?, {start}, true, Start && WithinResourceLimits)",
            (name, name, cpus),
        )
    return table


class TestIteration:
    def test_yields_classads(self, connection, machines):
        ads = list(connection.ads(f"SELECT * FROM {machines}"))
        assert len(ads) == 3
        assert all(isinstance(ad, classad2.ClassAd) for ad in ads)

    def test_expressions_stay_expressions(self, connection, machines):
        # The whole reason this API exists: a column select would answer True/False here.
        for ad in connection.ads(f"SELECT * FROM {machines}"):
            assert "Start" in str(ad["Requirements"])
            assert isinstance(ad.eval("Requirements"), bool)

    def test_evaluate_against_a_target(self, connection, machines):
        # Two-scope evaluation -- the machine as scope, a candidate job as TARGET. Not
        # expressible through the column API at all.
        job = classad2.ClassAd({"RequestCpus": 8})
        fits = classad2.ExprTree("Cpus >= TARGET.RequestCpus")
        matched = {
            ad["Name"] for ad in connection.ads(f"SELECT * FROM {machines}")
            if fits.eval(scope=ad, target=job)
        }
        assert matched == {"slot3"}

    def test_where_and_limit(self, connection, machines):
        assert len(list(connection.ads(f"SELECT * FROM {machines} WHERE Cpus > 3"))) == 2
        assert len(list(connection.ads(f"SELECT * FROM {machines} LIMIT 2"))) == 2

    def test_order_by(self, connection, machines):
        names = [ad["Name"] for ad in connection.ads(f"SELECT * FROM {machines} ORDER BY Cpus DESC")]
        assert names == ["slot3", "slot1", "slot2"]

    def test_empty_result(self, connection, machines):
        assert list(connection.ads(f"SELECT * FROM {machines} WHERE Cpus > 1000")) == []

    def test_projected_ads_still_evaluate(self, connection, machines):
        # A narrow select carries the siblings the projected expression reads.
        for ad in connection.ads(f"SELECT Name, Requirements FROM {machines} WHERE Name == 'slot1'"):
            assert ad.eval("Requirements") is True


class TestParameters:
    """Binding is the same code as Cursor.execute -- same placeholders, same quoting."""

    def test_qmark_binding(self, connection, machines):
        names = {ad["Name"] for ad in connection.ads(
            f"SELECT * FROM {machines} WHERE Cpus > ?", (3,))}
        assert names == {"slot1", "slot3"}

    def test_hostile_value_is_data(self, connection, machines):
        hostile = "'; DROP TABLE " + machines + " --"
        assert list(connection.ads(f"SELECT * FROM {machines} WHERE Name = ?", (hostile,))) == []
        # The table is still there.
        assert len(list(connection.ads(f"SELECT * FROM {machines}"))) == 3

    def test_wrong_parameter_count(self, connection, machines):
        with pytest.raises(htcondordb.ProgrammingError, match="placeholder"):
            list(connection.ads(f"SELECT * FROM {machines} WHERE Cpus > ? AND Name = ?", (3,)))


class TestAggregates:
    """An aggregate has no ads; one is synthesized per group row."""

    def test_group_rows_become_ads(self, connection, machines):
        ads = list(connection.ads(
            f"SELECT Start, COUNT(*), SUM(Cpus) AS total FROM {machines} GROUP BY Start"))
        assert len(ads) == 2
        by_start = {bool(ad["Start"]): ad for ad in ads}
        assert by_start[True]["Count"] == 2
        assert by_start[True]["total"] == 20
        assert by_start[False]["Count"] == 1

    def test_derived_attribute_names_are_legal(self, connection, machines):
        # COUNT(*) is not a legal attribute name; it folds to Count.
        (ad,) = list(connection.ads(f"SELECT COUNT(*) FROM {machines}"))
        assert ad["Count"] == 3


class TestLifetime:
    def test_close_stops_early_and_leaves_the_connection_usable(self, connection, machines):
        stream = connection.ads(f"SELECT * FROM {machines}")
        first = next(stream)
        assert first["Name"]
        stream.close()
        # The connection is not wedged by an abandoned stream.
        assert len(list(connection.ads(f"SELECT * FROM {machines}"))) == 3

    def test_close_is_idempotent(self, connection, machines):
        stream = connection.ads(f"SELECT * FROM {machines}")
        stream.close()
        stream.close()

    def test_context_manager(self, connection, machines):
        with connection.ads(f"SELECT * FROM {machines}") as stream:
            assert next(stream)["Name"]

    def test_exhausted_stream_stops(self, connection, machines):
        stream = connection.ads(f"SELECT * FROM {machines} LIMIT 1")
        assert next(stream)["Name"]
        with pytest.raises(StopIteration):
            next(stream)
        with pytest.raises(StopIteration):
            next(stream)

    def test_two_streams_in_sequence(self, connection, machines):
        # The connection serializes statements; a finished stream releases it.
        a = list(connection.ads(f"SELECT * FROM {machines}"))
        b = list(connection.ads(f"SELECT * FROM {machines}"))
        assert len(a) == len(b) == 3


class TestErrors:
    def test_non_select_is_refused(self, connection, table):
        with pytest.raises(htcondordb.DatabaseError):
            connection.ads(f"INSERT INTO {table} (Key) VALUES ('k')")

    def test_bad_sql_is_a_programming_error(self, connection):
        with pytest.raises(htcondordb.ProgrammingError):
            connection.ads("NOT SQL AT ALL")

    def test_closed_connection(self, daemon_address, table):
        conn = htcondordb.connect(daemon_address)
        conn.close()
        with pytest.raises(htcondordb.InterfaceError):
            conn.ads("SELECT * FROM machines")
