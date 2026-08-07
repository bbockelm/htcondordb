"""Expression-valued parameters.

Every other parameter binds as data. These bind as ClassAd code, which is the one place a
parameter is not automatically safe -- so the tests pin both that it works and that it stays
opt-in.
"""

from __future__ import annotations

import pytest

import htcondordb
from htcondordb import Expr
from htcondordb._params import bind, render_literal


class TestExprMarker:
    def test_renders_verbatim(self):
        assert render_literal(Expr("Start && WithinResourceLimits")) == "Start && WithinResourceLimits"

    def test_a_plain_string_is_still_data(self):
        # Without the marker the same text is a quoted string, not code. This is the
        # distinction the whole type exists for.
        assert render_literal("Start && WithinResourceLimits") == "'Start && WithinResourceLimits'"

    def test_bind_substitutes_unquoted(self):
        got = bind("INSERT INTO t (Key, Req) VALUES (?, ?)", ("k", Expr("Cpus > 4")))
        assert got == "INSERT INTO t (Key, Req) VALUES ('k', Cpus > 4)"

    def test_equality_and_repr(self):
        assert Expr("a > 1") == Expr("a > 1")
        assert Expr("a > 1") != Expr("a > 2")
        assert "a > 1" in repr(Expr("a > 1"))

    def test_wrapping_an_expr_is_idempotent(self):
        assert Expr(Expr("a > 1")).text == "a > 1"

    def test_exported_from_the_package(self):
        assert htcondordb.Expr is Expr


class TestClassAd2Binding:
    """classad2 objects bind directly, with no wrapper."""

    classad2 = pytest.importorskip("classad2")

    def test_exprtree_binds_as_code(self):
        expr = self.classad2.ExprTree("Start && WithinResourceLimits")
        assert render_literal(expr) == "Start && WithinResourceLimits"

    def test_classad_binds_as_a_nested_ad(self):
        rendered = render_literal(self.classad2.ClassAd({"a": 1, "b": "x"}))
        assert rendered.startswith("[") and rendered.endswith("]")
        assert "\n" not in rendered, "a nested-ad literal must be one line for old-format text"

    def test_undefined_is_not_the_integer_two(self):
        # classad2.Value is an IntEnum, so an int check ahead of it would bind Undefined
        # as 2. This is the trap the ordering in render_literal exists to avoid.
        assert render_literal(self.classad2.Value.Undefined) == "UNDEFINED"
        assert render_literal(self.classad2.Value.Error) == "error"
        assert render_literal(2) == "2"


@pytest.mark.usefixtures("connection")
class TestExprEndToEnd:
    def test_expression_is_stored_not_stringified(self, connection, table):
        classad2 = pytest.importorskip("classad2")
        cur = connection.cursor()
        cur.execute(
            f"INSERT INTO {table} (Key, Name, Cpus, Req) VALUES (?, ?, ?, ?)",
            ("k", "n", 8, Expr("Cpus > 4")),
        )
        (ad,) = list(connection.ads(f"SELECT * FROM {table}"))
        assert "Cpus" in str(ad["Req"]), "the expression was flattened"
        assert ad.eval("Req") is True

    def test_exprtree_parameter(self, connection, table):
        classad2 = pytest.importorskip("classad2")
        cur = connection.cursor()
        cur.execute(
            f"INSERT INTO {table} (Key, Cpus, Req) VALUES (?, ?, ?)",
            ("k", 8, classad2.ExprTree("Cpus >= 8")),
        )
        (ad,) = list(connection.ads(f"SELECT * FROM {table}"))
        assert "Cpus" in str(ad["Req"])

    def test_a_string_parameter_stays_a_string(self, connection, table):
        cur = connection.cursor()
        cur.execute(
            f"INSERT INTO {table} (Key, Req) VALUES (?, ?)", ("k", "Cpus > 4")
        )
        cur.execute(f"SELECT Req FROM {table}")
        assert cur.fetchone() == ("Cpus > 4",)
