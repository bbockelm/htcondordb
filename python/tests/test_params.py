"""Parameter binding.

This is the driver's injection boundary -- the daemon has no server-side bind parameters --
so these tests are about escaping and placeholder detection, not convenience.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

from htcondordb._errors import DataError, ProgrammingError
from htcondordb._params import bind, placeholder_count, render_literal


class TestRenderLiteral:
    def test_none_is_undefined(self):
        assert render_literal(None) == "UNDEFINED"

    def test_bool_before_int(self):
        # bool subclasses int; `true` and `1` are different values to the ClassAd engine.
        assert render_literal(True) == "true"
        assert render_literal(False) == "false"

    @pytest.mark.parametrize("value", [0, 1, -1, 2**63 - 1, -(2**63)])
    def test_int_in_range(self, value):
        assert render_literal(value) == str(value)

    @pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**200])
    def test_int_out_of_range(self, value):
        with pytest.raises(DataError, match="64-bit"):
            render_literal(value)

    def test_float_round_trips(self):
        assert float(render_literal(0.1)) == 0.1
        assert float(render_literal(1e300)) == 1e300

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_rejected(self, value):
        with pytest.raises(DataError, match="no literal"):
            render_literal(value)

    def test_decimal_widens_to_real(self):
        assert float(render_literal(decimal.Decimal("1.5"))) == 1.5

    def test_plain_string(self):
        assert render_literal("alice") == "'alice'"

    def test_single_quote_is_doubled(self):
        # The daemon's lexer unquotes '' back to one quote, so this is exact -- and it is
        # the escape that stops a value from closing the literal early.
        assert render_literal("O'Brien") == "'O''Brien'"

    def test_injection_attempt_stays_data(self):
        hostile = "x' OR 1=1 --"
        assert render_literal(hostile) == "'x'' OR 1=1 --'"

    def test_backslash_is_literal(self):
        # Only ' is special to the daemon's lexer; it re-escapes for ClassAd on its side,
        # so the driver must NOT double backslashes here.
        assert render_literal("C:\\path") == "'C:\\path'"

    def test_double_quote_is_literal(self):
        assert render_literal('say "hi"') == "'say \"hi\"'"

    def test_newline_survives(self):
        assert render_literal("a\nb") == "'a\nb'"

    def test_bytes_rejected(self):
        with pytest.raises(DataError, match="no binary type"):
            render_literal(b"\x00\x01")

    def test_datetime_is_epoch_seconds(self):
        moment = datetime.datetime(2026, 1, 2, 3, 4, 5)
        assert render_literal(moment) == str(int(moment.timestamp()))

    def test_date_is_midnight_epoch_seconds(self):
        day = datetime.date(2026, 1, 2)
        expected = int(datetime.datetime(2026, 1, 2).timestamp())
        assert render_literal(day) == str(expected)

    def test_sequence_becomes_classad_list(self):
        assert render_literal([1, 2, 3]) == "{1, 2, 3}"
        assert render_literal(("a", None)) == "{'a', UNDEFINED}"

    def test_unsupported_type(self):
        with pytest.raises(DataError, match="cannot bind"):
            render_literal(object())


class TestPlaceholderDetection:
    def test_counts_plain_placeholders(self):
        assert placeholder_count("SELECT * FROM t WHERE a = ? AND b = ?") == 2

    def test_ignores_question_mark_in_single_quotes(self):
        assert placeholder_count("SELECT * FROM t WHERE a = 'why?'") == 0

    def test_ignores_question_mark_in_double_quotes(self):
        # The daemon accepts "..." as a string too (the ClassAd spelling).
        assert placeholder_count('SELECT * FROM t WHERE a = "why?"') == 0

    def test_escaped_quote_does_not_end_the_string(self):
        # 'O''Brien?' is one string containing a ?; the placeholder is the trailing one.
        assert placeholder_count("SELECT * FROM t WHERE a = 'O''Brien?' AND b = ?") == 1

    def test_unterminated_string_is_an_error(self):
        with pytest.raises(ProgrammingError, match="unterminated"):
            placeholder_count("SELECT * FROM t WHERE a = 'oops")


class TestBind:
    def test_no_parameters(self):
        assert bind("SELECT 1", None) == "SELECT 1"
        assert bind("SELECT 1", ()) == "SELECT 1"

    def test_substitutes_in_order(self):
        got = bind("SELECT * FROM t WHERE a = ? AND b = ?", ("x", 2))
        assert got == "SELECT * FROM t WHERE a = 'x' AND b = 2"

    def test_value_containing_a_placeholder_is_not_rescanned(self):
        # A '?' inside a bound value must not be treated as another placeholder -- the
        # classic bug in naive sequential-replace implementations.
        got = bind("SELECT * FROM t WHERE a = ? AND b = ?", ("what?", 1))
        assert got == "SELECT * FROM t WHERE a = 'what?' AND b = 1"

    def test_value_that_closes_a_quote_cannot_inject(self):
        got = bind("SELECT * FROM t WHERE a = ?", ("' OR 1=1 OR a = '",))
        assert got == "SELECT * FROM t WHERE a = ''' OR 1=1 OR a = '''"

    def test_too_few_parameters(self):
        with pytest.raises(ProgrammingError, match="2 placeholder"):
            bind("SELECT * FROM t WHERE a = ? AND b = ?", ("x",))

    def test_too_many_parameters(self):
        with pytest.raises(ProgrammingError, match="1 placeholder"):
            bind("SELECT * FROM t WHERE a = ?", ("x", "y"))

    def test_dict_rejected(self):
        with pytest.raises(ProgrammingError, match="qmark"):
            bind("SELECT * FROM t WHERE a = ?", {"a": 1})

    def test_bare_string_rejected(self):
        # A common slip: passing "alice" instead of ("alice",) would otherwise bind each
        # character as a separate parameter.
        with pytest.raises(ProgrammingError, match="not a string"):
            bind("SELECT * FROM t WHERE a = ?", "alice")
