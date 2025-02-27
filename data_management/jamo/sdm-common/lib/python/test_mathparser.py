import unittest
from mathparser import NumericStringParser
from parameterized import parameterized
import math


class TestMathParser(unittest.TestCase):

    @parameterized.expand([
        ("9", 9),
        ("-9", -9),
        ("--9", 9),
        ("-E", -math.e),
        ("9 + 3 + 6", 9 + 3 + 6),
        ("9 + 3 / 11", 9 + 3.0 / 11),
        ("(9 + 3)", (9 + 3)),
        ("(9+3) / 11", (9 + 3.0) / 11),
        ("9 - 12 - 6", 9 - 12 - 6),
        ("9 - (12 - 6)", 9 - (12 - 6)),
        ("2*3.14159", 2 * 3.14159),
        ("3.1415926535*3.1415926535 / 10", 3.1415926535 * 3.1415926535 / 10),
        ("PI * PI / 10", math.pi * math.pi / 10),
        ("PI*PI/10", math.pi * math.pi / 10),
        ("PI^2", math.pi ** 2),
        ("round(PI^2)", round(math.pi ** 2)),
        ("6.02E23 * 8.048", 6.02e23 * 8.048),
        ("e / 3", math.e / 3),
        ("sin(PI/2)", math.sin(math.pi / 2)),
        ("10+sin(PI/4)^2", 10 + math.sin(math.pi / 4) ** 2),
        ("trunc(E)", int(math.e)),
        ("trunc(-E)", int(-math.e)),
        ("round(E)", round(math.e)),
        ("round(-E)", round(-math.e)),
        ("E^PI", math.e ** math.pi),
        ("2^3^2", 2 ** 3 ** 2),
        ("(2^3)^2", (2 ** 3) ** 2),
        ("2^3+2", 2 ** 3 + 2),
        ("2^3+5", 2 ** 3 + 5),
        ("2^9", 2 ** 9),
        ("sgn(-2)", -1),
        ("sgn(0)", 0),
        ("sgn(0.1)", 1),
        ("sgn(cos(PI/4))", 1),
        ("sgn(cos(PI/2))", 0),
        ("sgn(cos(PI*3/4))", -1),
        ("+(sgn(cos(PI/4)))", 1),
        ("-(sgn(cos(PI/4)))", -1),
    ])
    def test_eval(self, string_input, expected_value):
        parser = NumericStringParser()

        self.assertEqual(parser.eval(string_input), expected_value)


if __name__ == '__main__':
    unittest.main()
