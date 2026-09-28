#!/usr/bin/env python3
"""Tests for literal and grouped keyword matching."""

from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app


class KeywordHitsTest(unittest.TestCase):
    def test_all_group_allows_text_between_parts(self) -> None:
        keywords = ["ALL:VMISS|US.LA|TRI"]

        self.assertEqual(
            ["VMISS + US.LA + TRI"],
            app.keyword_hits("VMISS 上新了，上了 US.LA TRI", keywords),
        )

    def test_all_group_ignores_spacing_and_punctuation(self) -> None:
        keywords = ["ALL:VMISS|US.LA|TRI"]

        self.assertEqual(
            ["VMISS + US.LA + TRI"],
            app.keyword_hits("VMISS 新增 US LA - TRI 套餐", keywords),
        )

    def test_all_group_requires_every_part(self) -> None:
        keywords = ["ALL:VMISS|US.LA|TRI"]

        self.assertEqual([], app.keyword_hits("VMISS US.LA", keywords))
        self.assertEqual([], app.keyword_hits("US.LA TRI", keywords))

    def test_regular_keyword_remains_substring_match(self) -> None:
        self.assertEqual(["VPS"], app.keyword_hits("低价 VPS 补货", ["VPS"]))


if __name__ == "__main__":
    unittest.main()
