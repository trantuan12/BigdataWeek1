"""Exercise release refusal without requiring Spark or table writes."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'templates'))
from student_impl import validate_release_binding


class ReleaseBindingTests(unittest.TestCase):
    def setUp(self):
        self.expected = {
            'handoff_logical_sha256': 'a' * 64, 'code_sha256': 'b' * 64,
            'silver_path': 's3a://teaching/run/tables/silver', 'silver_version': 4,
            'gold_path': 's3a://teaching/run/tables/gold', 'gold_version': 2,
            'gold_source_silver_version': 4,
        }
        self.draft = dict(self.expected, author='Transformation author',
                          reviewer='Independent reviewer', steward='Data steward')

    def assert_rejects(self, draft, reason, expected=None):
        passed, reasons = validate_release_binding(draft, self.expected if expected is None else expected)
        self.assertIs(passed, False)
        self.assertIn(reason, reasons)
        self.assertEqual(len(reasons), len(set(reasons)))

    def test_valid_and_input_not_mutated(self):
        before = copy.deepcopy((self.draft, self.expected))
        self.assertEqual(validate_release_binding(self.draft, self.expected), (True, []))
        self.assertEqual((self.draft, self.expected), before)

    def test_every_required_key_is_required(self):
        for key in self.draft:
            with self.subTest(key=key):
                draft = dict(self.draft)
                draft.pop(key)
                self.assertFalse(validate_release_binding(draft, self.expected)[0])

    def test_expected_keys_cannot_be_missing(self):
        for key in self.expected:
            with self.subTest(key=key):
                expected = dict(self.expected)
                expected.pop(key)
                self.assertFalse(validate_release_binding(self.draft, expected)[0])

    def test_self_review_normalization(self):
        for reviewer in ('Transformation author', ' transformation AUTHOR '):
            with self.subTest(reviewer=reviewer):
                self.assert_rejects(dict(self.draft, reviewer=reviewer), 'REVIEW')

    def test_empty_or_wrong_identity_types(self):
        for key in ('author', 'reviewer', 'steward'):
            for value in ('', '  ', None, 7, False, []):
                with self.subTest(key=key, value=value):
                    self.assert_rejects(dict(self.draft, **{key: value}), 'METADATA')

    def test_wrong_or_malformed_hash(self):
        for key, reason in (('code_sha256', 'CODE_BINDING'),
                            ('handoff_logical_sha256', 'HANDOFF_BINDING')):
            for value in ('c' * 64, '', None, 1, 'A' * 64, 'g' * 64, 'a' * 63):
                with self.subTest(key=key, value=value):
                    self.assert_rejects(dict(self.draft, **{key: value}), reason)

    def test_wrong_paths(self):
        for key in ('silver_path', 'gold_path'):
            for value in ('s3a://other/table', '', ' ', None, 1):
                with self.subTest(key=key, value=value):
                    self.assert_rejects(dict(self.draft, **{key: value}), 'VERSION_BINDING')

    def test_versions_are_nonnegative_integers(self):
        for key in ('silver_version', 'gold_version', 'gold_source_silver_version'):
            for value in (-1, True, False, 4.0, '4', None, 99):
                with self.subTest(key=key, value=value):
                    self.assert_rejects(dict(self.draft, **{key: value}), 'VERSION_BINDING')

    def test_even_matching_mixed_pair_rejects(self):
        draft = dict(self.draft, gold_source_silver_version=3)
        expected = dict(self.expected, gold_source_silver_version=3)
        self.assert_rejects(draft, 'VERSION_BINDING', expected)

    def test_malformed_containers_reject_cleanly(self):
        for draft, expected in ((None, self.expected), ([], self.expected),
                                (self.draft, None), (self.draft, []), ({}, {})):
            with self.subTest(draft_type=type(draft).__name__, expected_type=type(expected).__name__):
                self.assertFalse(validate_release_binding(draft, expected)[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
