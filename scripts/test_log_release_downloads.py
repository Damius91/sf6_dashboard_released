"""log_release_downloads.py の純粋関数のテスト（GitHub には触らない）。

実行: python -m unittest discover -s scripts -p "test_*.py"
"""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))
import log_release_downloads as lrd  # noqa: E402


def rel(tag, assets, draft=False):
    return {"tag_name": tag, "draft": draft,
            "assets": [{"name": n, "download_count": c} for n, c in assets]}


class TestCountKey(unittest.TestCase):
    def test_legacy_is_bare_asset_name(self):
        # 既存の last_counts.json（asset 名だけがキー）と連続させる
        self.assertEqual(lrd.count_key("latest", "sf6_dashboard_bundle_latest.zip"),
                         "sf6_dashboard_bundle_latest.zip")

    def test_versioned_release_is_tag_prefixed(self):
        self.assertEqual(lrd.count_key("v3.0.0", "sf6_dashboard_bundle_v3.0.0.zip"),
                         "v3.0.0/sf6_dashboard_bundle_v3.0.0.zip")


class TestCollectCounts(unittest.TestCase):
    def test_skips_drafts_and_keys_by_release(self):
        rows = lrd.collect_counts([
            rel("latest", [("sf6_dashboard_bundle_latest.zip", 10), ("sf6_dashboard_bundle_v3.0.0.zip", 2)]),
            rel("v3.0.0", [("sf6_dashboard_bundle_v3.0.0.zip", 5)]),
            rel("v3.0.1", [("sf6_dashboard_bundle_v3.0.1.zip", 7)], draft=True),
        ])
        self.assertEqual(rows, [
            ("sf6_dashboard_bundle_latest.zip", "sf6_dashboard_bundle_latest.zip", 10),
            ("sf6_dashboard_bundle_v3.0.0.zip", "sf6_dashboard_bundle_v3.0.0.zip", 2),
            ("v3.0.0/sf6_dashboard_bundle_v3.0.0.zip", "sf6_dashboard_bundle_v3.0.0.zip", 5),
        ])


class TestComputeDeltas(unittest.TestCase):
    def test_first_seen_key_has_no_event(self):
        events, current = lrd.compute_deltas([("v3.0.0/a.zip", "a.zip", 5)], {})
        self.assertEqual(events, [])
        self.assertEqual(current, {"v3.0.0/a.zip": 5})

    def test_increase_emits_bare_asset_name(self):
        events, _ = lrd.compute_deltas([("v3.0.0/a.zip", "a.zip", 8)], {"v3.0.0/a.zip": 5})
        self.assertEqual(events, [("a.zip", 3)])

    def test_same_name_in_two_releases_is_counted_separately(self):
        rows = [("sf6_dashboard_bundle_v3.0.0.zip", "sf6_dashboard_bundle_v3.0.0.zip", 3),
                ("v3.0.0/sf6_dashboard_bundle_v3.0.0.zip", "sf6_dashboard_bundle_v3.0.0.zip", 9)]
        last = {"sf6_dashboard_bundle_v3.0.0.zip": 2, "v3.0.0/sf6_dashboard_bundle_v3.0.0.zip": 4}
        events, current = lrd.compute_deltas(rows, last)
        self.assertEqual(events, [("sf6_dashboard_bundle_v3.0.0.zip", 1),
                                  ("sf6_dashboard_bundle_v3.0.0.zip", 5)])
        self.assertEqual(current, {"sf6_dashboard_bundle_v3.0.0.zip": 3,
                                   "v3.0.0/sf6_dashboard_bundle_v3.0.0.zip": 9})

    def test_old_last_counts_continue_for_legacy(self):
        # 移行前の last_counts.json（asset 名キー）をそのまま読んで差分が出る
        events, _ = lrd.compute_deltas(
            [("sf6_dashboard_bundle_latest.zip", "sf6_dashboard_bundle_latest.zip", 101)],
            {"sf6_dashboard_bundle_latest.zip": 100},
        )
        self.assertEqual(events, [("sf6_dashboard_bundle_latest.zip", 1)])


class TestListReleases(unittest.TestCase):
    def test_follows_pages_until_short_page(self):
        pages = [[{"tag_name": f"t{i}"} for i in range(100)], [{"tag_name": "last"}]]
        with mock.patch.object(lrd, "_gh_api_get", side_effect=pages) as get:
            got = lrd._list_releases("o", "r", "tok")
        self.assertEqual(len(got), 101)
        urls = [c.args[0] for c in get.call_args_list]
        self.assertEqual(urls, [
            "https://api.github.com/repos/o/r/releases?per_page=100&page=1",
            "https://api.github.com/repos/o/r/releases?per_page=100&page=2",
        ])


if __name__ == "__main__":
    unittest.main()
