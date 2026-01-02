from __future__ import annotations

import csv
import os
import json
import urllib.request
from zoneinfo import ZoneInfo

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


@dataclass(frozen=True)
class AssetRow:
    timestamp_utc: str
    release_tag: str
    asset_name: str
    download_count_total: int
    delta_since_prev: int


def _gh_api_get(url: str, token: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "release-download-logger",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_last_counts(csv_path: Path) -> Dict[str, int]:
    last: Dict[str, int] = {}
    if not csv_path.exists():
        return last
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            last[row["asset_name"]] = int(row["download_count_total"])
    return last


def main() -> None:
    owner = os.environ["OWNER"].strip()
    repo = os.environ["REPO"].strip()
    token = os.environ["GITHUB_TOKEN"].strip()

    # 最新リリースを取得
    latest = _gh_api_get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest",
        token,
    )
    release_tag = latest.get("tag_name") or "latest"
    assets: List[dict] = latest.get("assets") or []

    metrics_dir = Path("metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)
    csv_path = metrics_dir / ""downloads_log_jst.csv""

    last_counts = _load_last_counts(csv_path)

    jst = ZoneInfo("Asia/Tokyo")
    now_jst = datetime.now(timezone.utc).astimezone(jst).replace(microsecond=0).isoformat()


    header = ["timestamp_jst", "release_tag", "asset_name", "download_count_total", "delta_since_prev"]
    new_rows: List[AssetRow] = []

    for a in assets:
        name = str(a.get("name", ""))
        total = int(a.get("download_count", 0))
        prev = last_counts.get(name, total)
        delta = total - prev
        new_rows.append(AssetRow(_utc, release_tag, name, total, delta))

    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        for r in new_rows:
            writer.writerow([r.timestamp_jst, r.release_tag, r.asset_name, r.download_count_total, r.delta_since_prev])

    print(f"Wrote {len(new_rows)} rows to {csv_path}")


if __name__ == "__main__":
    main()
