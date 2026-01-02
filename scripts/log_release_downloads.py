from __future__ import annotations

import csv
import json
import os
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


# ===== 設定 =====
SNAPSHOT_CSV = "downloads_log.csv"               # metrics/ 配下
APPROX_EVENTS_CSV = "download_events_approx.csv" # metrics/ 配下
LAST_POLL_FILE = "last_poll_jst.txt"             # metrics/ 配下
JST_TZ = ZoneInfo("Asia/Tokyo")


@dataclass(frozen=True)
class SnapshotRow:
    timestamp_jst: str
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


def _read_csv_header(path: Path) -> Optional[List[str]]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return None


def _backup_if_header_mismatch(path: Path, expected_header: List[str]) -> None:
    """
    既存CSVのヘッダが期待値と違う場合、破壊を避けるために退避して新規作成する。
    """
    hdr = _read_csv_header(path)
    if hdr is None:
        return
    if hdr == expected_header:
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = path.with_name(f"{path.stem}_backup_{ts}{path.suffix}")
    path.rename(backup)
    print(f"[WARN] Header mismatch. Backed up {path} -> {backup}")


def _load_last_counts(snapshot_csv_path: Path) -> Dict[str, int]:
    """
    asset_name -> last download_count_total
    """
    last: Dict[str, int] = {}
    if not snapshot_csv_path.exists():
        return last

    with snapshot_csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 同名assetの最終値で上書きされ続けるので、結果的に「最後」が残る
            name = row.get("asset_name", "")
            total = row.get("download_count_total", "")
            if name and total:
                try:
                    last[name] = int(total)
                except ValueError:
                    # 壊れた行は無視
                    pass
    return last


def _read_last_poll_jst(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    s = path.read_text(encoding="utf-8").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except ValueError:
        return None


def _write_last_poll_jst(path: Path, dt_jst: datetime) -> None:
    path.write_text(dt_jst.replace(microsecond=0).isoformat(), encoding="utf-8")


def main() -> None:
    owner = os.environ.get("OWNER", "").strip()
    repo = os.environ.get("REPO", "").strip()
    token = os.environ.get("GITHUB_TOKEN", "").strip()

    if not owner or not repo or not token:
        raise SystemExit("Missing OWNER/REPO/GITHUB_TOKEN env vars")

    metrics_dir = Path("metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)

    snapshot_csv_path = metrics_dir / SNAPSHOT_CSV
    events_csv_path = metrics_dir / APPROX_EVENTS_CSV
    last_poll_path = metrics_dir / LAST_POLL_FILE

    # スナップショットCSVの期待ヘッダ（JST）
    snapshot_header = [
        "timestamp_jst",
        "release_tag",
        "asset_name",
        "download_count_total",
        "delta_since_prev",
    ]

    # 既存がUTC版などで混在しているとCSVが壊れるので、ヘッダ不一致なら退避
    _backup_if_header_mismatch(snapshot_csv_path, snapshot_header)

    # 前回の累計を読む（delta計算用）
    last_counts = _load_last_counts(snapshot_csv_path)

    # 最新リリースを取得
    latest = _gh_api_get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest",
        token,
    )
    release_tag = latest.get("tag_name") or "latest"
    assets: List[dict] = latest.get("assets") or []

    now_jst = datetime.now(timezone.utc).astimezone(JST_TZ).replace(microsecond=0)
    now_jst_str = now_jst.isoformat()

    # 近似イベントログ用の「前回ポーリング時刻」
    prev_poll_jst = _read_last_poll_jst(last_poll_path)
    if prev_poll_jst is None:
        # 初回は window を作れないので「イベント出力なし」にする
        prev_poll_jst = now_jst

    # スナップショット行生成
    rows: List[SnapshotRow] = []
    for a in assets:
        name = str(a.get("name", ""))
        total = int(a.get("download_count", 0))

        prev_total = last_counts.get(name, total)
        delta = total - prev_total

        rows.append(SnapshotRow(
            timestamp_jst=now_jst_str,
            release_tag=release_tag,
            asset_name=name,
            download_count_total=total,
            delta_since_prev=delta,
        ))

    # スナップショットCSVへ追記
    write_header = not snapshot_csv_path.exists()
    with snapshot_csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(snapshot_header)
        for r in rows:
            w.writerow([
                r.timestamp_jst,
                r.release_tag,
                r.asset_name,
                r.download_count_total,
                r.delta_since_prev,
            ])

    # 近似イベントログ（増分があるときだけ、区間で記録）
    # 形式: window_start_jst, window_end_jst, asset_name, delta_downloads
    events_header = ["window_start_jst", "window_end_jst", "asset_name", "delta_downloads"]
    if not events_csv_path.exists():
        with events_csv_path.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(events_header)

    # “初回”は prev_poll==now なので delta>0でもイベントを出すか迷うが、
    # 誤解を避けるため初回は出さない（windowが0幅になるため）
    is_first_poll = (prev_poll_jst == now_jst and not last_poll_path.exists())

    if not is_first_poll:
        with events_csv_path.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            for r in rows:
                if r.delta_since_prev > 0:
                    w.writerow([
                        prev_poll_jst.isoformat(),
                        now_jst.isoformat(),
                        r.asset_name,
                        r.delta_since_prev,
                    ])

    # 最後にポーリング時刻を保存
    _write_last_poll_jst(last_poll_path, now_jst)

    print(f"Wrote snapshot rows: {len(rows)} -> {snapshot_csv_path}")
    print(f"Updated last poll -> {last_poll_path}")
    print(f"Approx events -> {events_csv_path}")


if __name__ == "__main__":
    main()
