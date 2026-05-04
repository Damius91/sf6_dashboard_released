from __future__ import annotations

import csv
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

# ===== 設定 =====
APPROX_EVENTS_CSV = "download_events_approx.csv"  # metrics/ 配下
LAST_POLL_FILE    = "last_poll_jst.txt"            # metrics/ 配下
LAST_COUNTS_FILE  = "last_counts.json"             # metrics/ 配下（上書き、膨らまない）

JST_TZ = ZoneInfo("Asia/Tokyo")


def _fmt(dt: datetime) -> str:
    return f"{dt.year}.{dt.month}.{dt.day} {dt.hour:02d}.{dt.minute:02d}"


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


def _load_last_counts(path: Path) -> Dict[str, int]:
    """
    前回の累計カウントを読む（delta計算用）。
    last_counts.json から読み込む。存在しなければ空dictを返す。
    """
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {k: int(v) for k, v in data.items()}
    except (json.JSONDecodeError, ValueError):
        return {}


def _save_last_counts(path: Path, counts: Dict[str, int]) -> None:
    """
    今回の累計カウントを上書き保存する（追記しないので膨らまない）。
    """
    path.write_text(json.dumps(counts, ensure_ascii=False, indent=2), encoding="utf-8")


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
    repo  = os.environ.get("REPO", "").strip()
    token = os.environ.get("GITHUB_TOKEN", "").strip()

    if not owner or not repo or not token:
        raise SystemExit("Missing OWNER/REPO/GITHUB_TOKEN env vars")

    metrics_dir = Path("metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)

    events_csv_path   = metrics_dir / APPROX_EVENTS_CSV
    last_poll_path    = metrics_dir / LAST_POLL_FILE
    last_counts_path  = metrics_dir / LAST_COUNTS_FILE

    # 前回の累計を読む（delta計算用）
    last_counts = _load_last_counts(last_counts_path)

    # 最新リリースを取得
    latest = _gh_api_get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest",
        token,
    )
    release_tag = latest.get("tag_name") or "latest"
    assets: List[dict] = latest.get("assets") or []

    now_jst = datetime.now(timezone.utc).astimezone(JST_TZ).replace(microsecond=0)

    # 近似イベントログ用の「前回ポーリング時刻」
    prev_poll_jst = _read_last_poll_jst(last_poll_path)
    is_first_poll = (prev_poll_jst is None)
    if prev_poll_jst is None:
        # 初回は window を作れないのでイベント出力なし
        prev_poll_jst = now_jst

    # 近似イベントログCSVヘッダ初期化
    events_header = ["start", "end", "asset_name", "delta_downloads"]
    if not events_csv_path.exists():
        with events_csv_path.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(events_header)

    # 今回の累計カウントを組み立てつつ、増分があればイベントCSVに追記
    current_counts: Dict[str, int] = {}
    event_rows_written = 0

    with events_csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for a in assets:
            name  = str(a.get("name", ""))
            total = int(a.get("download_count", 0))
            prev_total = last_counts.get(name, total)
            delta = total - prev_total

            current_counts[name] = total

            # 初回は window が0幅になるため出力しない
            if not is_first_poll and delta > 0:
                w.writerow([
                    _fmt(prev_poll_jst),
                    _fmt(now_jst),
                    name,
                    delta,
                ])
                event_rows_written += 1

    # 今回の累計を上書き保存（膨らまない）
    _save_last_counts(last_counts_path, current_counts)

    # 最後にポーリング時刻を保存（上書き）
    _write_last_poll_jst(last_poll_path, now_jst)

    print(f"[{now_jst.isoformat()}] Polled {len(assets)} asset(s) for {release_tag}")
    print(f"  Event rows written : {event_rows_written} -> {events_csv_path}")
    print(f"  Last counts saved  -> {last_counts_path}")
    print(f"  Last poll saved    -> {last_poll_path}")


if __name__ == "__main__":
    main()
