"""
GitHub Push Script
Отправляет все новые/изменённые CSV-файлы из папки data/ на GitHub.
Запускается автоматически раз в час через Планировщик задач Windows.
"""

import base64
import time
import yaml
import requests
from pathlib import Path
from datetime import datetime, timezone

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
DATA_PATH   = Path(__file__).parent.parent / "data"
LAST_PUSH_FILE = Path(__file__).parent.parent / ".last_push"


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_last_push_time() -> float:
    if LAST_PUSH_FILE.exists():
        try:
            return float(LAST_PUSH_FILE.read_text().strip())
        except Exception:
            pass
    return 0.0


def save_push_time():
    LAST_PUSH_FILE.write_text(str(time.time()))


def push_file(filepath: Path, token: str, repo: str) -> bool:
    remote_path = filepath.relative_to(Path(__file__).parent.parent).as_posix()
    headers = {
        "Authorization": f"token {token}",
        "Content-Type": "application/json",
    }

    # Получаем SHA если файл уже есть на GitHub
    r = requests.get(
        f"https://api.github.com/repos/{repo}/contents/{remote_path}",
        headers=headers, timeout=15
    )
    sha = r.json().get("sha", "") if r.status_code == 200 else ""

    # Читаем и кодируем файл
    with open(filepath, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    payload = {
        "message": f"data: {filepath.name} | {ts}",
        "content": content,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(
        f"https://api.github.com/repos/{repo}/contents/{remote_path}",
        headers=headers, json=payload, timeout=15
    )
    return r.status_code in (200, 201)


def main():
    config = load_config()
    github = config.get("github", {})
    token = github.get("token", "")
    repo  = github.get("repo", "")

    if not token or not repo:
        print("[ERR] GitHub token или repo не настроены в config.yaml")
        return

    last_push = get_last_push_time()
    all_csv = list(DATA_PATH.rglob("*.csv"))

    # Только файлы изменённые с последнего пуша
    changed = [f for f in all_csv if f.stat().st_mtime > last_push]

    if not changed:
        print(f"Нет изменений с последнего пуша. Файлов всего: {len(all_csv)}")
        return

    print(f"\n=== GitHub Push | {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} ===")
    print(f"Изменено файлов: {len(changed)} из {len(all_csv)}\n")

    ok = 0
    for filepath in changed:
        try:
            if push_file(filepath, token, repo):
                print(f"  [OK] {filepath.relative_to(Path(__file__).parent.parent)}")
                ok += 1
            else:
                print(f"  [ERR] {filepath.name}")
            time.sleep(0.5)  # не превышаем лимиты GitHub API
        except Exception as e:
            print(f"  [ERR] {filepath.name}: {e}")

    save_push_time()
    print(f"\nОтправлено: {ok}/{len(changed)}\n")


if __name__ == "__main__":
    main()
