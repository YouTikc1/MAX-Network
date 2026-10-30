#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -d .venv ]; then
  python_bin=''
  for candidate in python3.12 /Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12 python3.13 python3.14 python3; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; assert sys.version_info >= (3,12)' >/dev/null 2>&1; then
      python_bin="$candidate"
      break
    fi
  done
  if [ -z "$python_bin" ]; then
    echo 'Установите Python 3.12+ с python.org (macOS universal2 installer).'
    exit 1
  fi
  "$python_bin" -m venv .venv
fi
.venv/bin/python -c 'import sys; assert sys.version_info >= (3,12), "Нужен Python 3.12+. Переименуйте старую .venv и повторите запуск."'
.venv/bin/python -m pip install -r requirements.lock.txt
if [ ! -f .env ]; then
  echo 'Нет .env. Поместите сохранённый файл рядом с app.py; не отправляйте его в Git.'
  exit 1
fi
# Online SQLite backup before additive migrations. Never overwrite an existing backup.
.venv/bin/python - <<'PY'
import sqlite3
from pathlib import Path
from datetime import datetime
source=Path('network.db')
if source.exists():
    folder=Path('backups'); folder.mkdir(exist_ok=True)
    target=folder / ('network-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '.db')
    with sqlite3.connect('file:network.db?mode=ro',uri=True) as original, sqlite3.connect(target) as backup:
        original.backup(backup)
    print('Резервная копия базы:', target)
PY
exec .venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000 --workers 1
