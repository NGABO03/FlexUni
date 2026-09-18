#!/usr/bin/env sh
set -eu
python -m flask --app app db init 2>/dev/null || true
python -m flask --app app db migrate -m "initial production schema"
python -m flask --app app db upgrade
