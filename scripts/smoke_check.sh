#!/usr/bin/env bash
set -euo pipefail
BASE="$1"
fail=0
check() {
  path="$1"
  code=$(curl -sS -o /dev/null -w "%{http_code}" "$BASE$path")
  case "$path" in
    /api/liff/watch|/api/liff/unwatch)
      [[ "$code" == "200" || "$code" == "401" || "$code" == "403" || "$code" == "503" ]] || { echo "NG $path -> $code"; fail=1; }
      ;;
    *)
      [[ "$code" == 2* ]] || { echo "NG $path -> $code"; fail=1; }
      ;;
  esac
}
for p in /liff/activities /api/liff/concerts /api/liff/quick-check /api/liff/watch /api/liff/unwatch
do check "$p"; done
exit $fail
