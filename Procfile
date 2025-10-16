web: GUNICORN_CMD_ARGS="--workers 2 --threads 4 --timeout 30 --log-level debug" gunicorn -k sync -b :$PORT app:app
