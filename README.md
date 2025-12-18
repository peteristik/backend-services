# backend-services

Small collection of Python utilities and cron jobs.

## Setup

Create a virtualenv and install deps:

```bash
pip install -r requirements.txt
```

Required environment variables:

- `COINALYZE_API_KEY` (used by `utils/coinalyze_rest_adapter.py`)
- `DISCORD_EMERGENCY_WEBHOOK_URL` (only if using `EmergencyExitDiscordNotifier`)

## Running the 4h candle refresh job

From the repo root:

```bash
python -m cron_jobs.refresh_db_4h_candles
```

## Crontab

Runs at 2am, 6am, 10am, 2pm, 6pm, 10pm daily and writes logs to `logs/`:

```cron
0 2,6,10,14,18,22 * * * /bin/zsh -lc 'cd /Users/peterhuang/GitHub/backend-services && python -m cron_jobs.refresh_db_4h_candles' >> /Users/peterhuang/GitHub/backend-services/logs/refresh_db_4h_candles.log 2>&1
```
