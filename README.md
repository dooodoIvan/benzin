# Бензин · Воронеж

С 12:00 до 24:00 (МСК) GitHub Actions читает публичный канал [@voronezh_benzin](https://t.me/voronezh_benzin):
днём каждые 45 минут, вечером каждые 10 минут. Отчёты о наличии топлива дописываются в `data/obs.csv`,
сводка публикуется на https://dooodoivan.github.io/benzin-page/, а бот пишет в Telegram,
когда на одной из отслеживаемых заправок появляется АИ-95/98.

- `benzin.py` — сборщик, сводка, статистика (`python3 benzin.py --help`)
- `.github/workflows/collect.yml` — запуск на GitHub Actions
- секреты: `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `PAGES_DEPLOY_KEY`
