# Telegram Peer Review Bot

## Local run

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:BOT_TOKEN="your_bot_token"
python app.py
```

## Scalingo

Required environment variables:

- `BOT_TOKEN`
- `ADMIN_ID`
