FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "if [ \"$APP_MODE\" = \"worker\" ]; then python bot.py; else uvicorn dashboard:app --host 0.0.0.0 --port ${PORT:-8000}; fi"]
