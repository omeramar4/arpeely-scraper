FROM python:3.10-slim

WORKDIR /app
COPY ../.. /app

RUN pip install --upgrade pip && pip install poetry && poetry install
EXPOSE 8000
CMD ["poetry", "run", "uvicorn", "arpeely_scraper.app.main:app", "--host", "0.0.0.0", "--port", "8000"]

