FROM python:3.10-slim

WORKDIR /app
COPY ../.. /app

COPY arpeely_scraper/app/cli/cli_entrypoint.sh /usr/local/bin/cli_entrypoint.sh
RUN chmod +x /usr/local/bin/cli_entrypoint.sh

RUN pip install --upgrade pip
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install
