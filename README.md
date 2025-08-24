# arpeely-scraper

## 1. Introduction

**arpeely-scraper** is a robust, production-ready web scraping application designed to crawl websites, extract content, and classify topics using state-of-the-art machine learning models. It is built with FastAPI for RESTful API access and also provides a command-line interface (CLI) for direct control. The motivation behind arpeely-scraper is to offer a scalable, fault-tolerant, and extensible solution for large-scale web data collection and topic classification, with seamless integration to a PostgreSQL database for persistent storage and recovery.

**Key Features:**
- Synchronous and asynchronous scraping modes for performance and flexibility.
- Topic classification using Hugging Face zero-shot models.
- Fault tolerance: resumes scraping from the last state after a crash.
- REST API and CLI interfaces for easy integration and automation.
- Singleton database connection for efficiency and reliability.

---

## 2. How to Run the Scraper

arpeely-scraper supports two main modes of operation: **REST API** and **CLI**. Both require a running PostgreSQL database.

### Prerequisites

- **PostgreSQL Database**: Ensure you have a PostgreSQL instance running. You can use the provided `docker-compose.yml` for easy setup.
- **Python Environment**: Install dependencies using Poetry or pip as specified in `pyproject.toml`.

### Database Setup

Start the PostgreSQL database using Docker Compose:

```bash
docker compose up postgres -d
```

This will start a PostgreSQL instance with default credentials as specified in your environment variables or Docker Compose file.

### REST API Mode

To run the scraper as a REST API server:

```bash
docker compose up fastapi -d
```

**API Endpoints:**
- `POST /scrape`: Start synchronous scraping.
- `POST /ascrape`: Start asynchronous scraping.
- `GET /status`: Check scraping status for a base URL.
- `GET /results`: Retrieve scraping results for a base URL.
- `POST /add_topics`: Add or update the list of topics for classification.

You can interact with the API using tools like `curl`, Postman, or directly from your code.

### CLI Mode

To run the scraper from the command line, use the CLI entrypoint:

```bash
docker compose run cli
```

This command opens an interactive Bash session where the topic classification model is loaded first, after which you can execute CLI commands. Available commands include:

- `scrape`: Start synchronous scraping for a given URL.
- `ascrape`: Start asynchronous scraping for a given URL.
- `status`: Check the scraping status for a base URL.
- `results`: Retrieve scraping results for a base URL.
- `add_topics`: Add or update the list of topics for classification.
- `help`: Show help information for CLI commands.

---

## 3. Topic Classifier Model

arpeely-scraper uses a **zero-shot classification model** from Hugging Face's Transformers library to automatically assign topics to scraped content.

- **Model Selection**: The model name is configurable via the `TOPIC_MODEL_NAME` environment variable (default: `valhalla/distilbart-mnli-12-1`). You can use any compatible zero-shot classification model from Hugging Face.
- **Topic List**: The application maintains a list of topics (e.g., news, sports, technology, etc.) which can be updated at runtime via the `/add_topics` API endpoint.
- **Classification Logic**: For each scraped page, the classifier predicts the most relevant topic from the current topic list. The model is loaded once at startup for efficiency.
- **Extensibility**: You can add new topics dynamically without restarting the application.

---

## 4. Details

### Synchronous and Asynchronous Modes

- **Synchronous Mode** (`/scrape`): Processes pages one at a time, suitable for smaller crawls or environments where concurrency is not needed.
- **Asynchronous Mode** (`/ascrape`): Uses Python's `asyncio` and `aiohttp` for concurrent requests, enabling high-throughput scraping of large sites.

### Singleton Database Connection

- The application uses a singleton pattern for the database connector, ensuring only one connection pool is created per application instance. This improves resource usage and avoids connection leaks.

### Fault Tolerance and Recovery

arpeely-scraper is designed to be fault-tolerant and can recover gracefully from crashes or interruptions. When the application restarts, it checks the database for URLs in the "queued" state. The new recovery logic ensures that for each queued URL, its associated source URL is also re-added to the processing list. This means the entire scraping process for the source URL is repeated, guaranteeing that no part of the workflow is missed, regardless of when the crash occurred.

**How it works:**
- On recovery, both queued URLs and their source URLs are scheduled for reprocessing.
- This approach ensures that any partially scraped data is completed and the integrity of the scraping process is maintained.
- The system avoids duplicate processing by tracking already scheduled URLs.

This robust recovery mechanism provides greater reliability for large-scale or long-running scraping jobs, ensuring that all intended data is collected even after unexpected failures.

### Additional Features

- **Topic Classification**: Each page is classified into a topic using the loaded model, and the result is stored in the database.
- **API and CLI**: Both interfaces provide full control over scraping operations and topic management.
- **Extensible Design**: Easily add new scraping logic, models, or database backends as needed.

---

## Example Workflow

1. Start PostgreSQL with Docker Compose.
2. Launch the REST API server or CLI.
3. Use the `/scrape` or `/ascrape` endpoint to start scraping a website.
4. Check progress with `/status` and retrieve results with `/results`.
5. Update topics for classification with `/add_topics` as needed.
6. If the application crashes, simply restart—it will resume from where it left off.

---

## Environment Variables

- `POSTGRES_DB`: Database name (default: `arpeely_db`)
- `POSTGRES_USER`: Database user (default: `arpeely_user`)
- `POSTGRES_PASSWORD`: Database password (default: `arpeely_pass`)
- `POSTGRES_HOST`: Database host (default: `localhost`)
- `POSTGRES_PORT`: Database port (default: `5432`)
- `TOPIC_MODEL_NAME`: Hugging Face model name for topic classification (default: `valhalla/distilbart-mnli-12-1`)

---
