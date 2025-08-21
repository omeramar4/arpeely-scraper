CREATE TABLE IF NOT EXISTS scraped_urls (
    base_url VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    source_url VARCHAR,
    depth INTEGER NOT NULL,
    title VARCHAR,
    links_to_texts JSONB,
    topic VARCHAR DEFAULT 'other',
    status url_status_enum NOT NULL,
    PRIMARY KEY (base_url, url)
);
