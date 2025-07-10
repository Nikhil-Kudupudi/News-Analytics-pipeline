CREATE TABLE IF NOT EXISTS
ARTICLES (
    id  INT AUTOINCREMENT PRIMARY KEY,
    author TEXT,
    title TEXT,
    description TEXT,
    url TEXT,
    urlToImage TEXT,
    publishedAt TEXT,
    content TEXT,
    apiType VARCHAR(100),
    source_id TEXT,
    source_name TEXT
)



