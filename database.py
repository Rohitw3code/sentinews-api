# database.py

import sqlite3
from datetime import datetime
import pytz

DB_NAME = 'news_data.db'

# --- Table Creation ---
def create_database():
    """Initializes the database and creates all tables if they don't exist."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        # Links to be scraped
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL UNIQUE,
            source_website TEXT NOT NULL, scraped_date TEXT NOT NULL
        )''')
        # Scraped article content
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT, link_id INTEGER NOT NULL,
            url TEXT NOT NULL UNIQUE, title TEXT, author TEXT, publication_date TEXT,
            raw_text TEXT, cleaned_text TEXT, is_analyzed INTEGER DEFAULT 0,
            FOREIGN KEY (link_id) REFERENCES links (id)
        );''')
        # Sentiment analysis results
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, article_id INTEGER NOT NULL,
            entity_name TEXT NOT NULL, entity_type TEXT NOT NULL,
            financial_sentiment TEXT NOT NULL, overall_sentiment TEXT NOT NULL,
            reasoning TEXT, FOREIGN KEY (article_id) REFERENCES articles (id)
        )''')
        # API usage and cost tracking logs
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, article_id INTEGER NOT NULL,
            provider TEXT NOT NULL, total_tokens INTEGER, prompt_tokens INTEGER,
            completion_tokens INTEGER, total_cost_usd REAL, timestamp TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles (id)
        )''')
        # Application settings
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        )''')
        # Pipeline execution statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, run_timestamp TEXT NOT NULL,
            new_links_found INTEGER, articles_scraped INTEGER,
            entities_analyzed INTEGER, status TEXT
        )''')
        # Set default schedule time if not present
        cursor.execute("INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)", ('schedule_time', '01:00'))
        conn.commit()
    print("Database initialized successfully.")

# --- Config Management ---
def get_config_value(key, default=None):
    """Retrieves a configuration value from the app_config table."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM app_config WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else default

def set_config_value(key, value):
    """Sets or updates a configuration value in the app_config table."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

# --- Data Addition ---
def add_link(url, source):
    """Adds a new link to the database, ignoring duplicates."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            scraped_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor = conn.execute("INSERT INTO links (url, source_website, scraped_date) VALUES (?, ?, ?)", (url, source, scraped_date))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None

def add_article(link_id, article_data):
    """Adds a scraped article to the database."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.execute('''
                INSERT INTO articles (link_id, url, title, author, publication_date, raw_text, cleaned_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (link_id, article_data['url'], article_data['title'], article_data['author'],
                  article_data['publication_date'], article_data['raw_text'], article_data['cleaned_text']))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None

def add_sentiment(article_id, entity_name, entity_type, financial_sentiment, overall_sentiment, reasoning):
    """Adds a dual sentiment record to the database."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT INTO sentiments (article_id, entity_name, entity_type, financial_sentiment, overall_sentiment, reasoning) VALUES (?, ?, ?, ?, ?, ?)",
            (article_id, entity_name, entity_type, financial_sentiment, overall_sentiment, reasoning)
        )
        conn.commit()

def add_usage_log(article_id, provider, usage_stats):
    """Adds a new usage log entry to the database."""
    with sqlite3.connect(DB_NAME) as conn:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            INSERT INTO usage_logs (article_id, provider, total_tokens, prompt_tokens, completion_tokens, total_cost_usd, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (article_id, provider, usage_stats.get('total_tokens'), usage_stats.get('prompt_tokens'),
              usage_stats.get('completion_tokens'), usage_stats.get('total_cost_usd'), timestamp))
        conn.commit()

def add_pipeline_run(stats):
    """Adds a new pipeline run record to the database."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT INTO pipeline_runs (run_timestamp, new_links_found, articles_scraped, entities_analyzed, status) VALUES (?, ?, ?, ?, ?)",
            (datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S'), stats.get('new_links_found', 0),
             stats.get('articles_scraped', 0), stats.get('entities_analyzed', 0), stats.get('status', 'Completed'))
        )
        conn.commit()

# --- Data Retrieval ---
def get_unscraped_links():
    """Fetches links that have not yet been stored in the articles table."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('SELECT l.id, l.url, l.source_website as source FROM links l LEFT JOIN articles a ON l.id = a.link_id WHERE a.id IS NULL')
        return [dict(row) for row in cursor.fetchall()]

def get_unanalyzed_articles():
    """Fetches articles that have not been analyzed, using the `is_analyzed` flag."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('SELECT id, cleaned_text as text FROM articles WHERE is_analyzed = 0 AND cleaned_text IS NOT NULL AND cleaned_text != "N/A"')
        return [dict(row) for row in cursor.fetchall()]

def mark_article_as_analyzed(article_id):
    """Marks an article as analyzed by setting the `is_analyzed` flag to 1."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("UPDATE articles SET is_analyzed = 1 WHERE id = ?", (article_id,))
        conn.commit()
