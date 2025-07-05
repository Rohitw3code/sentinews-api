# api.py

import sqlite3
import os
import threading
import re
from datetime import datetime
from dotenv import load_dotenv

# --- Flask & Web Server Imports ---
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

# --- AI & Pipeline Imports ---
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic.v1 import BaseModel, Field, ValidationError
from typing import List

# --- Custom Module Imports ---
import pipeline
import database
from scrapers import scraper_manager

# --- Configuration ---
DB_NAME = 'news_data.db'
load_dotenv()
PIPELINE_PASSWORD = os.getenv("PIPELINE_PASSWORD")

# --- Global State for Pipeline Tracking ---
pipeline_status_tracker = {
    "is_running": False,
    "status": "Idle",
    "progress": 0,
    "total": 0,
    "current_task": "N/A",
    "stop_event": None, # NEW: For graceful shutdown
}

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Database Helper ---
def get_db_connection():
    """Creates a database connection that returns dictionary-like rows."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

# --- Summarization Agent Setup ---
class Summary(BaseModel):
    """A structured summary of an entity's sentiment profile."""
    positive_financial: List[str] = Field(description="A list of key positive points related to financial performance.")
    negative_financial: List[str] = Field(description="A list of key negative points related to financial performance.")
    neutral_financial: List[str] = Field(description="A list of key neutral points or factual statements related to financial performance.")
    positive_overall: List[str] = Field(description="A list of key positive points related to general operations, products, and decisions.")
    negative_overall: List[str] = Field(description="A list of key negative points related to general operations, products, and decisions.")
    neutral_overall: List[str] = Field(description="A list of key neutral points or factual statements related to general operations.")
    final_summary: str = Field(description="A brief, conclusive summary of the entity's overall position based on the provided reasons.")

try:
    summary_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    structured_summary_llm = summary_llm.with_structured_output(Summary)
    summary_prompt_template = ChatPromptTemplate.from_messages([
        ("system", """
You are an expert financial analyst. You will be given a list of reasoning snippets from multiple news articles about a specific company or cryptocurrency. Your task is to synthesize these snippets into a clear, structured summary.

Analyze all the provided reasons and categorize the key points into six lists:
1.  **Positive Financial:** Reasons related to stock growth, good earnings, etc.
2.  **Negative Financial:** Reasons related to stock decline, poor earnings, etc.
3.  **Neutral Financial:** Factual financial statements without clear positive or negative sentiment.
4.  **Positive Overall:** Reasons related to successful products, partnerships, good decisions, etc.
5.  **Negative Overall:** Reasons related to failed projects, legal issues, poor decisions, etc.
6.  **Neutral Overall:** Factual statements about operations, announcements, or collaborations without clear positive or negative sentiment.

Finally, provide a brief, one or two-sentence `final_summary` of the entity's overall position based on the balance of the points.

Do not invent new information. Base your summary *only* on the provided reasoning snippets. It is critical that your final JSON object includes all fields, especially `final_summary`.
"""),
        ("human", "Please summarize the following reasoning points for {entity_name}:\n\n{reasoning_list}")
    ])
    summary_chain = summary_prompt_template | structured_summary_llm
except Exception as e:
    print(f"Warning: Could not initialize summarization LLM. The /summarize_entity endpoint will not work. Error: {e}")
    summary_chain = None


# --- API Endpoints ---

@app.route('/')
def home():
    """A simple welcome message and guide for the API root."""
    return jsonify({
        "message": "Welcome to the Financial News Sentiment API.",
        "endpoints": {
            "/api/scrapers": {
                "method": "GET",
                "description": "NEW: Get a list of all available scraper names.",
            },
            "/api/trigger_pipeline": {
                "method": "POST",
                "description": "MODIFIED: Starts scraping and analysis. Can specify which scrapers to run.",
                "body_example": {"password": "your_password", "provider": "openai", "model_name": "gpt-4-turbo", "scrapers": ["zawya.com", "menabytes.com"]}
            },
            "/api/stop_pipeline": {
                "method": "POST",
                "description": "NEW: Requests the running pipeline to stop gracefully. Requires password.",
                "body_example": {"password": "your_password"}
            },
            "/api/configure_schedule": {
                "method": "POST",
                "description": "Sets the daily UTC time for the automated pipeline run. Requires password.",
                "body_example": {"password": "your_password", "schedule_time": "02:30"}
            },
            "/api/pipeline_status": {
                "method": "GET",
                "description": "Returns the real-time status of the currently running pipeline."
            },
            "/api/pipeline_last_run": {
                "method": "GET",
                "description": "Returns the statistics from the most recently completed pipeline run."
            },
            "/api/articles": {
                "method": "GET",
                "description": "Get and filter articles with sentiment data.",
                "params": ["limit", "entity_name", "entity_type", "financial_sentiment", "overall_sentiment"]
            },
            "/api/entities": {
                "method": "GET",
                "description": "Get a list of all unique entities."
            },
            "/api/top_entities": {
                "method": "GET",
                "description": "Get top entities ranked by sentiment count.",
                "params": ["sentiment_type (financial or overall)", "sentiment (positive, negative, neutral)", "order (asc or desc)", "limit"]
            },
            "/api/sentiment_over_time": {
                "method": "GET",
                "description": "Get an entity's sentiment trend over time, formatted for graphing.",
                "params": ["entity_name"]
            },
            "/api/summarize_entity": {
                "method": "GET",
                "description": "Get an AI-generated summary for a specific company or crypto.",
                "params": ["entity_name"]
            },
            "/api/entity_articles_by_sentiment": {
                "method": "GET",
                "description": "Get articles for an entity, grouped by sentiment categories.",
                "params": ["entity_name", "entity_type"]
            },
             "/api/usage_stats": {
                "method": "GET",
                "description": "Get API usage and cost statistics.",
                "params": ["summarize=true"]
            }
        }
    })

@app.route('/api/scrapers', methods=['GET'])
def list_scrapers():
    """Lists the names of all available scraper modules."""
    try:
        scraper_names = scraper_manager.get_all_scraper_names()
        return jsonify(scraper_names)
    except Exception as e:
        return jsonify({"error": "Could not retrieve scraper list.", "details": str(e)}), 500

@app.route('/api/stop_pipeline', methods=['POST'])
def stop_pipeline():
    """Requests the currently running pipeline to stop gracefully."""
    if not pipeline_status_tracker["is_running"]:
        return jsonify({"error": "No pipeline is currently running."}), 404

    data = request.get_json(silent=True) or {}
    # Uncomment the following lines to enforce password protection
    # password = data.get("password")
    # if not password or password != PIPELINE_PASSWORD:
    #     return jsonify({"error": "Unauthorized. A valid password is required."}), 401

    stop_event = pipeline_status_tracker.get("stop_event")
    if stop_event and isinstance(stop_event, threading.Event):
        stop_event.set()
        pipeline_status_tracker["status"] = "Stopping..."
        return jsonify({"message": "Pipeline stop signal sent. It will terminate shortly."}), 202
    
    return jsonify({"error": "Could not send stop signal. The pipeline may be in a state that cannot be interrupted."}), 500

@app.route('/api/trigger_pipeline', methods=['POST'])
def trigger_pipeline():
    """
    Triggers the full data pipeline. Now accepts a list of scrapers to run.
    If 'scrapers' is not provided, it will run all available scrapers.
    """
    if pipeline_status_tracker["is_running"]:
        return jsonify({"error": "A pipeline is already running."}), 409

    data = request.get_json(silent=True) or {}
    # Uncomment the following lines to enforce password protection
    # password = data.get("password")
    # if not password or password != PIPELINE_PASSWORD:
    #     return jsonify({"error": "Unauthorized. A valid password is required."}), 401
    
    # --- Scraper Selection ---
    selected_scrapers = data.get("scrapers") # Can be a list of names or None
    try:
        scraper_modules = scraper_manager.get_scraper_modules(selected_scrapers)
        if not scraper_modules:
            return jsonify({"error": "No valid scrapers found for the given selection."}), 400
    except Exception as e:
        return jsonify({"error": "Failed to load scraper modules.", "details": str(e)}), 500

    # --- AI Config ---
    config = {
        "provider": data.get("provider"), "model_name": data.get("model_name"),
        "openai_api_key": data.get("openai_api_key"), "groq_api_key": data.get("groq_api_key")
    }

    def pipeline_task(app_context, scraper_mods, stop_event, llm_config):
        with app_context:
            pipeline_status_tracker["is_running"] = True
            pipeline_status_tracker["stop_event"] = stop_event
            run_status = "Completed"
            try:
                scraping_stats = pipeline.run_scraping_pipeline(pipeline_status_tracker, scraper_mods, stop_event)
                
                analysis_stats = {}
                if not stop_event.is_set():
                    analysis_stats = pipeline.run_analysis_pipeline(pipeline_status_tracker, stop_event, **llm_config)
                
                if stop_event.is_set():
                    run_status = "Stopped by user"

                final_stats = {**scraping_stats, **analysis_stats, "status": run_status}
                database.add_pipeline_run(final_stats)

            except Exception as e:
                print(f"Pipeline failed: {e}")
                database.add_pipeline_run({"status": f"Failed: {e}"})
            finally:
                # Reset global state
                pipeline_status_tracker.update({
                    "is_running": False, "status": "Idle", "progress": 0, "total": 0,
                    "current_task": "N/A", "stop_event": None
                })

    stop_event = threading.Event()
    thread = threading.Thread(target=pipeline_task, args=(app.app_context(), scraper_modules, stop_event, config))
    thread.daemon = True
    thread.start()

    return jsonify({"message": "Pipeline triggered successfully in the background."}), 202

@app.route('/api/configure_schedule', methods=['POST'])
def configure_schedule():
    """Sets the daily UTC time for the automated pipeline run. Requires a password."""
    data = request.get_json(silent=True) or {}
    # Uncomment the following lines to enforce password protection
    # password = data.get("password")
    # if not password or password != PIPELINE_PASSWORD:
    #     return jsonify({"error": "Unauthorized. A valid password is required."}), 401

    new_time = data.get("schedule_time")
    if not new_time or not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', new_time):
        return jsonify({"error": "Invalid time format. Please use 'HH:MM'."}), 400

    try:
        hour, minute = map(int, new_time.split(':'))
        database.set_config_value('schedule_time', new_time)
        scheduler.reschedule_job('daily_pipeline_job', trigger='cron', hour=hour, minute=minute, timezone='utc')
        return jsonify({"message": f"Pipeline schedule updated successfully to {new_time} UTC."})
    except Exception as e:
        return jsonify({"error": "Failed to update schedule.", "details": str(e)}), 500

@app.route('/api/pipeline_status', methods=['GET'])
def get_pipeline_status():
    """Returns the real-time status of the currently running pipeline."""
    # Create a copy to avoid returning the non-serializable Event object
    status_copy = pipeline_status_tracker.copy()
    status_copy.pop("stop_event", None)
    return jsonify(status_copy)

@app.route('/api/pipeline_last_run', methods=['GET'])
def get_last_run_stats():
    """Returns the statistics from the most recently completed pipeline run."""
    conn = get_db_connection()
    last_run = conn.execute("SELECT * FROM pipeline_runs ORDER BY run_timestamp DESC LIMIT 1").fetchone()
    conn.close()
    if last_run:
        return jsonify(dict(last_run))
    else:
        return jsonify({"message": "No previous pipeline run found."}), 404

@app.route('/api/top_entities', methods=['GET'])
def get_top_entities():
    """Returns a ranked list of entities based on the count of a specific sentiment."""
    sentiment_type = request.args.get('sentiment_type', 'overall')
    sentiment = request.args.get('sentiment', 'positive')
    order = request.args.get('order', 'desc')
    limit = request.args.get('limit', 10, type=int)

    if sentiment_type not in ['financial', 'overall'] or sentiment not in ['positive', 'negative', 'neutral'] or order.upper() not in ['ASC', 'DESC']:
        return jsonify({"error": "Invalid query parameters."}), 400

    sentiment_column = f"{sentiment_type}_sentiment"
    conn = get_db_connection()
    query = f"SELECT entity_name, entity_type, COUNT(*) as sentiment_count FROM sentiments WHERE {sentiment_column} = ? GROUP BY entity_name, entity_type ORDER BY sentiment_count {order} LIMIT ?"
    rows = conn.execute(query, (sentiment, limit)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

@app.route('/api/sentiment_over_time', methods=['GET'])
def get_sentiment_over_time():
    """For a given entity, returns its sentiment scores over time, formatted for graphing."""
    entity_name = request.args.get('entity_name')
    if not entity_name: return jsonify({"error": "An 'entity_name' query parameter is required."}), 400
    conn = get_db_connection()
    query = "SELECT a.publication_date, s.financial_sentiment, s.overall_sentiment FROM sentiments s JOIN articles a ON s.article_id = a.id WHERE s.entity_name LIKE ? ORDER BY a.publication_date ASC"
    rows = conn.execute(query, (f"%{entity_name}%",)).fetchall()
    conn.close()
    if not rows: return jsonify({"error": f"No sentiment data found for entity: {entity_name}"}), 404
    def get_score(sentiment): return 1 if sentiment == 'positive' else -1 if sentiment == 'negative' else 0
    financial_trend = [[row['publication_date'], get_score(row['financial_sentiment'])] for row in rows]
    overall_trend = [[row['publication_date'], get_score(row['overall_sentiment'])] for row in rows]
    return jsonify({"entity_name": entity_name, "financial_sentiment_trend": financial_trend, "overall_sentiment_trend": overall_trend})

@app.route('/api/dashboard_stats', methods=['GET'])
def get_dashboard_stats():
    """Provides a set of key statistics for a dashboard view."""
    conn = get_db_connection()
    total_entities = conn.execute("SELECT COUNT(DISTINCT entity_name) FROM sentiments").fetchone()[0]
    articles_analyzed = conn.execute("SELECT COUNT(DISTINCT article_id) FROM sentiments").fetchone()[0]
    total_sentiments = conn.execute("SELECT COUNT(*) FROM sentiments").fetchone()[0]
    query = """
        SELECT sentiment, COUNT(*) as count
        FROM (
            SELECT financial_sentiment as sentiment FROM sentiments
            UNION ALL
            SELECT overall_sentiment as sentiment FROM sentiments
        )
        GROUP BY sentiment
    """
    dist_rows = conn.execute(query).fetchall()
    conn.close()
    
    distribution = {'positive': 0, 'negative': 0, 'neutral': 0}
    for row in dist_rows:
        if row['sentiment'] in distribution:
            distribution[row['sentiment']] = row['count']

    return jsonify({
        "total_entities": total_entities or 0,
        "articles_analyzed": articles_analyzed or 0,
        "total_sentiment_points": total_sentiments or 0,
        "sentiment_distribution": distribution
    })

@app.route('/api/entity_articles_by_sentiment', methods=['GET'])
def get_entity_articles_by_sentiment():
    """For a given entity, returns a structured list of its associated articles, grouped by sentiment."""
    entity_name = request.args.get('entity_name')
    entity_type = request.args.get('entity_type')
    if not entity_name or not entity_type: return jsonify({"error": "Both 'entity_name' and 'entity_type' query parameters are required."}), 400
    conn = get_db_connection()
    query = "SELECT a.title, a.url, s.reasoning, s.financial_sentiment, s.overall_sentiment FROM sentiments s JOIN articles a ON s.article_id = a.id WHERE s.entity_name LIKE ? AND s.entity_type = ?"
    rows = conn.execute(query, (f"%{entity_name}%", entity_type)).fetchall()
    conn.close()
    if not rows: return jsonify({"error": f"No articles found for entity '{entity_name}' of type '{entity_type}'"}), 404
    response_data = {"positive_financial": [], "negative_financial": [], "neutral_financial": [], "positive_overall": [], "negative_overall": [], "neutral_overall": []}
    for row in rows:
        article_info = {"title": row['title'], "url": row['url'], "reasoning": row['reasoning']}
        if row['financial_sentiment'] == 'positive': response_data["positive_financial"].append(article_info)
        elif row['financial_sentiment'] == 'negative': response_data["negative_financial"].append(article_info)
        else: response_data["neutral_financial"].append(article_info)
        if row['overall_sentiment'] == 'positive': response_data["positive_overall"].append(article_info)
        elif row['overall_sentiment'] == 'negative': response_data["negative_overall"].append(article_info)
        else: response_data["neutral_overall"].append(article_info)
    for key in response_data:
        response_data[key] = [dict(t) for t in {tuple(d.items()) for d in response_data[key]}]
    return jsonify(response_data)

@app.route('/api/summarize_entity', methods=['GET'])
def summarize_entity():
    """Takes an entity name and uses an AI agent to generate a structured summary."""
    entity_name = request.args.get('entity_name')
    if not entity_name: return jsonify({"error": "An 'entity_name' query parameter is required."}), 400
    conn = get_db_connection()
    reasonings = conn.execute("SELECT reasoning, financial_sentiment, overall_sentiment FROM sentiments WHERE entity_name LIKE ?", (f"%{entity_name}%",)).fetchall()
    conn.close()
    if not reasonings: return jsonify({"error": f"No sentiment data found for entity: {entity_name}"}), 404
    reasoning_list_str = "\n".join([f"- (Financial: {r['financial_sentiment']}, Overall: {r['overall_sentiment']}) {r['reasoning']}" for r in reasonings])
    if not summary_chain: return jsonify({"error": "Summarization agent is not available."}), 503
    for attempt in range(3):
        try:
            summary_response = summary_chain.invoke({"entity_name": entity_name, "reasoning_list": reasoning_list_str})
            return jsonify(summary_response.dict())
        except ValidationError as e:
            if attempt >= 2: return jsonify({"error": "Failed to generate a valid summary after multiple attempts.", "details": str(e)}), 500
    return jsonify({"error": "Failed to generate summary."}), 500

@app.route('/api/articles', methods=['GET'])
def get_articles():
    """The main endpoint for fetching and filtering articles."""
    conn = get_db_connection()
    query = "SELECT a.id as article_id, a.title, a.url, a.author, a.publication_date, s.id as sentiment_id, s.entity_name, s.entity_type, s.financial_sentiment, s.overall_sentiment, s.reasoning FROM articles a LEFT JOIN sentiments s ON a.id = s.article_id"
    conditions, params = [], {}
    if request.args.get('entity_name'): conditions.append("s.entity_name LIKE :entity_name"); params['entity_name'] = f"%{request.args.get('entity_name')}%"
    if request.args.get('entity_type'): conditions.append("s.entity_type = :entity_type"); params['entity_type'] = request.args.get('entity_type')
    if request.args.get('financial_sentiment'): conditions.append("s.financial_sentiment = :financial_sentiment"); params['financial_sentiment'] = request.args.get('financial_sentiment')
    if request.args.get('overall_sentiment'): conditions.append("s.overall_sentiment = :overall_sentiment"); params['overall_sentiment'] = request.args.get('overall_sentiment')
    if conditions: query += f" WHERE a.id IN (SELECT DISTINCT article_id FROM sentiments WHERE {' AND '.join(conditions)})"
    query += " ORDER BY a.publication_date DESC"
    limit = request.args.get('limit', 20, type=int)
    query += f" LIMIT {limit}"
    
    rows = conn.execute(query, params).fetchall()
    conn.close()
    
    articles = {}
    for row in rows:
        article_id = row['article_id']
        if article_id not in articles:
            articles[article_id] = {
                "id": article_id, "title": row['title'], "url": row['url'],
                "author": row['author'], "publication_date": row['publication_date'],
                "sentiments": []
            }
        if row['sentiment_id']:
            articles[article_id]['sentiments'].append({
                "entity_name": row['entity_name'], "entity_type": row['entity_type'],
                "financial_sentiment": row['financial_sentiment'],
                "overall_sentiment": row['overall_sentiment'], "reasoning": row['reasoning']
            })
    return jsonify(list(articles.values()))

@app.route('/api/entities', methods=['GET'])
def get_entities():
    """Returns a list of all unique entities found."""
    conn = get_db_connection()
    entities = conn.execute("SELECT DISTINCT entity_name, entity_type FROM sentiments ORDER BY entity_name").fetchall()
    conn.close()
    return jsonify([dict(row) for row in entities])

@app.route('/api/usage_stats', methods=['GET'])
def get_usage_stats():
    """Returns API usage and cost statistics."""
    summarize = request.args.get('summarize', 'false').lower() == 'true'
    conn = get_db_connection()
    if summarize: query = "SELECT provider, COUNT(*) as total_calls, SUM(total_tokens) as total_tokens, SUM(total_cost_usd) as total_cost FROM usage_logs GROUP BY provider"
    else: query = "SELECT * FROM usage_logs ORDER BY timestamp DESC"
    stats = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(row) for row in stats])


# --- Scheduler Setup ---
def scheduled_pipeline_run():
    """A wrapper for the scheduler to run the pipeline with all available scrapers."""
    with app.app_context():
        if pipeline_status_tracker["is_running"]:
            print("Scheduled run skipped: A pipeline is already in progress.")
            return

        print(f"--- Scheduled pipeline run started at {datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
        
        stop_event = threading.Event()
        try:
            scraper_modules = scraper_manager.get_scraper_modules() # All scrapers
            if not scraper_modules:
                print("Scheduled run aborted: No scrapers found.")
                return
        except Exception as e:
            print(f"Scheduled run failed during scraper discovery: {e}")
            return

        pipeline_status_tracker["is_running"] = True
        pipeline_status_tracker["stop_event"] = stop_event
        run_status = "Completed"
        try:
            scraping_stats = pipeline.run_scraping_pipeline(pipeline_status_tracker, scraper_modules, stop_event)
            analysis_stats = pipeline.run_analysis_pipeline(pipeline_status_tracker, stop_event) # Default LLM config
            final_stats = {**scraping_stats, **analysis_stats, "status": run_status}
            database.add_pipeline_run(final_stats)
        except Exception as e:
            print(f"Scheduled pipeline failed: {e}")
            database.add_pipeline_run({"status": f"Failed: {e}"})
        finally:
            pipeline_status_tracker.update({
                "is_running": False, "status": "Idle", "progress": 0, "total": 0,
                "current_task": "N/A", "stop_event": None
            })

# --- Main Execution ---
if __name__ == '__main__':
    database.create_database()
    scraper_manager.discover_scrapers() # Pre-discover on startup
    
    scheduler = BackgroundScheduler(daemon=True)
    schedule_time_str = database.get_config_value('schedule_time', '01:00')
    hour, minute = map(int, schedule_time_str.split(':'))
    scheduler.add_job(scheduled_pipeline_run, 'cron', hour=hour, minute=minute, timezone='utc', id='daily_pipeline_job')
    scheduler.start()
    
    print(f"Pipeline scheduler started. Next run scheduled for {schedule_time_str} UTC daily.")
    print(f"Available scrapers found: {scraper_manager.get_all_scraper_names()}")
    app.run(debug=True, use_reloader=False)
