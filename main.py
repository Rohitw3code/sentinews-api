# main.py

import database
import pipeline
from scrapers import scraper_manager
import threading
from typing import Dict, Any

def main():
    """
    Main function to run the full data pipeline from the command line.
    This script will:
    1. Initialize the database.
    2. Discover all available scrapers.
    3. Run the scraping pipeline to gather new article links and content.
    4. Run the analysis pipeline to process new articles for sentiment.
    """
    print("--- Starting Command-Line Pipeline Execution ---")

    # 1. Initialize the database
    database.create_database()

    # 2. Discover all available scrapers
    print("\nDiscovering scraper modules...")
    scraper_modules = scraper_manager.get_scraper_modules()
    if not scraper_modules:
        print("No scraper modules found. Exiting.")
        return
    
    scraper_names = [getattr(s, 'SOURCE_NAME', 'Unknown') for s in scraper_modules]
    print(f"Found {len(scraper_modules)} scrapers: {', '.join(scraper_names)}")

    # 3. Set up components for the pipeline run
    # The status_tracker is used by the API, but we create a dummy one here
    # for compatibility with the pipeline functions.
    status_tracker: Dict[str, Any] = {}
    
    # The stop_event allows for graceful shutdown, not used in this simple script
    # but required by the function signature.
    stop_event = threading.Event()

    # 4. Run the scraping pipeline
    print("\n" + "="*20 + " STAGE 1: SCRAPING " + "="*20)
    pipeline.run_scraping_pipeline(status_tracker, scraper_modules, stop_event)

    # 5. Run the analysis pipeline
    print("\n" + "="*20 + " STAGE 2: ANALYSIS " + "="*20)
    # This runs with the default AI provider and model settings from .env
    pipeline.run_analysis_pipeline(status_tracker, stop_event)

    print("\n" + "="*50)
    print("--- Command-Line Pipeline Execution Complete ---")
    print("="*50)


if __name__ == "__main__":
    main()
