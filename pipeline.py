# pipeline.py

import database
from analysis.sentiment_analyzer import SentimentAnalyzer
import threading
from typing import List, Dict, Any

def run_scraping_pipeline(status_tracker: Dict[str, Any], scraper_modules: List[Any], stop_event: threading.Event) -> Dict[str, int]:
    """
    Executes the scraping part of the data pipeline. It now accepts a list
    of scraper modules to run and a stop event for graceful termination.

    Args:
        status_tracker: A dictionary to update the real-time status of the pipeline.
        scraper_modules: A list of imported scraper modules to execute.
        stop_event: A threading.Event object to signal when to stop the process.

    Returns:
        A dictionary containing statistics about the scraping run.
    """
    # --- Step 1: Scrape Links ---
    status_tracker.update({
        'status': 'Scraping links', 'progress': 0, 'total': len(scraper_modules),
        'current_task': 'Fetching article lists from sources.'
    })
    
    new_links_found = 0
    for i, scraper in enumerate(scraper_modules):
        if stop_event.is_set():
            print("Stop request received. Halting link scraping.")
            status_tracker['status'] = 'Stopping...'
            return {'new_links_found': new_links_found, 'articles_scraped': 0}
        
        source_name = getattr(scraper, 'SOURCE_NAME', 'Unknown Scraper')
        print(f"\nRunning scraper for: {source_name}")
        status_tracker['current_task'] = f"Fetching links from {source_name}"
        
        try:
            urls = scraper.get_article_urls()
            if not urls: 
                print(f"No links found for {source_name}.")
                continue
            for url in urls:
                if database.add_link(url=url, source=source_name):
                    new_links_found += 1
        except Exception as e:
            print(f"Error running scraper {source_name}: {e}")

        status_tracker['progress'] = i + 1
    
    print(f"Finished scraping links. Found {new_links_found} new URLs.")

    # --- Step 2: Scrape Articles ---
    if stop_event.is_set():
        return {'new_links_found': new_links_found, 'articles_scraped': 0}

    links_to_scrape = database.get_unscraped_links()
    status_tracker.update({
        'status': 'Scraping articles', 'progress': 0, 'total': len(links_to_scrape)
    })
    
    articles_scraped_count = 0
    if not links_to_scrape:
        status_tracker['current_task'] = 'No new articles to scrape.'
    else:
        # Create a mapping from source name to scraper module for efficient lookup
        scraper_map = {getattr(s, 'SOURCE_NAME', 'Unknown'): s for s in scraper_modules}
        for i, link in enumerate(links_to_scrape):
            if stop_event.is_set():
                print("Stop request received. Halting article scraping.")
                status_tracker['status'] = 'Stopping...'
                break # Exit the loop gracefully

            scraper_to_use = scraper_map.get(link['source'])
            if scraper_to_use:
                try:
                    article_data = scraper_to_use.scrape_article_content(link['url'])
                    if article_data:
                        database.add_article(link_id=link['id'], article_data=article_data)
                        articles_scraped_count += 1
                        status_tracker['current_task'] = f"Scraped: {article_data.get('title', 'N/A')}"
                except Exception as e:
                    print(f"Error scraping content from {link['url']}: {e}")
            status_tracker['progress'] = i + 1

    print(f"Finished scraping articles. Scraped {articles_scraped_count} new articles.")
    return {'new_links_found': new_links_found, 'articles_scraped': articles_scraped_count}

def run_analysis_pipeline(status_tracker: Dict[str, Any], stop_event: threading.Event, **kwargs: Any) -> Dict[str, int]:
    """
    Executes the analysis part of the pipeline. It now accepts a stop_event
    for graceful termination.

    Args:
        status_tracker: A dictionary to update the real-time status of the pipeline.
        stop_event: A threading.Event object to signal when to stop the process.
        **kwargs: Configuration for the SentimentAnalyzer (provider, model_name, api keys).

    Returns:
        A dictionary containing statistics about the analysis run.
    """
    try:
        analyzer = SentimentAnalyzer(**kwargs)
    except Exception as e:
        print(f"Failed to initialize SentimentAnalyzer: {e}")
        status_tracker['status'] = f"Error: {e}"
        return {'entities_analyzed': 0}

    articles_to_analyze = database.get_unanalyzed_articles()
    
    status_tracker.update({
        'status': 'Analyzing sentiment', 'progress': 0, 'total': len(articles_to_analyze)
    })
    
    sentiments_found_count = 0
    total_session_cost = 0.0
    
    if not articles_to_analyze:
        status_tracker['current_task'] = 'No new articles to analyze.'
    else:
        for i, article in enumerate(articles_to_analyze):
            if stop_event.is_set():
                print("Stop request received. Halting analysis.")
                status_tracker['status'] = 'Stopping...'
                break # Exit the loop gracefully

            status_tracker['current_task'] = f"Analyzing article ID: {article['id']}"
            try:
                entities_list, usage_stats = analyzer.analyze_text_for_sentiment(article['text'])
                
                if usage_stats:
                    database.add_usage_log(article['id'], analyzer.provider, usage_stats)
                    total_session_cost += usage_stats.get('total_cost_usd', 0.0)

                if entities_list:
                    for entity in entities_list:
                        database.add_sentiment(
                            article_id=article['id'], entity_name=entity.entity_name,
                            entity_type=entity.entity_type, financial_sentiment=entity.financial_sentiment,
                            overall_sentiment=entity.overall_sentiment, reasoning=entity.reasoning
                        )
                        sentiments_found_count += 1
                
                database.mark_article_as_analyzed(article['id'])
            except Exception as e:
                print(f"Error analyzing article ID {article['id']}: {e}")

            status_tracker['progress'] = i + 1
            
    print(f"\nFinished sentiment analysis. Found {sentiments_found_count} new sentiment records.")
    print(f"Total estimated cost for this session: ${total_session_cost:.6f} USD")
    return {'entities_analyzed': sentiments_found_count}
