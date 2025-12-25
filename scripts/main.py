import os
import sys
from datetime import datetime
import logging

# Add scripts directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

from extract import fetch_trending_videos
from transform import transform_data

# Setup logging with UTF-8 encoding
log_dir = os.path.join(os.path.dirname(script_dir), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f'etl_{datetime.now().strftime("%Y%m%d")}.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_to_sqlite(df):
    """Load data to SQLite database with duplicate handling"""
    import sqlite3
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    db_path = os.path.join(project_dir, 'youtube_analytics.db')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    videos_loaded = 0
    videos_skipped = 0
    trending_loaded = 0
    
    # Load videos (skip duplicates)
    videos_df = df[[
        'video_id', 'title', 'channel_id', 'channel_name', 
        'category_id', 'published_at', 'duration_minutes', 'tags'
    ]].drop_duplicates(subset=['video_id'])
    
    for _, row in videos_df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO videos 
                (video_id, title, channel_id, channel_name, category_id, published_at, duration_minutes, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(row))
            if cursor.rowcount > 0:
                videos_loaded += 1
            else:
                videos_skipped += 1
        except Exception as e:
            logging.warning(f"Error inserting video {row['video_id']}: {str(e)}")
    
    # Load trending data
    trending_df = df[[
        'video_id', 'trending_date', 'region_code', 'view_count',
        'like_count', 'comment_count', 'engagement_rate', 
        'like_rate', 'comment_rate', 'days_to_trend', 'extracted_at'
    ]]
    
    for _, row in trending_df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO trending_data 
                (video_id, trending_date, region_code, view_count, like_count, comment_count, 
                 engagement_rate, like_rate, comment_rate, days_to_trend, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(row))
            trending_loaded += 1
        except Exception as e:
            logging.warning(f"Error inserting trending data: {str(e)}")
    
    conn.commit()
    conn.close()
    
    logging.info(f"Videos: {videos_loaded} new, {videos_skipped} already exist")
    logging.info(f"Trending records: {trending_loaded} loaded")
    
    return True

def run_etl_pipeline(region='US', max_results=50):
    """Run the complete ETL pipeline"""
    
    logging.info("=" * 60)
    logging.info("STARTING ETL PIPELINE")
    logging.info("=" * 60)
    
    try:
        # EXTRACT
        logging.info("PHASE 1: Extracting data from YouTube API...")
        df_raw = fetch_trending_videos(region_code=region, max_results=max_results)
        logging.info(f"Extracted {len(df_raw)} videos from {region}")
        
        # TRANSFORM
        logging.info("\nPHASE 2: Transforming data...")
        df_transformed = transform_data(df_raw)
        logging.info(f"Transformation complete: {len(df_transformed)} records")
        
        # LOAD
        logging.info("\nPHASE 3: Loading data to database...")
        success = load_to_sqlite(df_transformed)
        
        if success:
            logging.info("\n" + "=" * 60)
            logging.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logging.info("=" * 60)
            return True
        else:
            logging.error("ETL pipeline failed at loading stage")
            return False
        
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Set console to UTF-8
    if sys.platform == 'win32':
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    
    print("\n")
    print("=" * 60)
    print("       YOUTUBE TRENDING ETL PIPELINE")
    print("=" * 60)
    print("\n")
    
    # Run the pipeline
    success = run_etl_pipeline(region='US', max_results=50)
    
    if success:
        print("\nPipeline execution complete! Check logs for details.\n")
    else:
        print("\nPipeline execution failed! Check logs for errors.\n")