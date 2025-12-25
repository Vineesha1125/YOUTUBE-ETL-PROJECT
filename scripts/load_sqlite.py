import sqlite3
import pandas as pd
import os
from datetime import datetime

def create_database():
    """Create SQLite database and tables"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    db_path = os.path.join(project_dir, 'youtube_analytics.db')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.executescript('''
        DROP TABLE IF EXISTS trending_data;
        DROP TABLE IF EXISTS videos;
        DROP TABLE IF EXISTS categories;
        
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL
        );
        
        INSERT INTO categories VALUES
        (1, 'Film & Animation'),
        (2, 'Autos & Vehicles'),
        (10, 'Music'),
        (15, 'Pets & Animals'),
        (17, 'Sports'),
        (20, 'Gaming'),
        (22, 'People & Blogs'),
        (23, 'Comedy'),
        (24, 'Entertainment'),
        (25, 'News & Politics'),
        (26, 'Howto & Style'),
        (27, 'Education'),
        (28, 'Science & Technology');
        
        CREATE TABLE videos (
            video_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            category_id INTEGER,
            published_at TEXT NOT NULL,
            duration_minutes REAL,
            tags TEXT
        );
        
        CREATE TABLE trending_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            trending_date TEXT NOT NULL,
            region_code TEXT NOT NULL,
            view_count INTEGER NOT NULL,
            like_count INTEGER NOT NULL,
            comment_count INTEGER NOT NULL,
            engagement_rate REAL,
            like_rate REAL,
            comment_rate REAL,
            days_to_trend INTEGER,
            extracted_at TEXT NOT NULL,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        );
    ''')
    
    conn.commit()
    print(f"Database created: {db_path}")
    return conn

def load_data(df, conn):
    """Load data to SQLite"""
    # Videos data
    videos_df = df[[
        'video_id', 'title', 'channel_id', 'channel_name', 
        'category_id', 'published_at', 'duration_minutes', 'tags'
    ]].drop_duplicates(subset=['video_id'])
    
    videos_df.to_sql('videos', conn, if_exists='append', index=False)
    print(f"Loaded {len(videos_df)} videos")
    
    # Trending data
    trending_df = df[[
        'video_id', 'trending_date', 'region_code', 'view_count',
        'like_count', 'comment_count', 'engagement_rate', 
        'like_rate', 'comment_rate', 'days_to_trend', 'extracted_at'
    ]]
    
    trending_df.to_sql('trending_data', conn, if_exists='append', index=False)
    print(f"Loaded {len(trending_df)} trending records")

if __name__ == "__main__":
    print("=" * 60)
    print("YOUTUBE DATA LOADER (SQLite)")
    print("=" * 60)
    
    # Get transformed data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    transformed_dir = os.path.join(project_dir, 'data', 'transformed')
    
    transformed_files = [f for f in os.listdir(transformed_dir) if f.endswith('.csv')]
    latest_file = sorted(transformed_files)[-1]
    input_path = os.path.join(transformed_dir, latest_file)
    
    print(f"\nReading: {latest_file}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} records")
    
    # Create database and load
    print("\nCreating database...")
    conn = create_database()
    
    print("\nLoading data...")
    load_data(df, conn)
    
    # Verify
    cursor = conn.cursor()
    video_count = cursor.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    trending_count = cursor.execute("SELECT COUNT(*) FROM trending_data").fetchone()[0]
    
    print(f"\nTotal videos: {video_count}")
    print(f"Total trending records: {trending_count}")
    
    conn.close()
    print("\nData loading complete!")