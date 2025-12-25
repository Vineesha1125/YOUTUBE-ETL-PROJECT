from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def get_database_connection():
    """Create database connection using SQLAlchemy"""
    db_user = os.getenv('DB_USER', 'root')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'youtube_analytics')
    
    if not db_password:
        raise ValueError("Database password not found in .env file")
    
    # Create connection string
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    print(f"Connecting to database: {db_name} on {db_host}:{db_port}")
    
    try:
        engine = create_engine(connection_string, echo=False)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful")
        return engine
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        raise

def load_to_database(df, engine):
    """Load transformed data to database"""
    
    print("\n" + "=" * 60)
    print("LOADING DATA TO DATABASE")
    print("=" * 60)
    
    # Separate videos and trending data
    print("\nPreparing data for loading...")
    
    # Videos table data
    videos_df = df[[
        'video_id', 'title', 'channel_id', 'channel_name', 
        'category_id', 'published_at', 'duration_minutes', 'tags'
    ]].drop_duplicates(subset=['video_id'])
    
    print(f"- Videos to load: {len(videos_df)}")
    
    # Trending data table
    trending_df = df[[
        'video_id', 'trending_date', 'region_code', 'view_count',
        'like_count', 'comment_count', 'engagement_rate', 
        'like_rate', 'comment_rate', 'days_to_trend', 'extracted_at'
    ]]
    
    print(f"- Trending records to load: {len(trending_df)}")
    
    # Load to database
    try:
        print("\nLoading videos to database...")
        videos_df.to_sql('videos', engine, if_exists='append', index=False, method='multi')
        print(f"Loaded {len(videos_df)} videos")
        
        print("\nLoading trending data to database...")
        trending_df.to_sql('trending_data', engine, if_exists='append', index=False, method='multi')
        print(f"Loaded {len(trending_df)} trending records")
        
        return True
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return False

def verify_data(engine):
    """Verify data was loaded correctly"""
    print("\n" + "=" * 60)
    print("VERIFYING DATA")
    print("=" * 60)
    
    try:
        with engine.connect() as conn:
            # Count videos
            result = conn.execute(text("SELECT COUNT(*) as count FROM videos"))
            video_count = result.fetchone()[0]
            print(f"Total videos in database: {video_count}")
            
            # Count trending records
            result = conn.execute(text("SELECT COUNT(*) as count FROM trending_data"))
            trending_count = result.fetchone()[0]
            print(f"Total trending records: {trending_count}")
            
            # Show top 5 videos by views
            print("\nTop 5 videos by views:")
            query = """
                SELECT v.title, v.channel_name, t.view_count, t.engagement_rate
                FROM videos v
                JOIN trending_data t ON v.video_id = t.video_id
                ORDER BY t.view_count DESC
                LIMIT 5
            """
            result = conn.execute(text(query))
            for row in result:
                print(f"  - {row[0][:50]}... | Views: {row[2]:,} | Engagement: {row[3]:.2f}%")
        
        return True
    except Exception as e:
        print(f"Verification failed: {str(e)}")
        return False

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("YOUTUBE DATA LOADER")
    print("=" * 60)
    
    # Get paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    transformed_dir = os.path.join(project_dir, 'data', 'transformed')
    
    # Get the most recent transformed file
    transformed_files = [f for f in os.listdir(transformed_dir) if f.endswith('.csv')]
    
    if not transformed_files:
        print("No transformed data files found in data/transformed/")
        print("Run transform.py first!")
        exit()
    
    # Get latest file
    latest_file = sorted(transformed_files)[-1]
    input_path = os.path.join(transformed_dir, latest_file)
    
    print(f"\nInput file: {latest_file}")
    
    # Read transformed data
    print("Reading transformed data...")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} records")
    
    # Connect to database
    print()
    engine = get_database_connection()
    
    # Load data
    success = load_to_database(df, engine)
    
    if success:
        # Verify
        verify_data(engine)
        print("\nData loading complete!\n")
    else:
        print("\nData loading failed!\n")