import pandas as pd
import re
from datetime import datetime
import os

def clean_text(text):
    """Remove special characters and clean text"""
    if pd.isna(text):
        return ''
    # Remove emojis and special characters (keep letters, numbers, spaces, basic punctuation)
    text = re.sub(r'[^\w\s,.\-!?]', '', str(text))
    return text.strip()

def parse_duration(duration):
    """Convert ISO 8601 duration to minutes"""
    try:
        # Handle PT format (e.g., PT10M30S, PT1H5M, PT45S)
        duration = duration.replace('PT', '')
        
        hours = 0
        minutes = 0
        seconds = 0
        
        if 'H' in duration:
            hours = int(duration.split('H')[0])
            duration = duration.split('H')[1]
        
        if 'M' in duration:
            minutes = int(duration.split('M')[0])
            duration = duration.split('M')[1] if 'M' in duration else ''
        
        if 'S' in duration and duration:
            seconds = int(duration.replace('S', ''))
        
        total_minutes = hours * 60 + minutes + seconds / 60
        return round(total_minutes, 2)
    except:
        return 0

def calculate_engagement_rate(row):
    """Calculate engagement rate"""
    if row['view_count'] == 0:
        return 0
    return round(((row['like_count'] + row['comment_count']) / row['view_count']) * 100, 4)

def transform_data(df):
    """Apply all transformations"""
    df_clean = df.copy()
    
    print("Applying transformations...")
    
    # Clean text fields
    print("- Cleaning text fields...")
    df_clean['title'] = df_clean['title'].apply(clean_text)
    df_clean['channel_name'] = df_clean['channel_name'].apply(clean_text)
    
    # Parse duration
    print("- Parsing video durations...")
    df_clean['duration_minutes'] = df_clean['duration'].apply(parse_duration)
    
    # Calculate metrics
    print("- Calculating engagement metrics...")
    df_clean['engagement_rate'] = df_clean.apply(calculate_engagement_rate, axis=1)
    df_clean['like_rate'] = round((df_clean['like_count'] / df_clean['view_count'] * 100).fillna(0), 4)
    df_clean['comment_rate'] = round((df_clean['comment_count'] / df_clean['view_count'] * 100).fillna(0), 4)
    
    # Convert dates (FIX: Make both timezone-aware)
    print("- Converting date formats...")
    df_clean['published_at'] = pd.to_datetime(df_clean['published_at'], utc=True)
    df_clean['trending_date'] = pd.to_datetime(df_clean['trending_date'], utc=True)
    
    # Calculate days to trend
    df_clean['days_to_trend'] = (df_clean['trending_date'] - df_clean['published_at']).dt.days
    
    # Remove duplicates
    print("- Removing duplicates...")
    original_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['video_id', 'trending_date'])
    duplicates_removed = original_count - len(df_clean)
    print(f"  Removed {duplicates_removed} duplicate records")
    
    # Handle missing values
    df_clean = df_clean.fillna({
        'like_count': 0,
        'comment_count': 0,
        'tags': ''
    })
    
    return df_clean

# Test
if __name__ == "__main__":
    print("=" * 60)
    print("YOUTUBE DATA TRANSFORMATION")
    print("=" * 60)
    
    # Get paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    raw_dir = os.path.join(project_dir, 'data', 'raw')
    transformed_dir = os.path.join(project_dir, 'data', 'transformed')
    
    # Create transformed directory if it doesn't exist
    if not os.path.exists(transformed_dir):
        os.makedirs(transformed_dir)
    
    # Get the most recent raw file
    raw_files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
    
    if not raw_files:
        print("❌ No raw data files found in data/raw/")
        print("   Run extract.py first!")
        exit()
    
    # Sort by filename (which includes timestamp) and get the latest
    latest_file = sorted(raw_files)[-1]
    input_path = os.path.join(raw_dir, latest_file)
    
    print(f"\nInput file: {latest_file}")
    print(f"Full path: {input_path}")
    
    # Read raw data
    print("\nReading raw data...")
    df_raw = pd.read_csv(input_path)
    print(f"✓ Loaded {len(df_raw)} records")
    
    # Transform
    print()
    df_transformed = transform_data(df_raw)
    
    # Save transformed data
    output_filename = f'youtube_transformed_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = os.path.join(transformed_dir, output_filename)
    
    print(f"\nSaving transformed data...")
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        df_transformed.to_csv(f, index=False)
    
    print(f"✓ Saved to: {output_path}")
    print(f"✓ File size: {os.path.getsize(output_path)} bytes")
    
    # Summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION SUMMARY")
    print("=" * 60)
    print(f"Original records:    {len(df_raw)}")
    print(f"Transformed records: {len(df_transformed)}")
    print(f"New columns added:   duration_minutes, engagement_rate, like_rate, comment_rate, days_to_trend")
    
    print("\n" + "=" * 60)
    print("SAMPLE TRANSFORMED DATA")
    print("=" * 60)
    print(df_transformed[['title', 'view_count', 'engagement_rate', 'duration_minutes', 'days_to_trend']].head(5))
    
    print("\n" + "=" * 60)
    print("TOP 5 VIDEOS BY ENGAGEMENT")
    print("=" * 60)
    top_engagement = df_transformed.nlargest(5, 'engagement_rate')[['title', 'channel_name', 'view_count', 'engagement_rate']]
    print(top_engagement.to_string(index=False))
    
    print("\n✓ Transformation complete!\n")