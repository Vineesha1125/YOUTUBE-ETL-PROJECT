import os
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

def get_youtube_client():
    """Initialize YouTube API client"""
    api_key = os.getenv('YOUTUBE_API_KEY')
    
    if not api_key:
        raise ValueError("❌ API key not found! Make sure .env file exists with YOUTUBE_API_KEY")
    
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)

def fetch_trending_videos(region_code='US', max_results=50):
    """Fetch trending videos from YouTube"""
    print(f"Fetching trending videos for region: {region_code}")
    
    youtube = get_youtube_client()
    
    try:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            chart='mostPopular',
            regionCode=region_code,
            maxResults=max_results
        )
        
        response = request.execute()
        
        videos_data = []
        
        for item in response['items']:
            video_data = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'channel_name': item['snippet']['channelTitle'],
                'channel_id': item['snippet']['channelId'],
                'published_at': item['snippet']['publishedAt'],
                'category_id': item['snippet']['categoryId'],
                'tags': ','.join(item['snippet'].get('tags', [])),
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0)),
                'duration': item['contentDetails']['duration'],
                'region_code': region_code,
                'trending_date': datetime.now().strftime('%Y-%m-%d'),
                'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            videos_data.append(video_data)
        
        df = pd.DataFrame(videos_data)
        return df
    
    except Exception as e:
        print(f"❌ Error fetching videos: {str(e)}")
        raise

# Test the function
# Test the function
# Test the function
# Test the function
# Test the function
if __name__ == "__main__":
    import os
    
    print("Starting YouTube data extraction...")
    
    # Extract data
    df = fetch_trending_videos()
    print(f"✓ Successfully extracted {len(df)} videos")
    print("\nFirst 3 videos:")
    print(df[['title', 'channel_name', 'view_count']].head(3))
    
    # Get absolute path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    filename = os.path.join(project_dir, 'data', 'raw', f'youtube_raw_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    print(f"\nSaving to: {filename}")
    
    # Open file directly and write (bypasses pandas path check)
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        df.to_csv(f, index=False)
    
    print(f"✓ Data saved successfully!")
    print(f"✓ File size: {os.path.getsize(filename)} bytes")