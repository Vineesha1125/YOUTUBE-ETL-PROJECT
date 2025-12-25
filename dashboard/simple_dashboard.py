import sqlite3
import pandas as pd
import os

def run_query(query, db_path):
    """Execute SQL query and return DataFrame"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def display_dashboard():
    """Display simple text-based dashboard"""
    
    # Get database path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    db_path = os.path.join(project_dir, 'youtube_analytics.db')
    
    if not os.path.exists(db_path):
        print("❌ Database not found. Run main.py first!")
        return
    
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 25 + "YOUTUBE TRENDING ANALYTICS DASHBOARD" + " " * 17 + "║")
    print("╚" + "═" * 78 + "╝")
    print("\n")
    
    # 1. Summary Statistics
    print("=" * 80)
    print("📊 SUMMARY STATISTICS")
    print("=" * 80)
    
    query = """
        SELECT 
            COUNT(DISTINCT video_id) as total_videos,
            COUNT(*) as total_records,
            AVG(view_count) as avg_views,
            MAX(view_count) as max_views,
            AVG(engagement_rate) as avg_engagement
        FROM trending_data
    """
    df = run_query(query, db_path)
    
    print(f"Total Unique Videos: {df['total_videos'].values[0]}")
    print(f"Total Records: {df['total_records'].values[0]}")
    print(f"Average Views: {df['avg_views'].values[0]:,.0f}")
    print(f"Max Views: {df['max_views'].values[0]:,.0f}")
    print(f"Average Engagement Rate: {df['avg_engagement'].values[0]:.2f}%")
    
    # 2. Top 10 Videos
    print("\n" + "=" * 80)
    print("🏆 TOP 10 MOST VIEWED VIDEOS")
    print("=" * 80)
    
    query = """
        SELECT 
            v.title,
            v.channel_name,
            t.view_count,
            t.engagement_rate
        FROM videos v
        JOIN trending_data t ON v.video_id = t.video_id
        ORDER BY t.view_count DESC
        LIMIT 10
    """
    df = run_query(query, db_path)
    
    for idx, row in df.iterrows():
        title = row['title'][:60] + "..." if len(row['title']) > 60 else row['title']
        print(f"{idx+1:2d}. {title}")
        print(f"    Channel: {row['channel_name']} | Views: {row['view_count']:,} | Engagement: {row['engagement_rate']:.2f}%")
    
    # 3. Category Performance
    print("\n" + "=" * 80)
    print("📈 CATEGORY PERFORMANCE")
    print("=" * 80)
    
    query = """
        SELECT 
            c.category_name,
            COUNT(DISTINCT v.video_id) as video_count,
            AVG(t.view_count) as avg_views,
            AVG(t.engagement_rate) as avg_engagement
        FROM categories c
        JOIN videos v ON c.category_id = v.category_id
        JOIN trending_data t ON v.video_id = t.video_id
        GROUP BY c.category_name
        ORDER BY avg_engagement DESC
        LIMIT 8
    """
    df = run_query(query, db_path)
    
    print(f"{'Category':<25} {'Videos':<10} {'Avg Views':<15} {'Engagement':<12}")
    print("-" * 80)
    for _, row in df.iterrows():
        print(f"{row['category_name']:<25} {row['video_count']:<10} {row['avg_views']:>12,.0f}   {row['avg_engagement']:>10.2f}%")
    
    # 4. Duration Analysis
    print("\n" + "=" * 80)
    print("⏱️  DURATION vs ENGAGEMENT")
    print("=" * 80)
    
    query = """
        SELECT 
            CASE 
                WHEN v.duration_minutes < 5 THEN '0-5 min'
                WHEN v.duration_minutes < 10 THEN '5-10 min'
                WHEN v.duration_minutes < 20 THEN '10-20 min'
                ELSE '20+ min'
            END as duration,
            COUNT(*) as videos,
            AVG(t.engagement_rate) as avg_engagement
        FROM videos v
        JOIN trending_data t ON v.video_id = t.video_id
        GROUP BY duration
        ORDER BY avg_engagement DESC
    """
    df = run_query(query, db_path)
    
    print(f"{'Duration':<15} {'Videos':<10} {'Avg Engagement':<15}")
    print("-" * 80)
    for _, row in df.iterrows():
        print(f"{row['duration']:<15} {row['videos']:<10} {row['avg_engagement']:>12.2f}%")
    
    # 5. Top Channels
    print("\n" + "=" * 80)
    print("🌟 TOP PERFORMING CHANNELS")
    print("=" * 80)
    
    query = """
        SELECT 
            v.channel_name,
            COUNT(DISTINCT v.video_id) as videos,
            AVG(t.view_count) as avg_views,
            AVG(t.engagement_rate) as avg_engagement
        FROM videos v
        JOIN trending_data t ON v.video_id = t.video_id
        GROUP BY v.channel_name
        ORDER BY avg_engagement DESC
        LIMIT 10
    """
    df = run_query(query, db_path)
    
    print(f"{'Channel':<30} {'Videos':<10} {'Avg Views':<15} {'Engagement':<12}")
    print("-" * 80)
    for _, row in df.iterrows():
        channel = row['channel_name'][:28] if len(row['channel_name']) > 28 else row['channel_name']
        print(f"{channel:<30} {row['videos']:<10} {row['avg_views']:>12,.0f}   {row['avg_engagement']:>10.2f}%")
    
    print("\n" + "=" * 80)
    print("✓ Dashboard complete!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    display_dashboard()