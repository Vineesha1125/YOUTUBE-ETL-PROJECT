-- YouTube Analytics - Key Analysis Queries

-- 1. Top 10 Most Viewed Videos
SELECT 
    v.title,
    v.channel_name,
    t.view_count,
    t.like_count,
    t.engagement_rate,
    t.trending_date
FROM videos v
JOIN trending_data t ON v.video_id = t.video_id
ORDER BY t.view_count DESC
LIMIT 10;

-- 2. Category Performance Analysis
SELECT 
    c.category_name,
    COUNT(DISTINCT v.video_id) as video_count,
    AVG(t.view_count) as avg_views,
    AVG(t.engagement_rate) as avg_engagement,
    SUM(t.view_count) as total_views
FROM categories c
JOIN videos v ON c.category_id = v.category_id
JOIN trending_data t ON v.video_id = t.video_id
GROUP BY c.category_name
ORDER BY avg_engagement DESC;

-- 3. Top Channels by Performance
SELECT 
    v.channel_name,
    COUNT(DISTINCT v.video_id) as trending_videos,
    AVG(t.view_count) as avg_views,
    AVG(t.engagement_rate) as avg_engagement,
    SUM(t.like_count) as total_likes
FROM videos v
JOIN trending_data t ON v.video_id = t.video_id
GROUP BY v.channel_name
HAVING trending_videos >= 2
ORDER BY avg_engagement DESC
LIMIT 15;

-- 4. Video Duration vs Engagement
SELECT 
    CASE 
        WHEN v.duration_minutes < 5 THEN '0-5 min'
        WHEN v.duration_minutes < 10 THEN '5-10 min'
        WHEN v.duration_minutes < 20 THEN '10-20 min'
        WHEN v.duration_minutes < 40 THEN '20-40 min'
        ELSE '40+ min'
    END as duration_bucket,
    COUNT(*) as video_count,
    AVG(t.view_count) as avg_views,
    AVG(t.engagement_rate) as avg_engagement
FROM videos v
JOIN trending_data t ON v.video_id = t.video_id
GROUP BY duration_bucket
ORDER BY avg_engagement DESC;

-- 5. Time to Trend Analysis
SELECT 
    CASE 
        WHEN t.days_to_trend = 0 THEN 'Same Day'
        WHEN t.days_to_trend <= 1 THEN '1 Day'
        WHEN t.days_to_trend <= 3 THEN '2-3 Days'
        WHEN t.days_to_trend <= 7 THEN '4-7 Days'
        ELSE '7+ Days'
    END as time_to_trend,
    COUNT(*) as video_count,
    AVG(t.view_count) as avg_views,
    AVG(t.engagement_rate) as avg_engagement
FROM trending_data t
GROUP BY time_to_trend
ORDER BY avg_views DESC;

-- 6. Engagement Metrics Summary
SELECT 
    AVG(view_count) as avg_views,
    AVG(like_count) as avg_likes,
    AVG(comment_count) as avg_comments,
    AVG(engagement_rate) as avg_engagement_rate,
    AVG(like_rate) as avg_like_rate,
    AVG(comment_rate) as avg_comment_rate
FROM trending_data;

-- 7. Daily Trending Pattern
SELECT 
    DATE(trending_date) as date,
    COUNT(*) as videos_trending,
    AVG(view_count) as avg_views,
    AVG(engagement_rate) as avg_engagement
FROM trending_data
GROUP BY DATE(trending_date)
ORDER BY date DESC;