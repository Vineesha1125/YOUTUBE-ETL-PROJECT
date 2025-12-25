-- Create database
CREATE DATABASE IF NOT EXISTS youtube_analytics;
USE youtube_analytics;

-- Drop existing tables if they exist (for clean restart)
DROP TABLE IF EXISTS trending_data;
DROP TABLE IF EXISTS videos;
DROP TABLE IF EXISTS categories;

-- Categories lookup table
CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

-- Insert YouTube category mappings
INSERT INTO categories (category_id, category_name) VALUES
(1, 'Film & Animation'),
(2, 'Autos & Vehicles'),
(10, 'Music'),
(15, 'Pets & Animals'),
(17, 'Sports'),
(19, 'Travel & Events'),
(20, 'Gaming'),
(22, 'People & Blogs'),
(23, 'Comedy'),
(24, 'Entertainment'),
(25, 'News & Politics'),
(26, 'Howto & Style'),
(27, 'Education'),
(28, 'Science & Technology'),
(29, 'Nonprofits & Activism');

-- Videos table (stores unique video information)
CREATE TABLE videos (
    video_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    channel_id VARCHAR(50) NOT NULL,
    channel_name VARCHAR(200) NOT NULL,
    category_id INT,
    published_at DATETIME NOT NULL,
    duration_minutes DECIMAL(10,2),
    tags TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    INDEX idx_channel (channel_name),
    INDEX idx_category (category_id),
    INDEX idx_published (published_at)
);

-- Trending data table (time-series data)
CREATE TABLE trending_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(20) NOT NULL,
    trending_date DATE NOT NULL,
    region_code VARCHAR(5) NOT NULL,
    view_count BIGINT NOT NULL,
    like_count INT NOT NULL,
    comment_count INT NOT NULL,
    engagement_rate DECIMAL(10,4),
    like_rate DECIMAL(10,4),
    comment_rate DECIMAL(10,4),
    days_to_trend INT,
    extracted_at TIMESTAMP NOT NULL,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE,
    UNIQUE KEY unique_trending (video_id, trending_date, region_code),
    INDEX idx_trending_date (trending_date),
    INDEX idx_region (region_code),
    INDEX idx_views (view_count)
);