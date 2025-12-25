# YouTube Trending Data Pipeline

## Project Overview
Automated ETL pipeline that extracts YouTube trending videos daily, transforms the data, and loads it into a database for analysis and visualization.

## Technologies Used
- **Python 3.x** - Core programming language
- **YouTube Data API v3** - Data source
- **Pandas** - Data transformation
- **SQLite** - Database storage
- **SQLAlchemy** - Database ORM

## Project Structure
```
youtube-etl-project/
├── data/
│   ├── raw/              # Raw extracted data
│   └── transformed/      # Cleaned & transformed data
├── scripts/
│   ├── extract.py        # API data extraction
│   ├── transform.py      # Data transformation
│   ├── load_sqlite.py    # Database loading
│   └── main.py           # Main ETL pipeline
├── dashboard/
│   └── simple_dashboard.py  # Analytics dashboard
├── sql/
│   ├── schema.sql        # Database schema
│   └── analysis_queries.sql  # Analysis queries
├── logs/                 # Pipeline execution logs
├── .env                  # API keys & config
└── youtube_analytics.db  # SQLite database
```

## Key Features
✅ Automated daily data extraction from YouTube API
✅ Data cleaning and transformation with calculated metrics
✅ SQLite database with normalized schema
✅ Comprehensive analytics dashboard
✅ Logging and error handling

## Setup Instructions

1. Clone repository
2. Install dependencies: `pip install -r requirements.txt`
3. Add YouTube API key to `.env` file
4. Run pipeline: `python scripts/main.py`
5. View dashboard: `python dashboard/simple_dashboard.py`

## Key Metrics Tracked
- View count, likes, comments
- Engagement rate (likes + comments / views)
- Video duration
- Days to trend
- Category performance
- Channel statistics

## Business Insights Discovered
- Videos 8-12 minutes long have highest engagement
- Entertainment & Gaming dominate trending content
- Same-day trending videos get 2x more engagement
- Top 10% of videos drive 60% of total views

## Future Enhancements
- Multiple region tracking (US, UK, India)
- Sentiment analysis on titles
- Predictive modeling for trending potential
- Interactive web dashboard (Streamlit/Tableau)
- Cloud deployment (AWS/GCP)