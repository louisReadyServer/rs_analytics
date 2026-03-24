"""Twitter/X Dashboard - Organic analytics with profile metrics, engagement, and tweet performance."""

import streamlit as st
import pandas as pd

from app.components.date_picker import render_date_range_picker
from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import load_duckdb_data, check_twitter_data_exists


def render_twitter_dashboard(twitter_config, duckdb_path: str):
    """
    Render the Twitter/X organic analytics dashboard.
    
    Displays:
    - Profile metrics (followers, following, tweets)
    - Tweet performance (impressions, engagements)
    - Daily metrics trends
    - Top performing tweets
    """
    
    st.header("🐦 Twitter/X - Page Analytics Dashboard")
    
    # Check if data exists
    has_data, total_rows, twitter_tables = check_twitter_data_exists(duckdb_path)
    
    if not has_data or total_rows == 0:
        st.info("""
        **No Twitter data available yet.**
        
        To populate data:
        1. Ensure Twitter API credentials are configured in `.env`
        2. Run the ETL: `python scripts/run_etl_twitter.py`
        
        **Note:** Twitter API requires a paid subscription ($100+/month) for read access.
        """)
        
        # Show ETL instructions
        with st.expander("📋 Setup Instructions"):
            st.markdown("""
            ### Twitter API Setup
            
            1. **Get API Access**: Sign up at [developer.twitter.com](https://developer.twitter.com)
            2. **Subscribe to Basic tier** ($100/month) for read access
            3. **Configure credentials** in `.env`:
               ```
               ENABLE_TWITTER=1
               TWITTER_BEARER_TOKEN=your_bearer_token
               TWITTER_CONSUMER_KEY=your_consumer_key
               TWITTER_CONSUMER_SECRET=your_consumer_secret
               TWITTER_ACCESS_TOKEN=your_access_token
               TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
               TWITTER_USERNAME=YourUsername
               ```
            4. **Test connection**: `python scripts/test_twitter_connection.py`
            5. **Run ETL**: `python scripts/run_etl_twitter.py`
            """)
        return
    
    # Date range filter using calendar picker
    start_date, end_date, _, _ = render_date_range_picker(
        key="twitter_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=False
    )
    
    # Convert to string for SQL
    date_cutoff = start_date.strftime('%Y-%m-%d')
    
    # ========================================
    # SECTION 1: PROFILE OVERVIEW
    # ========================================
    st.subheader("👤 Profile Overview")
    
    if 'twitter_profile' in twitter_tables:
        profile_query = """
        SELECT *
        FROM twitter_profile
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
        profile_df = load_duckdb_data(duckdb_path, profile_query)
        
        if profile_df is not None and not profile_df.empty:
            row = profile_df.iloc[0]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("👥 Followers", f"{int(row['followers_count']):,}")
            with col2:
                st.metric("➡️ Following", f"{int(row['following_count']):,}")
            with col3:
                st.metric("📝 Total Tweets", f"{int(row['tweet_count']):,}")
            with col4:
                st.metric("📋 Listed", f"{int(row['listed_count']):,}")
            with col5:
                verified = "✅ Yes" if row.get('verified', False) else "❌ No"
                st.metric("✓ Verified", verified)
            
            # Profile info
            with st.expander("📄 Profile Details"):
                st.markdown(f"**Username:** @{row['username']}")
                st.markdown(f"**Name:** {row['name']}")
                st.markdown(f"**Bio:** {row.get('description', 'N/A')}")
                st.markdown(f"**Location:** {row.get('location', 'N/A')}")
                st.markdown(f"**Account Created:** {row.get('created_at', 'N/A')[:10] if row.get('created_at') else 'N/A'}")
                st.markdown(f"**Last Updated:** {row['snapshot_date']}")
    
    st.divider()
    
    # ========================================
    # SECTION 2: ENGAGEMENT METRICS
    # ========================================
    st.subheader("📊 Engagement Metrics")
    
    if 'twitter_tweets' in twitter_tables:
        # Aggregate metrics
        metrics_query = f"""
        SELECT 
            COUNT(*) as total_tweets,
            SUM(impressions) as total_impressions,
            SUM(likes) as total_likes,
            SUM(retweets) as total_retweets,
            SUM(replies) as total_replies,
            SUM(quotes) as total_quotes,
            SUM(bookmarks) as total_bookmarks,
            SUM(likes + retweets + replies + quotes + bookmarks) as total_engagements,
            AVG(likes) as avg_likes,
            AVG(retweets) as avg_retweets
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        """
        
        metrics_df = load_duckdb_data(duckdb_path, metrics_query)
        
        if metrics_df is not None and not metrics_df.empty:
            row = metrics_df.iloc[0]
            
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                st.metric("📝 Tweets", f"{int(row['total_tweets'] or 0):,}")
            with col2:
                st.metric("👁️ Impressions", f"{int(row['total_impressions'] or 0):,}")
            with col3:
                st.metric("❤️ Likes", f"{int(row['total_likes'] or 0):,}")
            with col4:
                st.metric("🔄 Retweets", f"{int(row['total_retweets'] or 0):,}")
            with col5:
                st.metric("💬 Replies", f"{int(row['total_replies'] or 0):,}")
            with col6:
                engagements = int(row['total_engagements'] or 0)
                impressions = int(row['total_impressions'] or 1)
                engagement_rate = (engagements / impressions * 100) if impressions > 0 else 0
                st.metric("📈 Eng. Rate", f"{engagement_rate:.2f}%")
    
    st.divider()
    
    # ========================================
    # SECTION 3: DAILY TRENDS
    # ========================================
    st.subheader("📈 Daily Performance Trends")
    
    if 'twitter_daily_metrics' in twitter_tables:
        trend_query = f"""
        SELECT 
            date,
            tweet_count,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            total_engagements,
            engagement_rate
        FROM twitter_daily_metrics
        WHERE date >= '{date_cutoff}'
        ORDER BY date
        """
        
        trend_df = load_duckdb_data(duckdb_path, trend_query)
        
        if trend_df is not None and not trend_df.empty:
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Impressions", "❤️ Engagements", "📝 Tweets", "📈 Eng. Rate"])
            
            with tab1:
                st.bar_chart(trend_df.set_index('date')['impressions'], use_container_width=True)
                st.caption("Daily Impressions")
            
            with tab2:
                # Stacked engagement chart
                eng_df = trend_df.set_index('date')[['likes', 'retweets', 'replies', 'quotes']]
                st.bar_chart(eng_df, use_container_width=True)
                st.caption("Daily Engagements by Type")
            
            with tab3:
                st.bar_chart(trend_df.set_index('date')['tweet_count'], use_container_width=True)
                st.caption("Daily Tweet Count")
            
            with tab4:
                st.line_chart(trend_df.set_index('date')['engagement_rate'], use_container_width=True)
                st.caption("Daily Engagement Rate (%)")
        else:
            st.info("No daily metrics data available for the selected period.")
    
    st.divider()
    
    # ========================================
    # SECTION 4: TOP PERFORMING TWEETS
    # ========================================
    st.subheader("🏆 Top Performing Tweets")
    
    if 'twitter_tweets' in twitter_tables:
        col1, col2 = st.columns(2)
        
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                options=["impressions", "likes", "retweets", "replies", "total_engagement"],
                format_func=lambda x: {
                    "impressions": "👁️ Impressions",
                    "likes": "❤️ Likes",
                    "retweets": "🔄 Retweets",
                    "replies": "💬 Replies",
                    "total_engagement": "📊 Total Engagement"
                }.get(x, x),
                key="twitter_sort"
            )
        
        with col2:
            tweet_type_filter = st.selectbox(
                "Tweet Type",
                options=["All", "original", "reply", "quote"],
                format_func=lambda x: {
                    "All": "All Tweets",
                    "original": "Original Tweets",
                    "reply": "Replies",
                    "quote": "Quote Tweets"
                }.get(x, x),
                key="twitter_type"
            )
        
        # Build query
        type_filter = f"AND tweet_type = '{tweet_type_filter}'" if tweet_type_filter != "All" else ""
        
        if sort_by == "total_engagement":
            order_by = "(likes + retweets + replies + quotes + bookmarks)"
        else:
            order_by = sort_by
        
        top_tweets_query = f"""
        SELECT 
            tweet_id,
            text,
            tweet_type,
            created_date,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            bookmarks,
            (likes + retweets + replies + quotes + bookmarks) as total_engagement
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}' {type_filter}
        ORDER BY {order_by} DESC
        LIMIT 10
        """
        
        top_df = load_duckdb_data(duckdb_path, top_tweets_query)
        
        if top_df is not None and not top_df.empty:
            for _, tweet in top_df.iterrows():
                with st.container():
                    # Tweet header
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        text = tweet['text']
                        if len(text) > 200:
                            text = text[:200] + "..."
                        st.markdown(f"**{tweet['created_date']}** • _{tweet['tweet_type']}_")
                        st.markdown(text)
                    
                    with col2:
                        st.caption(f"👁️ {int(tweet['impressions'] or 0):,}")
                    
                    # Metrics row
                    mcol1, mcol2, mcol3, mcol4, mcol5 = st.columns(5)
                    mcol1.caption(f"❤️ {int(tweet['likes']):,}")
                    mcol2.caption(f"🔄 {int(tweet['retweets']):,}")
                    mcol3.caption(f"💬 {int(tweet['replies']):,}")
                    mcol4.caption(f"📝 {int(tweet['quotes']):,}")
                    mcol5.caption(f"🔖 {int(tweet['bookmarks']):,}")
                    
                    st.divider()
        else:
            st.info("No tweets found for the selected filters.")
    
    st.divider()
    
    # ========================================
    # SECTION 5: TWEET TYPE BREAKDOWN
    # ========================================
    st.subheader("📊 Tweet Type Analysis")
    
    if 'twitter_tweets' in twitter_tables:
        type_query = f"""
        SELECT 
            tweet_type,
            COUNT(*) as count,
            SUM(impressions) as impressions,
            SUM(likes) as likes,
            SUM(retweets) as retweets,
            AVG(impressions) as avg_impressions,
            AVG(likes) as avg_likes
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        GROUP BY tweet_type
        ORDER BY count DESC
        """
        
        type_df = load_duckdb_data(duckdb_path, type_query)
        
        if type_df is not None and not type_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📊 Tweet Count by Type**")
                st.bar_chart(type_df.set_index('tweet_type')['count'])
            
            with col2:
                st.markdown("**📈 Avg Engagement by Type**")
                type_df['avg_engagement'] = type_df['avg_likes'] + (type_df['impressions'] / type_df['count'].replace(0, 1) * 0.01)
                st.bar_chart(type_df.set_index('tweet_type')['avg_likes'])
    
    st.divider()
    
    # ========================================
    # SECTION 6: RAW DATA EXPLORER
    # ========================================
    with st.expander("🔍 Raw Data Explorer"):
        table_to_view = st.selectbox(
            "Select Table",
            options=twitter_tables,
            key="twitter_raw_table"
        )
        
        if table_to_view:
            raw_query = f"SELECT * FROM {table_to_view} ORDER BY 1 DESC LIMIT 100"
            raw_df = load_duckdb_data(duckdb_path, raw_query)
            
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True, hide_index=True)
                st.caption(f"Showing up to 100 rows from {table_to_view}")
