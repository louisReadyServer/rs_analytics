#!/usr/bin/env python3
"""
Test Twitter API v2 Read-Only Access with Bearer Token Only

This script tests read-only access to Twitter API using only a bearer token
(App-only authentication). This does NOT require OAuth 1.0a credentials.

Bearer token can access:
- Public user lookup (by username or ID)
- Public tweets lookup
- Tweet search (limited on free tier)

Bearer token CANNOT access:
- get_me() endpoint (requires user context)
- Non-public metrics (impressions for your own tweets)
- Direct messages
- Private user information

Usage:
    # Set bearer token as environment variable
    set TWITTER_BEARER_TOKEN=your_token_here
    python scripts/test_twitter_bearer_only.py username_to_lookup
    
    # Or pass directly (less secure)
    python scripts/test_twitter_bearer_only.py username_to_lookup --token YOUR_TOKEN
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_bearer_token(bearer_token: str, username: str) -> None:
    """
    Test Twitter API v2 read-only access with bearer token.
    
    Args:
        bearer_token: Twitter API v2 bearer token
        username: Twitter username to lookup (without @)
    """
    try:
        import tweepy
    except ImportError:
        print("[X] tweepy is not installed. Run: pip install tweepy")
        return
    
    # Remove @ if present
    username = username.lstrip('@')
    
    print("=" * 60)
    print("Twitter API v2 - Bearer Token Read-Only Test")
    print("=" * 60)
    print(f"\nToken: {bearer_token[:20]}...{bearer_token[-10:]}")
    print(f"Looking up: @{username}")
    print()
    
    # Create client with bearer token only (no OAuth 1.0a)
    client = tweepy.Client(
        bearer_token=bearer_token,
        wait_on_rate_limit=True
    )
    
    # ========================================
    # Test 1: User Lookup by Username
    # ========================================
    print("Test 1: User Lookup")
    print("-" * 40)
    
    try:
        user = client.get_user(
            username=username,
            user_fields=[
                'id', 'name', 'username', 'created_at', 
                'description', 'location', 'public_metrics',
                'profile_image_url', 'verified', 'verified_type'
            ]
        )
        
        if user.data:
            data = user.data
            metrics = data.public_metrics or {}
            
            print(f"  [OK] User found!")
            print(f"  Name: {data.name}")
            print(f"  Username: @{data.username}")
            print(f"  ID: {data.id}")
            print(f"  Description: {(data.description or 'N/A')[:60]}...")
            print(f"  Location: {getattr(data, 'location', 'N/A') or 'N/A'}")
            print(f"  Verified: {getattr(data, 'verified', False)}")
            print(f"  Followers: {metrics.get('followers_count', 0):,}")
            print(f"  Following: {metrics.get('following_count', 0):,}")
            print(f"  Tweets: {metrics.get('tweet_count', 0):,}")
            print(f"  Listed: {metrics.get('listed_count', 0):,}")
            
            user_id = data.id
        else:
            print(f"  [X] User @{username} not found")
            return
            
    except tweepy.errors.Unauthorized as e:
        print(f"  [X] Unauthorized: Invalid bearer token")
        print(f"      Error: {e}")
        return
    except tweepy.errors.NotFound as e:
        print(f"  [X] User @{username} not found")
        return
    except Exception as e:
        error_str = str(e)
        if "402" in error_str or "Payment Required" in error_str:
            print(f"  [!] Token is VALID but account has no API credits!")
            print()
            print("  Your Twitter Developer account has run out of free tier credits.")
            print("  This is a billing issue, not a token issue.")
            print()
            print("  Options:")
            print("  1. Wait for monthly credit reset")
            print("  2. Upgrade at developer.twitter.com (Basic: $100/mo)")
            print("  3. Check usage at developer.twitter.com/en/portal/dashboard")
        else:
            print(f"  [X] Error: {e}")
        return
    
    print()
    
    # ========================================
    # Test 2: Fetch Recent Tweets (Public)
    # ========================================
    print("Test 2: Recent Tweets (public metrics)")
    print("-" * 40)
    
    try:
        tweets = client.get_users_tweets(
            id=user_id,
            max_results=5,
            tweet_fields=['id', 'text', 'created_at', 'public_metrics', 'lang'],
            exclude=['retweets']
        )
        
        if tweets.data:
            print(f"  [OK] Found {len(tweets.data)} recent tweets")
            print()
            
            for i, tweet in enumerate(tweets.data, 1):
                metrics = tweet.public_metrics or {}
                text_preview = tweet.text[:50].replace('\n', ' ') + "..." if len(tweet.text) > 50 else tweet.text.replace('\n', ' ')
                
                print(f"  Tweet {i}:")
                print(f"    Text: {text_preview}")
                print(f"    Created: {tweet.created_at}")
                print(f"    Likes: {metrics.get('like_count', 0):,}")
                print(f"    Retweets: {metrics.get('retweet_count', 0):,}")
                print(f"    Replies: {metrics.get('reply_count', 0):,}")
                print(f"    Impressions: {metrics.get('impression_count', 'N/A (requires auth)')}")
                print()
        else:
            print(f"  [i] No recent tweets found for @{username}")
            
    except tweepy.errors.Unauthorized as e:
        print(f"  [X] Unauthorized: {e}")
    except Exception as e:
        print(f"  [X] Error fetching tweets: {e}")
    
    # ========================================
    # Summary
    # ========================================
    print("=" * 60)
    print("BEARER TOKEN TEST COMPLETE")
    print("=" * 60)
    print("""
Note: Bearer token (App-only auth) provides READ-ONLY access to:
  - Public user profiles
  - Public tweets with public_metrics
  - Tweet search (limited on free tier)

For full metrics (impressions, profile clicks, etc.), you need:
  - OAuth 1.0a credentials (consumer key/secret, access token/secret)
  - The tweets must be from your own account
""")


def main():
    parser = argparse.ArgumentParser(
        description="Test Twitter API v2 with bearer token only"
    )
    parser.add_argument(
        "username",
        nargs="?",
        default="Twitter",
        help="Twitter username to lookup (without @)"
    )
    parser.add_argument(
        "--token",
        type=str,
        help="Bearer token (or set TWITTER_BEARER_TOKEN env var)"
    )
    
    args = parser.parse_args()
    
    # Get bearer token from args or environment
    bearer_token = args.token or os.getenv("TWITTER_BEARER_TOKEN")
    
    if not bearer_token:
        print("[X] No bearer token provided!")
        print()
        print("Usage:")
        print("  set TWITTER_BEARER_TOKEN=your_token_here")
        print("  python scripts/test_twitter_bearer_only.py username")
        print()
        print("Or:")
        print("  python scripts/test_twitter_bearer_only.py username --token YOUR_TOKEN")
        sys.exit(1)
    
    test_bearer_token(bearer_token, args.username)


if __name__ == "__main__":
    main()
