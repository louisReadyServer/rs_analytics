"""
Twitter/X Configuration Module

Handles loading and validation of Twitter API credentials
for organic page analytics extraction.

Twitter API v2 endpoints used:
- Users lookup (profile metrics)
- Tweets lookup (tweet metrics)
- User tweets timeline
"""

import os
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from etl.utils import load_env_file, resolve_path

# Load environment variables from project root
load_env_file()


class TwitterConfigurationError(Exception):
    """Raised when Twitter configuration is invalid or missing."""
    pass


@dataclass
class TwitterConfig:
    """
    Twitter API configuration container.
    
    Attributes:
        bearer_token: OAuth 2.0 Bearer Token for app-only auth
        consumer_key: OAuth 1.0a Consumer Key (API Key)
        consumer_secret: OAuth 1.0a Consumer Secret (API Secret)
        access_token: OAuth 1.0a Access Token
        access_token_secret: OAuth 1.0a Access Token Secret
        client_id: OAuth 2.0 Client ID (optional)
        client_secret: OAuth 2.0 Client Secret (optional)
        username: Twitter username to track (without @)
        duckdb_path: Path to DuckDB database file
    """
    bearer_token: str
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str
    username: str
    duckdb_path: Path
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Remove @ from username if present
        if self.username.startswith('@'):
            self.username = self.username[1:]
        
        # Validate required fields
        if not self.bearer_token:
            raise TwitterConfigurationError("TWITTER_BEARER_TOKEN is required")
        if not self.consumer_key:
            raise TwitterConfigurationError("TWITTER_CONSUMER_KEY is required")
        if not self.consumer_secret:
            raise TwitterConfigurationError("TWITTER_CONSUMER_SECRET is required")
        if not self.access_token:
            raise TwitterConfigurationError("TWITTER_ACCESS_TOKEN is required")
        if not self.access_token_secret:
            raise TwitterConfigurationError("TWITTER_ACCESS_TOKEN_SECRET is required")
        if not self.username:
            raise TwitterConfigurationError("TWITTER_USERNAME is required")


def get_twitter_config() -> TwitterConfig:
    """
    Load Twitter configuration from environment variables.
    
    Returns:
        TwitterConfig object with all credentials
        
    Raises:
        TwitterConfigurationError: If required configuration is missing
    """
    # Check if Twitter is enabled
    enabled = os.getenv("ENABLE_TWITTER", "0")
    if enabled not in ("1", "true", "True", "yes"):
        raise TwitterConfigurationError(
            "Twitter integration is disabled. Set ENABLE_TWITTER=1 in .env"
        )
    
    # Load credentials
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN", "")
    consumer_key = os.getenv("TWITTER_CONSUMER_KEY", "")
    consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET", "")
    access_token = os.getenv("TWITTER_ACCESS_TOKEN", "")
    access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")
    client_id = os.getenv("TWITTER_CLIENT_ID", "")
    client_secret = os.getenv("TWITTER_CLIENT_SECRET", "")
    username = os.getenv("TWITTER_USERNAME", "")
    
    # Resolve DuckDB path
    duckdb_path = resolve_path(os.getenv("DUCKDB_PATH", None), "data/warehouse.duckdb")
    
    return TwitterConfig(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        client_id=client_id if client_id else None,
        client_secret=client_secret if client_secret else None,
        username=username,
        duckdb_path=duckdb_path
    )


def get_twitter_client():
    """
    Initialize and return a Tweepy Client for Twitter API v2.
    
    Returns:
        tweepy.Client configured with credentials
        
    Raises:
        TwitterConfigurationError: If credentials are invalid
        ImportError: If tweepy is not installed
    """
    try:
        import tweepy
    except ImportError:
        raise ImportError(
            "tweepy is required for Twitter integration. "
            "Install with: pip install tweepy"
        )
    
    config = get_twitter_config()
    
    # Create client with both OAuth 1.0a and Bearer token
    # This allows access to both user-context and app-only endpoints
    client = tweepy.Client(
        bearer_token=config.bearer_token,
        consumer_key=config.consumer_key,
        consumer_secret=config.consumer_secret,
        access_token=config.access_token,
        access_token_secret=config.access_token_secret,
        wait_on_rate_limit=True  # Automatically handle rate limits
    )
    
    return client


def validate_twitter_connection() -> tuple[bool, str]:
    """
    Test Twitter API connection and credentials.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        import tweepy
        
        config = get_twitter_config()
        client = get_twitter_client()
        
        # Test by fetching the authenticated user's info
        me = client.get_me(user_fields=['public_metrics', 'description', 'created_at'])
        
        if me.data:
            username = me.data.username
            name = me.data.name
            followers = me.data.public_metrics.get('followers_count', 0)
            return True, f"Connected as @{username} ({name}) - {followers:,} followers"
        else:
            return False, "Could not retrieve user information"
            
    except tweepy.errors.Unauthorized as e:
        return False, f"Authentication failed: Invalid credentials - {e}"
    except tweepy.errors.Forbidden as e:
        return False, f"Access forbidden: {e}"
    except TwitterConfigurationError as e:
        return False, f"Configuration error: {e}"
    except Exception as e:
        return False, f"Connection error: {e}"


if __name__ == "__main__":
    """Test configuration when run directly."""
    print("Testing Twitter configuration...")
    
    try:
        config = get_twitter_config()
        print(f"  Username: @{config.username}")
        print(f"  Bearer token: {config.bearer_token[:20]}...")
        print(f"  DuckDB path: {config.duckdb_path}")
        
        print("\nTesting connection...")
        success, message = validate_twitter_connection()
        if success:
            print(f"  ✅ {message}")
        else:
            print(f"  ❌ {message}")
            
    except TwitterConfigurationError as e:
        print(f"  ❌ Configuration error: {e}")
    except Exception as e:
        print(f"  ❌ Error: {e}")
