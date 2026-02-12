"""
Slack API client using httpx for direct API calls.
Handles thread creation and message posting with proper error handling.
"""
import os
import logging
from typing import Dict
from dotenv import load_dotenv
import httpx

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# Slack API configuration
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SALES_CHANNEL_ID = os.getenv("SLACK_SALES_CHANNEL_ID")
# Fallback to SLACK_CHANNEL_ID for backward compatibility
SLACK_CHANNEL_ID = os.getenv("SLACK_SALES_CHANNEL_ID") or os.getenv("SLACK_CHANNEL_ID")
HTTP_TIMEOUT = 30.0  # seconds

if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN must be set in .env file")
if not SLACK_CHANNEL_ID:
    raise ValueError("SLACK_SALES_CHANNEL_ID (or SLACK_CHANNEL_ID) must be set in .env file")


class SlackClientError(Exception):
    """Custom exception for Slack API errors"""
    pass


async def create_thread(user_id: str) -> str:
    """
    Create a new Slack thread by posting an initial message.
    
    Args:
        user_id: Unique identifier for the user
        
    Returns:
        thread_ts: Thread timestamp from Slack response
        
    Raises:
        SlackClientError: If Slack API call fails or returns ok != true
    """
    text = f"New Website Conversation\nUser ID: {user_id}"
    
    logger.info(f"Creating Slack thread for user_id={user_id}")
    
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": text
    }
    
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(
                SLACK_API_URL,
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Validate Slack response
            if not data.get("ok", False):
                error_msg = data.get("error", "Unknown error")
                error_detail = f"Slack API error: {error_msg}, response: {data}"
                logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_detail}")
                raise SlackClientError(f"Slack API returned ok=false: {error_msg}")
            
            thread_ts = data.get("ts")
            if not thread_ts:
                error_msg = f"Slack response missing 'ts' field: {data}"
                logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_msg}")
                raise SlackClientError(error_msg)
            
            logger.info(f"Successfully created Slack thread for user_id={user_id}, thread_ts={thread_ts}")
            return thread_ts
            
    except httpx.TimeoutException as e:
        error_msg = f"Timeout after {HTTP_TIMEOUT}s while creating Slack thread"
        logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_msg}: {e}")
        raise SlackClientError(error_msg) from e
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code} error: {e.response.text}"
        logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_msg}")
        raise SlackClientError(error_msg) from e
        
    except httpx.RequestError as e:
        error_msg = f"Request error: {str(e)}"
        logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_msg}")
        raise SlackClientError(error_msg) from e
        
    except SlackClientError:
        # Re-raise SlackClientError as-is (already logged)
        raise
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Failed to create Slack thread for user_id={user_id}. {error_msg}", exc_info=True)
        raise SlackClientError(error_msg) from e


async def post_message(thread_ts: str, text: str) -> Dict:
    """
    Post a message to an existing Slack thread.
    
    Args:
        thread_ts: Thread timestamp to post to
        text: Message text to post
        
    Returns:
        Response data from Slack API
        
    Raises:
        SlackClientError: If Slack API call fails or returns ok != true
    """
    logger.info(f"Posting message to thread_ts={thread_ts}, text_length={len(text)}")
    
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": text,
        "thread_ts": thread_ts
    }
    
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(
                SLACK_API_URL,
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Validate Slack response
            if not data.get("ok", False):
                error_msg = data.get("error", "Unknown error")
                error_detail = f"Slack API error: {error_msg}, response: {data}"
                logger.error(f"Failed to post message to thread_ts={thread_ts}. {error_detail}")
                raise SlackClientError(f"Slack API returned ok=false: {error_msg}")
            
            logger.info(f"Successfully posted message to thread_ts={thread_ts}")
            return data
            
    except httpx.TimeoutException as e:
        error_msg = f"Timeout after {HTTP_TIMEOUT}s while posting message"
        logger.error(f"Failed to post message to thread_ts={thread_ts}. {error_msg}: {e}")
        raise SlackClientError(error_msg) from e
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code} error: {e.response.text}"
        logger.error(f"Failed to post message to thread_ts={thread_ts}. {error_msg}")
        raise SlackClientError(error_msg) from e
        
    except httpx.RequestError as e:
        error_msg = f"Request error: {str(e)}"
        logger.error(f"Failed to post message to thread_ts={thread_ts}. {error_msg}")
        raise SlackClientError(error_msg) from e
        
    except SlackClientError:
        # Re-raise SlackClientError as-is (already logged)
        raise
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Failed to post message to thread_ts={thread_ts}. {error_msg}", exc_info=True)
        raise SlackClientError(error_msg) from e

