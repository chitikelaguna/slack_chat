"""
Minimal bi-directional chat bridge: Website (WebSocket) ⇄ FastAPI ⇄ Slack Channel (Threads)
"""
import os
import logging
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import json
from . import mapping

# Load environment variables from .env file
# Get the project root directory (parent of app directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections per user_id.
    Supports multiple simultaneous users and server-side push to specific users.
    """
    
    def __init__(self):
        """Initialize connection manager with in-memory storage."""
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str) -> None:
        """
        Accept a new WebSocket connection and store it.
        
        Args:
            websocket: The WebSocket connection
            user_id: Unique identifier for the user
        """
        await websocket.accept()
        self.active_connections[user_id] = websocket
        logger.info(f"WebSocket connected: user_id={user_id} (total connections: {len(self.active_connections)})")
    
    def disconnect(self, user_id: str) -> None:
        """
        Remove a WebSocket connection.
        
        Args:
            user_id: Unique identifier for the user
        """
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            logger.info(f"WebSocket disconnected: user_id={user_id} (remaining connections: {len(self.active_connections)})")
    
    async def send_personal_message(self, message: dict, user_id: str) -> bool:
        """
        Send a message to a specific user via their WebSocket connection.
        
        Args:
            message: Dictionary to send as JSON
            user_id: Unique identifier for the user
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        connection = self.active_connections.get(user_id)
        if connection is None:
            logger.warning(f"No active connection found for user_id={user_id}")
            return False
        
        try:
            await connection.send_json(message)
            logger.debug(f"Sent message to user_id={user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to user_id={user_id}: {e}", exc_info=True)
            # Remove stale connection
            self.disconnect(user_id)
            return False
    
    def is_connected(self, user_id: str) -> bool:
        """
        Check if a user has an active connection.
        
        Args:
            user_id: Unique identifier for the user
            
        Returns:
            True if user is connected, False otherwise
        """
        return user_id in self.active_connections
    
    def get_connection_count(self) -> int:
        """
        Get the number of active connections.
        
        Returns:
            Number of active WebSocket connections
        """
        return len(self.active_connections)
    
    def get_connected_users(self) -> list:
        """
        Get list of all connected user IDs.
        
        Returns:
            List of user IDs with active connections
        """
        return list(self.active_connections.keys())


# FastAPI app
app = FastAPI(title="Slack Chat Bridge")

# Mount static files
# Get the project root directory (parent of app directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
static_dir = os.path.join(project_root, "static")
try:
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
except Exception as e:
    logger.warning(f"Could not mount static files: {e}")

# Connection manager instance
connection_manager = ConnectionManager()

# Slack configuration - loaded from .env file
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_APP_ID = os.getenv("SLACK_APP_ID")
SLACK_SALES_CHANNEL_ID = os.getenv("SLACK_SALES_CHANNEL_ID")
# Fallback to SLACK_CHANNEL_ID for backward compatibility
SLACK_CHANNEL_ID = os.getenv("SLACK_SALES_CHANNEL_ID") or os.getenv("SLACK_CHANNEL_ID")

# Validate required environment variables
if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN must be set in .env file")
if not SLACK_CHANNEL_ID:
    raise ValueError("SLACK_SALES_CHANNEL_ID (or SLACK_CHANNEL_ID) must be set in .env file")

logger.info(f"Loaded Slack configuration: APP_ID={SLACK_APP_ID}, CHANNEL_ID={SLACK_CHANNEL_ID}")

# Slack Web Client
slack_client = WebClient(token=SLACK_BOT_TOKEN)

# Get bot user ID to filter out our own messages
try:
    bot_info = slack_client.auth_test()
    BOT_USER_ID = bot_info.get("user_id")
    logger.info(f"Bot user ID: {BOT_USER_ID}")
except SlackApiError as e:
    logger.error(f"Failed to get bot user ID: {e}")
    BOT_USER_ID = None

# Use mapping module for thread-safe storage (imported above)


class ChatSendRequest(BaseModel):
    """Request model for POST /chat/send"""
    user_id: str
    message: str


@app.get("/")
async def root():
    """Root endpoint - serves the chat interface"""
    # Serve the chat.html file directly
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    chat_html_path = os.path.join(project_root, "static", "chat.html")
    return FileResponse(chat_html_path)


@app.post("/chat/send")
async def chat_send(request: ChatSendRequest):
    """
    Send a message to Slack thread for a user.
    
    Logic:
    1. Check if user already has a Slack thread
    2. If not, create a parent message in Slack and save thread_ts
    3. Post user message into that thread
    
    All Slack API calls are synchronous.
    """
    user_id = request.user_id
    message_text = request.message
    
    logger.info(f"POST /chat/send - user_id={user_id}, message_length={len(message_text)}")
    
    # Check if user already has a Slack thread
    thread_ts = mapping.get_thread(user_id)
    
    if not thread_ts:
        # Create parent message in Slack (this becomes the thread parent)
        logger.info(f"Creating new Slack thread for user {user_id}")
        try:
            response = slack_client.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=f"*New conversation started*\nUser ID: {user_id}\n\n{message_text}"
            )
            
            # Extract thread_ts from response
            thread_ts = response["ts"]
            
            # Store bidirectional mappings using mapping module
            mapping.save_mapping(user_id, thread_ts)
            
            # Log mapping creation for debugging correlation
            logger.info(f"Created mapping: user_id={user_id} → thread_ts={thread_ts}")
            logger.info(f"Created new Slack thread for user {user_id}: thread_ts={thread_ts}")
            
            return {
                "status": "sent",
                "user_id": user_id,
                "thread_ts": thread_ts,
                "message": message_text,
                "thread_created": True
            }
            
        except SlackApiError as e:
            error_msg = e.response.get("error", "Unknown error")
            error_detail = e.response.get("detail", str(e))
            logger.error(
                f"Failed to create Slack thread for user {user_id}. "
                f"Slack API error: {error_msg}, detail: {error_detail}, "
                f"response: {e.response}"
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create Slack thread: {error_msg}"
            )
    else:
        # Post message to existing thread
        logger.info(f"Posting message to existing thread {thread_ts} for user {user_id}")
        try:
            response = slack_client.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=message_text,
                thread_ts=thread_ts
            )
            
            logger.info(f"Posted message to Slack thread {thread_ts} for user {user_id}")
            
            return {
                "status": "sent",
                "user_id": user_id,
                "thread_ts": thread_ts,
                "message": message_text,
                "thread_created": False
            }
            
        except SlackApiError as e:
            error_msg = e.response.get("error", "Unknown error")
            error_detail = e.response.get("detail", str(e))
            logger.error(
                f"Failed to post message to Slack thread for user {user_id}. "
                f"Slack API error: {error_msg}, detail: {error_detail}, "
                f"response: {e.response}"
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to post message to Slack thread: {error_msg}"
            )


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """
    WebSocket endpoint for website users.
    Receives messages from website and forwards to Slack thread.
    Uses ConnectionManager to handle connection lifecycle.
    """
    await connection_manager.connect(websocket, user_id)

    try:
        while True:
            # Receive message from website user
            data = await websocket.receive_text()
            logger.info(f"Received message from user {user_id}: {data}")

            try:
                message_data = json.loads(data)
                message_text = message_data.get("message", data)
            except json.JSONDecodeError:
                message_text = data

            # Get or create Slack thread for this user
            thread_ts = mapping.get_thread(user_id)

            if not thread_ts:
                # Create initial message in Slack channel (this becomes the thread parent)
                try:
                    response = slack_client.chat_postMessage(
                        channel=SLACK_CHANNEL_ID,
                        text=f"*New conversation started*\nUser ID: {user_id}\n\n{message_text}"
                    )
                    thread_ts = response["ts"]
                    # Store bidirectional mappings using mapping module
                    mapping.save_mapping(user_id, thread_ts)
                    # Log mapping creation for debugging correlation
                    logger.info(f"Created mapping: user_id={user_id} → thread_ts={thread_ts}")
                    logger.info(f"Created new Slack thread for user {user_id}: thread_ts={thread_ts}")
                except SlackApiError as e:
                    error_msg = e.response.get("error", "Unknown error")
                    error_detail = e.response.get("detail", str(e))
                    logger.error(
                        f"Failed to create Slack thread for user {user_id}. "
                        f"Slack API error: {error_msg}, detail: {error_detail}, "
                        f"response: {e.response}"
                    )
                    await connection_manager.send_personal_message({
                        "error": "Failed to create Slack thread",
                        "details": error_msg
                    }, user_id)
                    continue
            else:
                # Post message to existing thread
                try:
                    slack_client.chat_postMessage(
                        channel=SLACK_CHANNEL_ID,
                        text=message_text,
                        thread_ts=thread_ts
                    )
                    logger.info(f"Posted message to Slack thread {thread_ts} for user {user_id}")
                except SlackApiError as e:
                    error_msg = e.response.get("error", "Unknown error")
                    error_detail = e.response.get("detail", str(e))
                    logger.error(
                        f"Failed to post message to Slack thread for user {user_id}. "
                        f"Slack API error: {error_msg}, detail: {error_detail}, "
                        f"response: {e.response}"
                    )
                    await connection_manager.send_personal_message({
                        "error": "Failed to post message to Slack",
                        "details": error_msg
                    }, user_id)
                    continue

            # Acknowledge receipt
            await connection_manager.send_personal_message({
                "status": "sent",
                "message": message_text
            }, user_id)

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: user_id={user_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket handler for user {user_id}: {e}", exc_info=True)
    finally:
        # Cleanup connection
        connection_manager.disconnect(user_id)


@app.post("/slack/events")
async def slack_events(request: Request):
    """
    Slack Events API endpoint - Hardened implementation.
    Handles URL verification challenge and message events from channels.
    Real-time relay: forwards Slack thread messages to website users via WebSocket.
    
    Processing Flow:
    1. Accept url_verification and return challenge (immediate response)
    2. Accept event_callback only (reject others with logs)
    3. Process only event.type == "message"
    4. Strict filtering: ignore bot messages (multiple checks)
    5. Require thread_ts (only process thread replies)
    6. Validate mapping exists before sending
    7. Check user connection before WebSocket push
    8. Comprehensive logging at every stage
    """
    # DIAGNOSTIC LOGGING: Confirm Slack Events API is reaching backend
    import datetime
    import json as json_lib
    
    logger.info("=" * 60)
    logger.info("Slack event endpoint HIT")
    logger.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
    logger.info(f"Request headers: {dict(request.headers)}")
    
    # Read raw body before parsing
    raw_body = await request.body()
    logger.info(f"Raw request body (first 500 chars): {raw_body[:500]}")
    logger.info("=" * 60)
    
    try:
        # Parse JSON body safely (parse from raw body since we already read it)
        body = json_lib.loads(raw_body.decode('utf-8'))
        logger.info("Received Slack event")

        # STEP 1: Handle URL verification challenge (Priority: Fast Response)
        # Slack sends this during Event Subscription setup
        if body.get("type") == "url_verification":
            challenge = body.get("challenge")
            if not challenge:
                logger.warning("URL verification challenge missing 'challenge' field")
                raise HTTPException(status_code=400, detail="Missing challenge field")
            
            # Log confirmation for setup verification
            logger.info("Slack URL verification challenge received - returning challenge")
            logger.info(f"URL verification successful - endpoint is reachable by Slack")
            
            # Return challenge immediately - do NOT process anything else
            return JSONResponse(content={"challenge": challenge})

        # STEP 2: Accept event_callback only - reject all other types
        if body.get("type") != "event_callback":
            event_type = body.get("type", "unknown")
            logger.info(f"Rejected non-event_callback type: {event_type}")
            return JSONResponse(content={"status": "ok"})

        event = body.get("event", {})
        if not event:
            logger.warning("Event callback missing 'event' field")
            return JSONResponse(content={"status": "ok"})

        # STEP 3: Process only event.type == "message"
        event_type = event.get("type")
        if event_type != "message":
            logger.info(f"Rejected non-message event: event_type={event_type}")
            return JSONResponse(content={"status": "ok"})

        logger.info("Processing Slack message event")

        # STEP 4: Strict Filtering Rules - Prevent Echo
        # Check multiple bot indicators - ignore if ANY exist
        bot_id = event.get("bot_id")
        subtype = event.get("subtype")
        event_user = event.get("user")
        
        ignore_reason = None
        
        # Check 1: bot_id field
        if bot_id is not None:
            ignore_reason = f"bot_id={bot_id}"
        
        # Check 2: bot_message subtype
        elif subtype == "bot_message":
            ignore_reason = "subtype=bot_message"
        
        # Check 3: Message from our own bot
        elif BOT_USER_ID and event_user == BOT_USER_ID:
            ignore_reason = f"our_bot_user_id={BOT_USER_ID}"
        
        if ignore_reason:
            logger.info(f"Ignored bot message — reason: {ignore_reason}")
            return JSONResponse(content={"status": "ok"})

        # STEP 5: Require thread_ts (We Only Support Replies)
        thread_ts = event.get("thread_ts")
        if not thread_ts:
            logger.info("Ignored non-thread message — no thread_ts (only processing thread replies)")
            return JSONResponse(content={"status": "ok"})

        logger.info(f"Thread TS: {thread_ts}")

        # Extract message text
        message_text = event.get("text", "")
        if not message_text:
            logger.info("Ignored empty message")
            return JSONResponse(content={"status": "ok"})

        logger.info(f"Processing Slack thread reply: thread_ts={thread_ts}, text_length={len(message_text)}, text_preview='{message_text[:50]}...'")

        # STEP 6: Validate Mapping Before Sending
        user_id = mapping.get_user(thread_ts)
        if not user_id:
            logger.error(
                f"Mapping missing — cannot deliver: thread_ts={thread_ts}. "
                f"Likely causes: server restart (mappings lost) or stale thread (mapping never created). "
                f"Message will NOT be forwarded."
            )
            return JSONResponse(content={"status": "ok"})

        logger.info(f"Mapped thread to user: thread_ts={thread_ts} → user_id={user_id}")

        # STEP 7: Harden WebSocket Delivery - Check Connection Before Sending
        is_connected = connection_manager.is_connected(user_id)
        logger.info(f"User connected: user_id={user_id}, connected={is_connected}")
        
        if not is_connected:
            logger.warning(
                f"User not connected — skipping delivery: user_id={user_id}, thread_ts={thread_ts}. "
                f"User may have closed browser or disconnected."
            )
            return JSONResponse(content={"status": "ok"})

        # STEP 8: Push Message to Website via WebSocket
        logger.info(f"Forwarding Slack reply to WebSocket: user_id={user_id}, thread_ts={thread_ts}")
        try:
            success = await connection_manager.send_personal_message({
                "from": "support",
                "text": message_text
            }, user_id)
            
            if success:
                logger.info(
                    f"Successfully forwarded Slack reply to user {user_id}: "
                    f"thread_ts={thread_ts}, message='{message_text[:50]}...'"
                )
            else:
                logger.warning(
                    f"Failed to forward Slack reply to user {user_id} (send failed) — "
                    f"WebSocket send_personal_message returned False"
                )
        except Exception as e:
            # Must not crash if user disconnected - catch all exceptions
            logger.error(
                f"Error sending message to user {user_id} via WebSocket: {e}. "
                f"User may have disconnected. Exception type: {type(e).__name__}",
                exc_info=True
            )
            # Continue gracefully - don't crash

        return JSONResponse(content={"status": "ok"})

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(
            f"Error processing Slack event: {e}. Exception type: {type(e).__name__}",
            exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Health check with connection stats"""
    return {
        "status": "ok",
        "active_connections": connection_manager.get_connection_count(),
        "connected_users": connection_manager.get_connected_users(),
        "mapped_users": mapping.get_mapping_count()
    }


@app.get("/debug/mappings")
async def debug_mappings():
    """
    Debug endpoint to view current in-memory mappings.
    Useful for verifying mapping state during development and debugging.
    
    Returns:
        Current user_to_thread and thread_to_user mappings
    """
    all_mappings = mapping.get_all_mappings()
    
    # Build reverse mapping for display
    thread_to_user_map = {}
    for uid, ts in all_mappings.items():
        thread_to_user_map[ts] = uid
    
    return {
        "user_to_thread": all_mappings,
        "thread_to_user": thread_to_user_map,
        "total_mappings": len(all_mappings),
        "note": "Mappings are in-memory and will be lost on server restart"
    }


@app.post("/push/{user_id}")
async def push_message(user_id: str, message: dict):
    """
    Server-side push endpoint to send a message to a specific user.
    Useful for testing or external systems pushing messages to users.
    
    Example:
        POST /push/user123
        {"type": "notification", "message": "Hello from server"}
    """
    if not connection_manager.is_connected(user_id):
        raise HTTPException(status_code=404, detail=f"User {user_id} is not connected")
    
    success = await connection_manager.send_personal_message(message, user_id)
    if success:
        return {"status": "sent", "user_id": user_id}
    else:
        raise HTTPException(status_code=500, detail="Failed to send message")

