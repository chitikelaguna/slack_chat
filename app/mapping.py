"""
In-memory storage for user-thread bidirectional mappings.
Thread-safe implementation using locks.
"""
import threading
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Thread-safe in-memory storage
_lock = threading.Lock()
user_to_thread: Dict[str, str] = {}  # user_id -> thread_ts
thread_to_user: Dict[str, str] = {}  # thread_ts -> user_id


def get_thread(user_id: str) -> Optional[str]:
    """
    Get thread_ts for a given user_id.
    
    Args:
        user_id: Unique identifier for the user
        
    Returns:
        thread_ts if found, None otherwise
    """
    with _lock:
        thread_ts = user_to_thread.get(user_id)
        if thread_ts:
            logger.debug(f"Retrieved thread_ts={thread_ts} for user_id={user_id}")
        else:
            logger.debug(f"No thread found for user_id={user_id}")
        return thread_ts


def get_user(thread_ts: str) -> Optional[str]:
    """
    Get user_id for a given thread_ts.
    
    Args:
        thread_ts: Thread timestamp from Slack
        
    Returns:
        user_id if found, None otherwise
    """
    with _lock:
        user_id = thread_to_user.get(thread_ts)
        if user_id:
            logger.debug(f"Retrieved user_id={user_id} for thread_ts={thread_ts}")
        else:
            logger.debug(f"No user found for thread_ts={thread_ts}")
        return user_id


def save_mapping(user_id: str, thread_ts: str) -> None:
    """
    Save bidirectional mapping between user_id and thread_ts.
    Updates both dictionaries atomically.
    
    Args:
        user_id: Unique identifier for the user
        thread_ts: Thread timestamp from Slack
        
    Raises:
        ValueError: If user_id or thread_ts is empty
    """
    if not user_id:
        raise ValueError("user_id cannot be empty")
    if not thread_ts:
        raise ValueError("thread_ts cannot be empty")
    
    with _lock:
        # Check if user_id already has a different thread_ts
        existing_thread = user_to_thread.get(user_id)
        if existing_thread and existing_thread != thread_ts:
            logger.warning(
                f"User {user_id} already mapped to thread_ts={existing_thread}, "
                f"updating to thread_ts={thread_ts}"
            )
            # Remove old mapping
            if existing_thread in thread_to_user:
                del thread_to_user[existing_thread]
        
        # Check if thread_ts already mapped to a different user_id
        existing_user = thread_to_user.get(thread_ts)
        if existing_user and existing_user != user_id:
            logger.warning(
                f"Thread {thread_ts} already mapped to user_id={existing_user}, "
                f"updating to user_id={user_id}"
            )
            # Remove old mapping
            if existing_user in user_to_thread:
                del user_to_thread[existing_user]
        
        # Save bidirectional mapping
        user_to_thread[user_id] = thread_ts
        thread_to_user[thread_ts] = user_id
        
        logger.info(f"Saved mapping: user_id={user_id} <-> thread_ts={thread_ts}")


def delete_mapping(user_id: Optional[str] = None, thread_ts: Optional[str] = None) -> bool:
    """
    Delete mapping by user_id or thread_ts.
    If user_id is provided, deletes both directions.
    If thread_ts is provided, deletes both directions.
    
    Args:
        user_id: User ID to delete mapping for (optional)
        thread_ts: Thread timestamp to delete mapping for (optional)
        
    Returns:
        True if mapping was deleted, False if not found
        
    Raises:
        ValueError: If neither user_id nor thread_ts is provided
    """
    if not user_id and not thread_ts:
        raise ValueError("Either user_id or thread_ts must be provided")
    
    with _lock:
        deleted = False
        
        if user_id and user_id in user_to_thread:
            thread_ts_to_delete = user_to_thread[user_id]
            del user_to_thread[user_id]
            if thread_ts_to_delete in thread_to_user:
                del thread_to_user[thread_ts_to_delete]
            deleted = True
            logger.info(f"Deleted mapping for user_id={user_id}")
        
        elif thread_ts and thread_ts in thread_to_user:
            user_id_to_delete = thread_to_user[thread_ts]
            del thread_to_user[thread_ts]
            if user_id_to_delete in user_to_thread:
                del user_to_thread[user_id_to_delete]
            deleted = True
            logger.info(f"Deleted mapping for thread_ts={thread_ts}")
        
        return deleted


def get_all_mappings() -> Dict[str, str]:
    """
    Get all user_id -> thread_ts mappings.
    Returns a copy of the current mappings.
    
    Returns:
        Dictionary of user_id -> thread_ts mappings
    """
    with _lock:
        return user_to_thread.copy()


def get_mapping_count() -> int:
    """
    Get the number of stored mappings.
    
    Returns:
        Number of mappings
    """
    with _lock:
        return len(user_to_thread)

