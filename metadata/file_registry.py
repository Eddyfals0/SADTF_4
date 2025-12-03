"""
File registry for tracking files in the distributed system.
Persists file metadata to metadata.json.
"""
import threading
import time
import logging
from typing import Dict, Optional, Any, List
from utils.persistence import load_metadata, save_metadata

logger = logging.getLogger(__name__)


class FileRegistry:
    """Thread-safe registry of files with persistence."""
    
    def __init__(self):
        """Initialize file registry, loading from metadata.json if it exists."""
        self.files: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self._load_from_disk()
    
    def _load_from_disk(self) -> None:
        """Load file metadata from disk."""
        metadata = load_metadata()
        with self.lock:
            self.files = metadata.get("files", {})
            logger.info(f"Loaded {len(self.files)} files from metadata.json")
    
    def _save_to_disk(self) -> None:
        """Save file metadata to disk."""
        metadata = load_metadata()
        with self.lock:
            metadata["files"] = self.files.copy()
        save_metadata(metadata)
    
    def register_file(self, file_name: str, size: int, num_blocks: int, 
                     block_ids: List[int]) -> None:
        """
        Register a new file.
        
        Args:
            file_name: Name of the file
            size: File size in bytes
            num_blocks: Number of blocks
            block_ids: List of block IDs
        """
        with self.lock:
            self.files[file_name] = {
                "size": size,
                "num_blocks": num_blocks,
                "upload_date": time.time(),
                "blocks": block_ids.copy()
            }
            logger.info(f"Registered file: {file_name} ({size} bytes, {num_blocks} blocks)")
        self._save_to_disk()
    
    def get_file_info(self, file_name: str) -> Optional[Dict[str, Any]]:
        """Get file information."""
        with self.lock:
            return self.files.get(file_name)
    
    def get_all_files(self) -> Dict[str, Dict[str, Any]]:
        """Get all files."""
        with self.lock:
            return {name: info.copy() for name, info in self.files.items()}
    
    def remove_file(self, file_name: str) -> None:
        """Remove a file from the registry."""
        with self.lock:
            if file_name in self.files:
                del self.files[file_name]
                logger.info(f"Removed file: {file_name}")
        self._save_to_disk()
    
    def update_from_sync(self, files: Dict[str, Dict[str, Any]]) -> None:
        """Update file registry from synchronization data."""
        with self.lock:
            self.files = {name: info.copy() for name, info in files.items()}
            logger.info(f"File registry synchronized, {len(self.files)} files")
        self._save_to_disk()

