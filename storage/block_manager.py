"""
Block manager for physical block storage.
Manages the espacioCompartido directory and block files.
"""
import os
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class BlockManager:
    """Manages physical block storage in espacioCompartido directory."""
    
    def __init__(self, capacity_mb: int):
        """
        Initialize block manager.
        
        Args:
            capacity_mb: Maximum capacity in MB
        """
        self.capacity_mb = capacity_mb
        self.capacity_bytes = capacity_mb * 1024 * 1024
        self.base_dir = self._get_shared_space_path()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Block manager initialized: {capacity_mb} MB at {self.base_dir}")
    
    def _get_shared_space_path(self) -> Path:
        """Get the path to espacioCompartido directory."""
        if os.name == 'nt':  # Windows
            user_home = Path.home()
            return user_home / "espacioCompartido"
        else:  # Linux/Mac
            return Path.home() / "espacioCompartido"
    
    def get_block_path(self, block_id: int) -> Path:
        """Get the file path for a block."""
        return self.base_dir / f"block_{block_id}.dat"
    
    def write_block(self, block_id: int, data: bytes) -> bool:
        """
        Write a block to disk.
        
        Args:
            block_id: Block ID
            data: Block data (up to 1 MB)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            block_path = self.get_block_path(block_id)
            with open(block_path, 'wb') as f:
                f.write(data)
            logger.debug(f"Wrote block {block_id} ({len(data)} bytes)")
            return True
        except Exception as e:
            logger.error(f"Failed to write block {block_id}: {e}")
            return False
    
    def read_block(self, block_id: int) -> Optional[bytes]:
        """
        Read a block from disk.
        
        Args:
            block_id: Block ID
        
        Returns:
            Block data or None if not found
        """
        try:
            block_path = self.get_block_path(block_id)
            if not block_path.exists():
                return None
            with open(block_path, 'rb') as f:
                data = f.read()
            logger.debug(f"Read block {block_id} ({len(data)} bytes)")
            return data
        except Exception as e:
            logger.error(f"Failed to read block {block_id}: {e}")
            return None
    
    def delete_block(self, block_id: int) -> bool:
        """
        Delete a block from disk.
        
        Args:
            block_id: Block ID
        
        Returns:
            True if successful, False otherwise
        """
        try:
            block_path = self.get_block_path(block_id)
            if block_path.exists():
                block_path.unlink()
                logger.debug(f"Deleted block {block_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete block {block_id}: {e}")
            return False
    
    def get_used_space(self) -> int:
        """
        Get used space in MB.
        
        Returns:
            Used space in MB
        """
        total_bytes = 0
        try:
            for block_file in self.base_dir.glob("block_*.dat"):
                total_bytes += block_file.stat().st_size
        except Exception as e:
            logger.error(f"Error calculating used space: {e}")
        return total_bytes // (1024 * 1024)
    
    def get_free_space(self) -> int:
        """
        Get free space in MB.
        
        Returns:
            Free space in MB
        """
        used = self.get_used_space()
        return max(0, self.capacity_mb - used)
    
    def has_space(self, size_mb: int) -> bool:
        """
        Check if there's enough space for size_mb.
        
        Args:
            size_mb: Required space in MB
        
        Returns:
            True if enough space available
        """
        return self.get_free_space() >= size_mb
    
    def set_capacity(self, new_capacity_mb: int) -> None:
        """
        Set new capacity (only if not connected to group).
        
        Args:
            new_capacity_mb: New capacity in MB
        """
        self.capacity_mb = new_capacity_mb
        self.capacity_bytes = new_capacity_mb * 1024 * 1024
        logger.info(f"Block manager capacity set to {new_capacity_mb} MB")

