"""
Block table for tracking all blocks in the distributed system.
Each entry represents a physical block (1 MB) in the system.
"""
import threading
import logging
from typing import List, Dict, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)


class BlockType(Enum):
    """Type of block: original or copy."""
    ORIGINAL = "original"
    COPY = "copy"


class BlockStatus(Enum):
    """Status of a block."""
    USED = "used"
    FREE = "free"
    UNAVAILABLE = "unavailable"  # Node is offline but block data might still exist


class BlockTable:
    """
    Thread-safe global block table.
    Size equals total capacity of all online nodes (in MB).
    """
    
    def __init__(self):
        """Initialize empty block table."""
        self.blocks: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.next_block_id = 1
    
    def resize(self, new_size: int) -> None:
        """
        Resize block table to new_size (in MB).
        If shrinking, marks excess blocks as unavailable.
        If growing, adds free blocks.
        """
        with self.lock:
            current_size = len(self.blocks)
            if new_size > current_size:
                # Add free blocks
                for i in range(current_size, new_size):
                    self.blocks.append({
                        "block_id": self.next_block_id,
                        "type": None,
                        "node_id": None,
                        "file_name": None,
                        "file_block_index": None,
                        "status": BlockStatus.FREE.value
                    })
                    self.next_block_id += 1
            elif new_size < current_size:
                # Mark excess blocks as unavailable if they're used
                for i in range(new_size, current_size):
                    if self.blocks[i]["status"] == BlockStatus.USED.value:
                        self.blocks[i]["status"] = BlockStatus.UNAVAILABLE.value
            logger.info(f"Block table resized from {current_size} to {new_size} MB")
    
    def allocate_block(self, node_id: int, file_name: str, file_block_index: int, 
                      block_type: BlockType) -> Optional[int]:
        """
        Allocate a free block for a file.
        
        Args:
            node_id: Node where block will be stored
            file_name: Name of the file
            file_block_index: Index of this block within the file
            block_type: Original or copy
        
        Returns:
            Block ID if allocated, None if no free blocks available
        """
        with self.lock:
            # Find a free block
            for block in self.blocks:
                if block["status"] == BlockStatus.FREE.value:
                    block["block_id"] = self.next_block_id
                    block["type"] = block_type.value
                    block["node_id"] = node_id
                    block["file_name"] = file_name
                    block["file_block_index"] = file_block_index
                    block["status"] = BlockStatus.USED.value
                    allocated_id = self.next_block_id
                    self.next_block_id += 1
                    logger.debug(f"Allocated block {allocated_id} for {file_name} on node {node_id}")
                    return allocated_id
            return None
    
    def mark_free(self, block_id: int) -> None:
        """Mark a block as free."""
        with self.lock:
            for block in self.blocks:
                if block.get("block_id") == block_id:
                    block["status"] = BlockStatus.FREE.value
                    block["type"] = None
                    block["node_id"] = None
                    block["file_name"] = None
                    block["file_block_index"] = None
                    logger.debug(f"Marked block {block_id} as free")
                    return
    
    def get_blocks_for_file(self, file_name: str) -> List[Dict[str, Any]]:
        """Get all blocks for a specific file."""
        with self.lock:
            return [block.copy() for block in self.blocks 
                   if block.get("file_name") == file_name]
    
    def get_blocks_on_node(self, node_id: int) -> List[Dict[str, Any]]:
        """Get all blocks stored on a specific node."""
        with self.lock:
            return [block.copy() for block in self.blocks 
                   if block.get("node_id") == node_id]
    
    def get_block(self, block_id: int) -> Optional[Dict[str, Any]]:
        """Get block information by ID."""
        with self.lock:
            for block in self.blocks:
                if block.get("block_id") == block_id:
                    return block.copy()
            return None
    
    def get_all_blocks(self) -> List[Dict[str, Any]]:
        """Get all blocks (for synchronization)."""
        with self.lock:
            return [block.copy() for block in self.blocks]
    
    def update_from_sync(self, blocks: List[Dict[str, Any]]) -> None:
        """Update block table from synchronization data."""
        with self.lock:
            self.blocks = [block.copy() for block in blocks]
            # Update next_block_id to be higher than any existing block_id
            max_id = 0
            for block in self.blocks:
                bid = block.get("block_id")
                if bid and bid > max_id:
                    max_id = bid
            self.next_block_id = max_id + 1
            logger.info(f"Block table synchronized, {len(self.blocks)} blocks")
    
    def mark_node_blocks_unavailable(self, node_id: int) -> None:
        """Mark all blocks on a node as unavailable (node went offline)."""
        with self.lock:
            count = 0
            for block in self.blocks:
                if block.get("node_id") == node_id and block["status"] == BlockStatus.USED.value:
                    block["status"] = BlockStatus.UNAVAILABLE.value
                    count += 1
            if count > 0:
                logger.info(f"Marked {count} blocks on node {node_id} as unavailable")

