"""
File manager for upload, download, and delete operations.
Handles file splitting into blocks and reconstruction.
"""
import os
import logging
import hashlib
import threading
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable
from storage.block_manager import BlockManager
from metadata.block_table import BlockTable, BlockType
from metadata.file_registry import FileRegistry
from replication.partitioner import Partitioner
from network.message_protocol import Message, MessageType

logger = logging.getLogger(__name__)

BLOCK_SIZE = 1024 * 1024  # 1 MB


class FileManager:
    """Manages file operations: upload, download, delete."""
    
    def __init__(self, block_manager: BlockManager, block_table: BlockTable,
                 file_registry: FileRegistry, partitioner: Partitioner,
                 send_message_callback: Callable = None, send_to_node_callback: Callable = None):
        """
        Initialize file manager.
        
        Args:
            block_manager: BlockManager instance
            block_table: BlockTable instance
            file_registry: FileRegistry instance
            partitioner: Partitioner instance
            send_message_callback: Callback to send messages to other nodes
            send_to_node_callback: Callback to send message to specific node (node_id, msg)
        """
        self.block_manager = block_manager
        self.block_table = block_table
        self.file_registry = file_registry
        self.partitioner = partitioner
        self.send_message = send_message_callback
        self.send_to_node = send_to_node_callback
        # Temporary cache for downloaded blocks
        self.block_cache: Dict[int, bytes] = {}
        self.cache_lock = threading.Lock()
    
    def upload_file(self, file_path: str, node_id: int) -> bool:
        """
        Upload a file by splitting it into blocks and distributing them.
        
        Args:
            file_path: Path to the file to upload
            node_id: ID of the node uploading (to exclude from allocation)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                logger.error(f"File not found: {file_path}")
                return False
            
            file_name = file_path_obj.name
            file_size = file_path_obj.stat().st_size
            
            # Calculate number of blocks needed
            num_blocks = (file_size + BLOCK_SIZE - 1) // BLOCK_SIZE
            
            logger.info(f"Uploading file: {file_name} ({file_size} bytes, {num_blocks} blocks)")
            
            # Allocate blocks and replicas
            allocations = self.partitioner.allocate_blocks(num_blocks, file_name, exclude_node_id=node_id)
            
            if len(allocations) < num_blocks:
                logger.error(f"Failed to allocate enough blocks for {file_name}")
                return False
            
            # Group allocations by file_block_index
            blocks_by_index = {}
            all_block_ids = []
            
            for block_id, target_node_id, block_type in allocations:
                # Get block info to find its file_block_index (set by partitioner)
                block_info = self.block_table.get_block(block_id)
                if block_info:
                    file_block_index = block_info.get("file_block_index")
                    if file_block_index is None:
                        logger.error(f"Block {block_id} has no file_block_index")
                        return False
                    
                    if file_block_index not in blocks_by_index:
                        blocks_by_index[file_block_index] = []
                    blocks_by_index[file_block_index].append((block_id, target_node_id, block_type))
                    all_block_ids.append(block_id)
                else:
                    logger.error(f"Block {block_id} not found in block table after allocation")
                    return False
            
            # Read file and send blocks
            block_ids = []
            with open(file_path, 'rb') as f:
                for file_block_index in range(num_blocks):
                    data = f.read(BLOCK_SIZE)
                    if not data:
                        break
                    
                    # Get allocations for this block index
                    if file_block_index not in blocks_by_index:
                        logger.error(f"No allocation for block index {file_block_index}")
                        return False
                    
                    # Send to all allocated nodes (original + replicas)
                    for block_id, target_node_id, block_type in blocks_by_index[file_block_index]:
                        if target_node_id == node_id:
                            # Store locally
                            if not self.block_manager.write_block(block_id, data):
                                logger.error(f"Failed to write block {block_id} locally")
                                return False
                        else:
                            # Send to remote node
                            if self.send_to_node:
                                msg = Message(MessageType.BLOCK_SEND, node_id, {
                                    "block_id": block_id,
                                    "file_name": file_name,
                                    "file_block_index": file_block_index,
                                    "block_type": block_type.value,
                                    "data": data.hex()  # Encode as hex for JSON
                                })
                                if not self.send_to_node(target_node_id, msg):
                                    logger.error(f"Failed to send block {block_id} to node {target_node_id}")
                                    return False
                            else:
                                logger.error(f"No send_to_node callback available")
                                return False
                        
            
            # Register file with all block IDs
            self.file_registry.register_file(file_name, file_size, num_blocks, all_block_ids)
            
            # Sync metadata with other nodes
            if self.send_message:
                self._sync_metadata()
            
            logger.info(f"Successfully uploaded file: {file_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file {file_path}: {e}")
            return False
    
    def download_file(self, file_name: str, save_path: str, node_id: int) -> bool:
        """
        Download a file by requesting blocks from nodes and reconstructing it.
        
        Args:
            file_name: Name of the file to download
            save_path: Path where to save the downloaded file
            node_id: ID of this node
        
        Returns:
            True if successful, False otherwise
        """
        try:
            file_info = self.file_registry.get_file_info(file_name)
            if not file_info:
                logger.error(f"File not found in registry: {file_name}")
                return False
            
            blocks = self.block_table.get_blocks_for_file(file_name)
            if not blocks:
                logger.error(f"No blocks found for file: {file_name}")
                return False
            
            # Group blocks by file_block_index
            blocks_by_index = {}
            for block in blocks:
                idx = block.get("file_block_index")
                if idx is not None:
                    if idx not in blocks_by_index:
                        blocks_by_index[idx] = []
                    blocks_by_index[idx].append(block)
            
            # Reconstruct file
            with open(save_path, 'wb') as f:
                for idx in sorted(blocks_by_index.keys()):
                    block_data = None
                    
                    # Try to get block from available sources
                    for block in blocks_by_index[idx]:
                        block_id = block.get("block_id")
                        block_node_id = block.get("node_id")
                        block_status = block.get("status")
                        
                        if block_status != "used":
                            continue
                        
                        # Try local first
                        if block_node_id == node_id:
                            block_data = self.block_manager.read_block(block_id)
                            if block_data:
                                break
                        else:
                            # Request from remote node
                            if self.send_to_node:
                                # Clear cache for this block
                                with self.cache_lock:
                                    if block_id in self.block_cache:
                                        del self.block_cache[block_id]
                                
                                msg = Message(MessageType.BLOCK_REQUEST, node_id, {
                                    "block_id": block_id
                                })
                                if self.send_to_node(block_node_id, msg):
                                    # Wait for block to arrive in cache (with timeout)
                                    logger.info(f"Requested block {block_id} from node {block_node_id}")
                                    timeout = 5.0  # 5 seconds timeout
                                    start_time = time.time()
                                    while time.time() - start_time < timeout:
                                        with self.cache_lock:
                                            if block_id in self.block_cache:
                                                block_data = self.block_cache[block_id]
                                                del self.block_cache[block_id]
                                                break
                                        time.sleep(0.1)  # Check every 100ms
                                    else:
                                        logger.warning(f"Timeout waiting for block {block_id}")
                                        continue
                                else:
                                    logger.warning(f"Failed to send block request to node {block_node_id}")
                                    continue
                            else:
                                logger.warning(f"Cannot request block {block_id} from node {block_node_id}")
                                continue
                    
                    if block_data:
                        f.write(block_data)
                    else:
                        logger.error(f"Failed to retrieve block at index {idx} for {file_name}")
                        return False
            
            logger.info(f"Successfully downloaded file: {file_name} to {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading file {file_name}: {e}")
            return False
    
    def delete_file(self, file_name: str, node_id: int) -> bool:
        """
        Delete a file by marking blocks as free and removing physical files.
        
        Args:
            file_name: Name of the file to delete
            node_id: ID of this node
        
        Returns:
            True if successful, False otherwise
        """
        try:
            file_info = self.file_registry.get_file_info(file_name)
            if not file_info:
                logger.error(f"File not found: {file_name}")
                return False
            
            blocks = self.block_table.get_blocks_for_file(file_name)
            
            # Mark blocks as free and delete physical files
            for block in blocks:
                block_id = block.get("block_id")
                block_node_id = block.get("node_id")
                
                if block_id:
                    self.block_table.mark_free(block_id)
                    
                    # Delete physical block if it's on this node
                    if block_node_id == node_id:
                        self.block_manager.delete_block(block_id)
            
            # Remove from registry
            self.file_registry.remove_file(file_name)
            
            # Notify other nodes
            if self.send_message:
                msg = Message(MessageType.DELETE_FILE, node_id, {
                    "file_name": file_name
                })
                # This will be handled by TCP handler
                self._sync_metadata()
            
            logger.info(f"Successfully deleted file: {file_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file {file_name}: {e}")
            return False
    
    def _sync_metadata(self) -> None:
        """Sync metadata with all online nodes."""
        # This will be called by TCP handler when sending METADATA_SYNC
        pass
    
    def cache_block(self, block_id: int, data: bytes) -> None:
        """Cache a block temporarily (for downloads)."""
        with self.cache_lock:
            self.block_cache[block_id] = data
            logger.debug(f"Cached block {block_id} ({len(data)} bytes)")

