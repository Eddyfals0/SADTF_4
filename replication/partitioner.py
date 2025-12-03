"""
Partitioner for allocating blocks and replicas across nodes.
Implements round-robin strategy considering free space.
"""
import logging
from typing import List, Tuple, Dict, Any
from metadata.block_table import BlockType
from metadata.node_registry import NodeRegistry

logger = logging.getLogger(__name__)


class Partitioner:
    """Allocates blocks and replicas across nodes using round-robin."""
    
    def __init__(self, block_table, node_registry):
        """
        Initialize partitioner.
        
        Args:
            block_table: Reference to BlockTable
            node_registry: Reference to NodeRegistry
        """
        self.block_table = block_table
        self.node_registry = node_registry
        self.round_robin_index = 0
    
    def allocate_blocks(self, num_blocks: int, file_name: str, 
                       exclude_node_id: int = None) -> List[Tuple[int, int, BlockType]]:
        """
        Allocate blocks and replicas for a file.
        
        Args:
            num_blocks: Number of blocks needed
            file_name: Name of the file
            exclude_node_id: Node ID to exclude (e.g., the uploading node)
        
        Returns:
            List of tuples (block_id, node_id, block_type)
            Each logical block has 1 original + at least 1 copy
        """
        online_nodes = self.node_registry.get_online_nodes()
        if exclude_node_id and exclude_node_id in online_nodes:
            # Create a copy without the excluded node
            online_nodes = {nid: info for nid, info in online_nodes.items() 
                          if nid != exclude_node_id}
        
        if not online_nodes:
            logger.error("No online nodes available for block allocation")
            return []
        
        # Filter nodes with available space
        available_nodes = {nid: info for nid, info in online_nodes.items() 
                          if info["free_space"] > 0}
        
        if not available_nodes:
            logger.error("No nodes with free space available")
            return []
        
        node_list = list(available_nodes.keys())
        allocations = []
        
        for file_block_index in range(num_blocks):
            # Allocate original block
            original_node = self._select_node(node_list, available_nodes)
            if original_node is None:
                logger.error(f"Failed to allocate original block {file_block_index}")
                break
            
            original_block_id = self.block_table.allocate_block(
                original_node, file_name, file_block_index, BlockType.ORIGINAL
            )
            
            if original_block_id is None:
                logger.error(f"No free blocks available for allocation")
                break
            
            allocations.append((original_block_id, original_node, BlockType.ORIGINAL))
            
            # Update available space (temporarily, for replica allocation)
            available_nodes[original_node]["free_space"] -= 1
            
            # Allocate at least one replica on a different node
            replica_node = self._select_node(node_list, available_nodes, exclude=[original_node])
            if replica_node is not None:
                replica_block_id = self.block_table.allocate_block(
                    replica_node, file_name, file_block_index, BlockType.COPY
                )
                if replica_block_id is not None:
                    allocations.append((replica_block_id, replica_node, BlockType.COPY))
                    available_nodes[replica_node]["free_space"] -= 1
                else:
                    logger.warning(f"Failed to allocate replica for block {file_block_index}")
            else:
                logger.warning(f"No available node for replica of block {file_block_index}")
            
            # Restore original node's free space
            available_nodes[original_node]["free_space"] += 1
        
        logger.info(f"Allocated {len(allocations)} blocks for {file_name}")
        return allocations
    
    def _select_node(self, node_list: List[int], available_nodes: Dict[int, Dict[str, Any]], 
                    exclude: List[int] = None) -> int:
        """
        Select a node using round-robin, considering free space.
        
        Args:
            node_list: List of node IDs
            available_nodes: Dict of node info with free_space
            exclude: List of node IDs to exclude
        
        Returns:
            Selected node ID or None
        """
        if exclude:
            candidates = [nid for nid in node_list if nid not in exclude and nid in available_nodes]
        else:
            candidates = [nid for nid in node_list if nid in available_nodes]
        
        if not candidates:
            return None
        
        # Round-robin selection
        start_index = self.round_robin_index % len(candidates)
        self.round_robin_index += 1
        
        # Try to find a node with free space, starting from round-robin index
        for i in range(len(candidates)):
            idx = (start_index + i) % len(candidates)
            node_id = candidates[idx]
            if available_nodes[node_id]["free_space"] > 0:
                return node_id
        
        return None

