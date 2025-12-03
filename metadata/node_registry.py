"""
Node registry for tracking all nodes in the P2P group.
Thread-safe registry with node information (ID, IP, status, capacity, etc.).
"""
import threading
import time
import logging
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class NodeRegistry:
    """Thread-safe registry of nodes in the P2P group."""
    
    def __init__(self):
        """Initialize empty node registry."""
        self.nodes: Dict[int, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.timeout_threshold = 9.0  # 9 seconds
    
    def add_node(self, node_id: int, ip: str, port: int, total_capacity: int, 
                 free_space: int, udp_port: int = 8889) -> None:
        """
        Add or update a node in the registry.
        
        Args:
            node_id: Unique node ID
            ip: Node IP address
            port: TCP port
            total_capacity: Total capacity in MB
            free_space: Free space in MB
            udp_port: UDP port for heartbeats
        """
        with self.lock:
            self.nodes[node_id] = {
                "ip": ip,
                "port": port,
                "udp_port": udp_port,
                "status": "online",
                "total_capacity": total_capacity,
                "free_space": free_space,
                "last_heartbeat": time.time()
            }
            logger.info(f"Added/updated node {node_id} at {ip}:{port}")
    
    def update_node(self, node_id: int, **kwargs) -> None:
        """Update node information."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].update(kwargs)
    
    def update_heartbeat(self, node_id: int, timestamp: float) -> None:
        """Update last heartbeat timestamp for a node."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]["last_heartbeat"] = timestamp
                if self.nodes[node_id]["status"] == "offline":
                    # Node came back online
                    self.nodes[node_id]["status"] = "online"
                    logger.info(f"Node {node_id} came back online")
    
    def mark_offline(self, node_id: int) -> None:
        """Mark a node as offline."""
        with self.lock:
            if node_id in self.nodes and self.nodes[node_id]["status"] == "online":
                self.nodes[node_id]["status"] = "offline"
                logger.warning(f"Node {node_id} marked as offline")
    
    def check_timeouts(self) -> None:
        """Check for nodes that haven't sent heartbeats and mark them offline."""
        current_time = time.time()
        with self.lock:
            for node_id, node_info in self.nodes.items():
                if node_info["status"] == "online":
                    time_since_heartbeat = current_time - node_info["last_heartbeat"]
                    if time_since_heartbeat > self.timeout_threshold:
                        self.nodes[node_id]["status"] = "offline"
                        logger.warning(f"Node {node_id} timed out (last heartbeat: {time_since_heartbeat:.1f}s ago)")
    
    def get_node(self, node_id: int) -> Optional[Dict[str, Any]]:
        """Get node information by ID."""
        with self.lock:
            return self.nodes.get(node_id)
    
    def get_online_nodes(self) -> Dict[int, Dict[str, Any]]:
        """Get all online nodes."""
        with self.lock:
            return {nid: info.copy() for nid, info in self.nodes.items() 
                   if info["status"] == "online"}
    
    def get_all_nodes(self) -> Dict[int, Dict[str, Any]]:
        """Get all nodes (online and offline)."""
        with self.lock:
            return {nid: info.copy() for nid, info in self.nodes.items()}
    
    def get_total_capacity(self) -> int:
        """Get total capacity of all online nodes in MB."""
        with self.lock:
            return sum(info["total_capacity"] for info in self.nodes.values() 
                      if info["status"] == "online")
    
    def get_total_free_space(self) -> int:
        """Get total free space of all online nodes in MB."""
        with self.lock:
            return sum(info["free_space"] for info in self.nodes.values() 
                      if info["status"] == "online")
    
    def remove_node(self, node_id: int) -> None:
        """Remove a node from the registry."""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                logger.info(f"Removed node {node_id} from registry")
    
    def update_free_space(self, node_id: int, free_space: int) -> None:
        """Update free space for a node."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]["free_space"] = free_space

