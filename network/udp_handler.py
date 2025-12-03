"""
UDP handler for heartbeats.
Manages sending and receiving heartbeat messages to detect node failures.
"""
import socket
import threading
import time
import logging
from typing import Dict, Set
from network.message_protocol import Message, MessageType

logger = logging.getLogger(__name__)


class UDPHandler:
    """Handles UDP communication for heartbeats."""
    
    def __init__(self, node_id: int, port: int, node_registry):
        """
        Initialize UDP handler.
        
        Args:
            node_id: ID of this node
            port: UDP port (8889)
            node_registry: Reference to NodeRegistry for updating heartbeat timestamps
        """
        self.node_id = node_id
        self.port = port
        self.node_registry = node_registry
        self.running = False
        self.sock = None
        self.sender_thread = None
        self.receiver_thread = None
        self.heartbeat_interval = 3.0  # 3 seconds
        self.timeout_threshold = 9.0  # 9 seconds (3 missed heartbeats)
        
    def start(self):
        """Start UDP sender and receiver threads."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('', self.port))
            self.sock.settimeout(1.0)  # Timeout for receiving
            self.running = True
            
            self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
            self.receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
            
            self.sender_thread.start()
            self.receiver_thread.start()
            
            logger.info(f"UDP handler started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start UDP handler: {e}")
            raise
    
    def stop(self):
        """Stop UDP handler and close socket."""
        self.running = False
        if self.sock:
            self.sock.close()
        logger.info("UDP handler stopped")
    
    def _sender_loop(self):
        """Thread that sends heartbeats to all online nodes every 3 seconds."""
        while self.running:
            try:
                online_nodes = self.node_registry.get_online_nodes()
                for node_id, node_info in online_nodes.items():
                    if node_id == self.node_id:
                        continue
                    
                    try:
                        msg = Message(MessageType.HEARTBEAT, self.node_id, {
                            "node_id": self.node_id
                        })
                        data = msg.serialize()
                        addr = (node_info["ip"], node_info.get("udp_port", self.port))
                        self.sock.sendto(data, addr)
                    except Exception as e:
                        logger.debug(f"Failed to send heartbeat to node {node_id}: {e}")
                
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat sender loop: {e}")
                time.sleep(self.heartbeat_interval)
    
    def _receiver_loop(self):
        """Thread that receives heartbeats and updates timestamps."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                try:
                    msg = Message.deserialize(data)
                    if msg.type == MessageType.HEARTBEAT:
                        sender_id = msg.payload.get("node_id", msg.sender_id)
                        self.node_registry.update_heartbeat(sender_id, time.time())
                        
                        # Send acknowledgment
                        ack_msg = Message(MessageType.HEARTBEAT_ACK, self.node_id, {
                            "node_id": self.node_id
                        })
                        ack_data = ack_msg.serialize()
                        self.sock.sendto(ack_data, addr)
                    elif msg.type == MessageType.HEARTBEAT_ACK:
                        sender_id = msg.payload.get("node_id", msg.sender_id)
                        self.node_registry.update_heartbeat(sender_id, time.time())
                except ValueError as e:
                    logger.debug(f"Failed to deserialize heartbeat message: {e}")
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"Error in heartbeat receiver loop: {e}")
                    time.sleep(0.1)

