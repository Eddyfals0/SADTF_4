"""
TCP handler for P2P communication.
Manages TCP server, connections, and message handling.
"""
import socket
import threading
import struct
import logging
import time
import uuid
from typing import Dict, Optional, Callable, Any
from network.message_protocol import Message, MessageType

logger = logging.getLogger(__name__)


class TCPHandler:
    """Handles TCP communication for P2P network."""
    
    def __init__(self, node_id: int, port: int, node_registry, block_table,
                 file_registry, block_manager, file_manager, get_group_id_callback: Callable,
                 set_group_id_callback: Callable, get_next_node_id_callback: Callable):
        """
        Initialize TCP handler.
        
        Args:
            node_id: ID of this node
            port: TCP port (8888)
            node_registry: Reference to NodeRegistry
            block_table: Reference to BlockTable
            file_registry: Reference to FileRegistry
            block_manager: Reference to BlockManager
            file_manager: Reference to FileManager
            get_group_id_callback: Callback to get current group_id
            set_group_id_callback: Callback to set group_id
            get_next_node_id_callback: Callback to get next available node_id
        """
        self.node_id = node_id
        self.port = port
        self.node_registry = node_registry
        self.block_table = block_table
        self.file_registry = file_registry
        self.block_manager = block_manager
        self.file_manager = file_manager
        self.get_group_id = get_group_id_callback
        self.set_group_id = set_group_id_callback
        self.get_next_node_id = get_next_node_id_callback
        
        self.running = False
        self.server_socket = None
        self.connections: Dict[int, socket.socket] = {}  # node_id -> socket
        self.connection_threads: Dict[int, threading.Thread] = {}
        self.lock = threading.Lock()
        self.server_thread = None
    
    def start(self):
        """Start TCP server."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('', self.port))
            self.server_socket.listen(10)
            self.running = True
            
            self.server_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self.server_thread.start()
            
            logger.info(f"TCP server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start TCP server: {e}")
            raise
    
    def stop(self):
        """Stop TCP server and close all connections."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        with self.lock:
            for sock in self.connections.values():
                try:
                    sock.close()
                except:
                    pass
            self.connections.clear()
        logger.info("TCP server stopped")
    
    def _accept_connections(self):
        """Thread that accepts incoming connections."""
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                logger.info(f"New connection from {addr}")
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket, addr),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {e}")
    
    def _handle_connection(self, sock: socket.socket, addr: tuple):
        """Handle a client connection."""
        try:
            # Receive initial message to identify the node
            data = self._receive_message(sock)
            if not data:
                return
            
            msg = Message.deserialize(data)
            sender_id = msg.sender_id
            
            # Handle CONNECT or RECONNECT
            if msg.type == MessageType.CONNECT:
                self._handle_connect(sock, addr, msg, sender_id)
            elif msg.type == MessageType.RECONNECT:
                self._handle_reconnect(sock, addr, msg, sender_id)
            else:
                # Already connected, handle other messages
                self._handle_message(sock, msg, sender_id)
                
        except Exception as e:
            logger.error(f"Error handling connection from {addr}: {e}")
        finally:
            try:
                sock.close()
            except:
                pass
    
    def _handle_connect(self, sock: socket.socket, addr: tuple, msg: Message, sender_id: int):
        """Handle CONNECT message."""
        try:
            payload = msg.payload
            remote_group_id = payload.get("group_id")
            remote_capacity = payload.get("capacity_mb", 50)
            remote_free_space = payload.get("free_space_mb", remote_capacity)
            
            local_group_id = self.get_group_id()
            
            # Determine node IDs and group
            if local_group_id is None and remote_group_id is None:
                # First two nodes: this becomes node1, remote becomes node2
                new_node_id = 2
                new_group_id = str(uuid.uuid4())
                self.set_group_id(new_group_id)
                logger.info(f"Creating new group {new_group_id}, assigning node_id={new_node_id} to {addr}")
            elif local_group_id is None:
                # Local joins remote's group
                new_node_id = self.get_next_node_id_callback()
                new_group_id = remote_group_id
                self.set_group_id(new_group_id)
                logger.info(f"Joining group {new_group_id}, assigned node_id={new_node_id}")
            elif remote_group_id is None:
                # Remote joins local's group
                new_node_id = self.get_next_node_id_callback()
                new_group_id = local_group_id
                logger.info(f"Remote joining group {new_group_id}, assigned node_id={new_node_id}")
            elif local_group_id == remote_group_id:
                # Same group, remote gets next ID
                new_node_id = self.get_next_node_id_callback()
                new_group_id = local_group_id
                logger.info(f"Node already in group, assigned node_id={new_node_id}")
            else:
                # Different groups - merge or reject
                logger.warning(f"Group mismatch: local={local_group_id}, remote={remote_group_id}")
                new_node_id = self.get_next_node_id_callback()
                new_group_id = local_group_id
                self.set_group_id(new_group_id)
            
            # Add node to registry
            self.node_registry.add_node(
                new_node_id, addr[0], self.port, remote_capacity, remote_free_space
            )
            
            # Store connection
            with self.lock:
                self.connections[new_node_id] = sock
            
            # Send CONNECT_ACK
            ack_msg = Message(MessageType.CONNECT_ACK, self.node_id, {
                "node_id": new_node_id,
                "group_id": new_group_id,
                "capacity_mb": self.block_manager.capacity_mb,
                "free_space_mb": self.block_manager.get_free_space(),
                "all_nodes": self._get_all_nodes_info(),
                "all_files": self.file_registry.get_all_files(),
                "all_blocks": self.block_table.get_all_blocks()
            })
            self._send_message(sock, ack_msg)
            
            # Send node discovery to new node
            discovery_msg = Message(MessageType.NODE_DISCOVERY, self.node_id, {
                "nodes": self._get_all_nodes_info()
            })
            self._send_message(sock, discovery_msg)
            
            # Update block table size
            total_capacity = self.node_registry.get_total_capacity()
            self.block_table.resize(total_capacity)
            
            # Continue handling messages from this connection
            while self.running:
                data = self._receive_message(sock)
                if not data:
                    break
                msg = Message.deserialize(data)
                self._handle_message(sock, msg, new_node_id)
                
        except Exception as e:
            logger.error(f"Error handling CONNECT: {e}")
    
    def _handle_reconnect(self, sock: socket.socket, addr: tuple, msg: Message, sender_id: int):
        """Handle RECONNECT message."""
        try:
            payload = msg.payload
            old_node_id = payload.get("node_id")
            remote_group_id = payload.get("group_id")
            
            local_group_id = self.get_group_id()
            
            # Verify node belongs to this group
            if local_group_id and local_group_id == remote_group_id:
                # Node is reconnecting to its group
                self.node_registry.add_node(
                    old_node_id, addr[0], self.port,
                    payload.get("capacity_mb", 50),
                    payload.get("free_space_mb", 50)
                )
                
                with self.lock:
                    self.connections[old_node_id] = sock
                
                # Send RECONNECT_ACK with full state
                ack_msg = Message(MessageType.RECONNECT_ACK, self.node_id, {
                    "node_id": old_node_id,
                    "group_id": local_group_id,
                    "all_nodes": self._get_all_nodes_info(),
                    "all_files": self.file_registry.get_all_files(),
                    "all_blocks": self.block_table.get_all_blocks()
                })
                self._send_message(sock, ack_msg)
                
                # Send node discovery
                discovery_msg = Message(MessageType.NODE_DISCOVERY, self.node_id, {
                    "nodes": self._get_all_nodes_info()
                })
                self._send_message(sock, discovery_msg)
                
                logger.info(f"Node {old_node_id} reconnected")
                
                # Continue handling messages
                while self.running:
                    data = self._receive_message(sock)
                    if not data:
                        break
                    msg = Message.deserialize(data)
                    self._handle_message(sock, msg, old_node_id)
            else:
                logger.warning(f"Reconnect rejected: group mismatch or invalid node_id")
                sock.close()
                
        except Exception as e:
            logger.error(f"Error handling RECONNECT: {e}")
    
    def _handle_message(self, sock: socket.socket, msg: Message, sender_id: int):
        """Handle various message types."""
        try:
            if msg.type == MessageType.BLOCK_SEND:
                self._handle_block_send(msg, sender_id)
            elif msg.type == MessageType.BLOCK_REQUEST:
                self._handle_block_request(sock, msg, sender_id)
            elif msg.type == MessageType.METADATA_SYNC:
                self._handle_metadata_sync(msg, sender_id)
            elif msg.type == MessageType.DELETE_FILE:
                self._handle_delete_file(msg, sender_id)
            elif msg.type == MessageType.NODE_DISCOVERY:
                self._handle_node_discovery(msg, sender_id)
            elif msg.type == MessageType.CONNECT_ACK:
                self._handle_connect_ack(msg, sender_id)
            elif msg.type == MessageType.RECONNECT_ACK:
                self._handle_reconnect_ack(msg, sender_id)
        except Exception as e:
            logger.error(f"Error handling message {msg.type}: {e}")
    
    def _handle_block_send(self, msg: Message, sender_id: int):
        """Handle BLOCK_SEND message."""
        payload = msg.payload
        block_id = payload.get("block_id")
        data_hex = payload.get("data")
        
        if block_id and data_hex:
            data = bytes.fromhex(data_hex)
            # Check if this is a response to a block request (for download) or a new block (for upload)
            # If block already exists, it might be a download response - cache it
            existing_block = self.block_manager.read_block(block_id)
            if existing_block is None:
                # New block for upload - store it
                if self.block_manager.write_block(block_id, data):
                    # Update free space
                    free_space = self.block_manager.get_free_space()
                    self.node_registry.update_free_space(self.node_id, free_space)
                    logger.debug(f"Received and stored block {block_id} from node {sender_id}")
            else:
                # Block exists - this is likely a download response, cache it
                if self.file_manager:
                    self.file_manager.cache_block(block_id, data)
                    logger.debug(f"Cached block {block_id} for download from node {sender_id}")
    
    def _handle_block_request(self, sock: socket.socket, msg: Message, sender_id: int):
        """Handle BLOCK_REQUEST message."""
        payload = msg.payload
        block_id = payload.get("block_id")
        
        if block_id:
            data = self.block_manager.read_block(block_id)
            if data:
                response_msg = Message(MessageType.BLOCK_SEND, self.node_id, {
                    "block_id": block_id,
                    "data": data.hex()
                })
                self._send_message(sock, response_msg)
                logger.debug(f"Sent block {block_id} to node {sender_id}")
            else:
                logger.warning(f"Block {block_id} not found locally")
    
    def _handle_metadata_sync(self, msg: Message, sender_id: int):
        """Handle METADATA_SYNC message."""
        payload = msg.payload
        files = payload.get("files", {})
        blocks = payload.get("blocks", [])
        
        # Update local registries
        self.file_registry.update_from_sync(files)
        self.block_table.update_from_sync(blocks)
        logger.debug(f"Metadata synchronized from node {sender_id}")
    
    def _handle_delete_file(self, msg: Message, sender_id: int):
        """Handle DELETE_FILE message."""
        payload = msg.payload
        file_name = payload.get("file_name")
        
        if file_name:
            # Get blocks for this file
            blocks = self.block_table.get_blocks_for_file(file_name)
            for block in blocks:
                block_id = block.get("block_id")
                block_node_id = block.get("node_id")
                
                if block_id and block_node_id == self.node_id:
                    # Delete local block
                    self.block_manager.delete_block(block_id)
                    self.block_table.mark_free(block_id)
            
            # Remove from registry
            self.file_registry.remove_file(file_name)
            logger.info(f"Deleted file {file_name} (notification from node {sender_id})")
    
    def _handle_node_discovery(self, msg: Message, sender_id: int):
        """Handle NODE_DISCOVERY message."""
        payload = msg.payload
        nodes = payload.get("nodes", [])
        
        # Establish connections to discovered nodes
        for node_info in nodes:
            node_id = node_info.get("node_id")
            if node_id != self.node_id and node_id not in self.connections:
                self._connect_to_node(node_info)
    
    def _handle_connect_ack(self, msg: Message, sender_id: int):
        """Handle CONNECT_ACK message (when we initiated connection)."""
        payload = msg.payload
        assigned_node_id = payload.get("node_id")
        group_id = payload.get("group_id")
        
        if assigned_node_id:
            self.set_group_id(group_id)
            # Update with received state
            all_nodes = payload.get("all_nodes", [])
            all_files = payload.get("all_files", {})
            all_blocks = payload.get("all_blocks", [])
            
            for node_info in all_nodes:
                self.node_registry.add_node(
                    node_info["node_id"], node_info["ip"], node_info["port"],
                    node_info["capacity_mb"], node_info["free_space_mb"]
                )
            
            self.file_registry.update_from_sync(all_files)
            self.block_table.update_from_sync(all_blocks)
            
            total_capacity = self.node_registry.get_total_capacity()
            self.block_table.resize(total_capacity)
    
    def _handle_reconnect_ack(self, msg: Message, sender_id: int):
        """Handle RECONNECT_ACK message."""
        payload = msg.payload
        # Similar to CONNECT_ACK
        self._handle_connect_ack(msg, sender_id)
    
    def connect_to_node(self, ip: str, port: int = 8888) -> bool:
        """Connect to a remote node."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            
            # Send CONNECT message
            group_id = self.get_group_id()
            connect_msg = Message(MessageType.CONNECT, self.node_id, {
                "group_id": group_id,
                "capacity_mb": self.block_manager.capacity_mb,
                "free_space_mb": self.block_manager.get_free_space()
            })
            self._send_message(sock, connect_msg)
            
            # Wait for response
            data = self._receive_message(sock)
            if data:
                msg = Message.deserialize(data)
                if msg.type == MessageType.CONNECT_ACK:
                    self._handle_connect_ack(msg, msg.sender_id)
                    # Store connection
                    assigned_id = msg.payload.get("node_id")
                    if assigned_id:
                        with self.lock:
                            self.connections[assigned_id] = sock
                        # Start thread to handle messages from this connection
                        thread = threading.Thread(
                            target=self._handle_connection_messages,
                            args=(sock, assigned_id),
                            daemon=True
                        )
                        thread.start()
                        return True
            
            sock.close()
            return False
            
        except Exception as e:
            logger.error(f"Failed to connect to {ip}:{port}: {e}")
            return False
    
    def _connect_to_node(self, node_info: Dict[str, Any]):
        """Connect to a node from discovery."""
        ip = node_info.get("ip")
        port = node_info.get("port", 8888)
        if ip:
            self.connect_to_node(ip, port)
    
    def _handle_connection_messages(self, sock: socket.socket, node_id: int):
        """Handle messages from an established connection."""
        try:
            while self.running:
                data = self._receive_message(sock)
                if not data:
                    break
                msg = Message.deserialize(data)
                self._handle_message(sock, msg, node_id)
        except Exception as e:
            logger.error(f"Error handling messages from node {node_id}: {e}")
        finally:
            with self.lock:
                if node_id in self.connections:
                    del self.connections[node_id]
    
    def _send_message(self, sock: socket.socket, msg: Message):
        """Send a message over TCP."""
        try:
            data = msg.serialize()
            # Send length prefix
            length = struct.pack('!I', len(data))
            sock.sendall(length + data)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
    
    def _receive_message(self, sock: socket.socket) -> Optional[bytes]:
        """Receive a message from TCP."""
        try:
            # Receive length prefix
            length_data = sock.recv(4)
            if len(length_data) < 4:
                return None
            length = struct.unpack('!I', length_data)[0]
            
            # Receive message data
            data = b''
            while len(data) < length:
                chunk = sock.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            
            return data
        except Exception as e:
            logger.debug(f"Error receiving message: {e}")
            return None
    
    def _get_all_nodes_info(self) -> list:
        """Get information about all nodes for synchronization."""
        nodes = self.node_registry.get_all_nodes()
        return [
            {
                "node_id": nid,
                "ip": info["ip"],
                "port": info["port"],
                "capacity_mb": info["total_capacity"],
                "free_space_mb": info["free_space"]
            }
            for nid, info in nodes.items()
        ]
    
    def send_to_node(self, node_id: int, msg: Message) -> bool:
        """Send a message to a specific node."""
        with self.lock:
            sock = self.connections.get(node_id)
            if sock:
                try:
                    self._send_message(sock, msg)
                    return True
                except:
                    return False
        return False
    
    def broadcast(self, msg: Message, exclude: list = None):
        """Broadcast message to all connected nodes."""
        exclude = exclude or []
        with self.lock:
            for node_id, sock in self.connections.items():
                if node_id not in exclude:
                    try:
                        self._send_message(sock, msg)
                    except:
                        pass

