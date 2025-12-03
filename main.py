"""
Main entry point for P2P distributed storage system.
Integrates all components and manages system lifecycle.
"""
import logging
import threading
import time
import uuid
import os
from pathlib import Path
from typing import Optional, Dict, Any, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import all components
from utils.persistence import load_config, save_config, load_node_state, save_node_state
from metadata.node_registry import NodeRegistry
from metadata.block_table import BlockTable
from metadata.file_registry import FileRegistry
from storage.block_manager import BlockManager
from storage.file_manager import FileManager
from replication.partitioner import Partitioner
from network.tcp_handler import TCPHandler
from network.udp_handler import UDPHandler
from gui.flet_ui import P2PGUI, run_gui

# Set logger for GUI module
import gui.flet_ui
gui.flet_ui.logger = logger


class P2PNode:
    """Main P2P node class that integrates all components."""
    
    def __init__(self):
        """Initialize the P2P node."""
        # Load configuration
        config = load_config()
        self.capacity_mb = config.get("capacity_mb", 50)
        self.port = config.get("port", 8888)
        self.udp_port = 8889
        
        # Load node state
        node_state = load_node_state()
        if node_state:
            self.node_id = node_state.get("node_id")
            self.group_id = node_state.get("group_id")
        else:
            self.node_id = None
            self.group_id = None
        
        # Initialize components
        self.block_manager = BlockManager(self.capacity_mb)
        self.node_registry = NodeRegistry()
        self.block_table = BlockTable()
        self.file_registry = FileRegistry()
        self.partitioner = Partitioner(self.block_table, self.node_registry)
        
        # Initialize file manager with callbacks (will be set after TCP handler is created)
        self.file_manager = None  # Will be initialized after TCP handler
        
        # Network handlers (will be initialized after node_id is set)
        self.tcp_handler: Optional[TCPHandler] = None
        self.udp_handler: Optional[UDPHandler] = None
        
        # Threads
        self.heartbeat_monitor_thread: Optional[threading.Thread] = None
        self.mesh_maintenance_thread: Optional[threading.Thread] = None
        self.running = False
        
        # GUI
        self.gui: Optional[P2PGUI] = None
        
        # Next node ID counter
        self.next_node_id = 1
    
    def start(self):
        """Start the P2P node."""
        logger.info("Starting P2P node...")
        
        # Assign node ID if not set
        if self.node_id is None:
            self.node_id = 1
            logger.info(f"Assigned node_id={self.node_id}")
        else:
            logger.info(f"Using existing node_id={self.node_id}")
        
        # Add self to registry
        self.node_registry.add_node(
            self.node_id,
            "127.0.0.1",  # Will be updated with actual IP
            self.port,
            self.capacity_mb,
            self.block_manager.get_free_space()
        )
        
        # Initialize block table with current capacity
        self.block_table.resize(self.capacity_mb)
        
        # Initialize file manager with callbacks
        self.file_manager = FileManager(
            self.block_manager,
            self.block_table,
            self.file_registry,
            self.partitioner,
            send_message_callback=self._send_message_to_nodes,
            send_to_node_callback=self._send_to_node
        )
        
        # Initialize network handlers
        self.tcp_handler = TCPHandler(
            self.node_id,
            self.port,
            self.node_registry,
            self.block_table,
            self.file_registry,
            self.block_manager,
            self.file_manager,
            get_group_id_callback=lambda: self.group_id,
            set_group_id_callback=self._set_group_id,
            get_next_node_id_callback=self._get_next_node_id
        )
        
        self.udp_handler = UDPHandler(self.node_id, self.udp_port, self.node_registry)
        
        # Start network handlers
        self.tcp_handler.start()
        self.udp_handler.start()
        
        # Start maintenance threads
        self.running = True
        self.heartbeat_monitor_thread = threading.Thread(
            target=self._heartbeat_monitor_loop,
            daemon=True
        )
        self.heartbeat_monitor_thread.start()
        
        self.mesh_maintenance_thread = threading.Thread(
            target=self._mesh_maintenance_loop,
            daemon=True
        )
        self.mesh_maintenance_thread.start()
        
        logger.info("P2P node started successfully")
    
    def init_gui(self):
        """Initialize GUI (must be called from main thread)."""
        self.gui = P2PGUI(
            node_id=self.node_id,
            capacity_mb=self.capacity_mb,
            used_space_mb=self.block_manager.get_used_space(),
            connect_callback=self._gui_connect,
            upload_callback=self._gui_upload,
            download_callback=self._gui_download,
            delete_callback=self._gui_delete,
            change_capacity_callback=self._gui_change_capacity,
            get_nodes_callback=self._gui_get_nodes,
            get_files_callback=self._gui_get_files,
            get_blocks_callback=self._gui_get_blocks,
            is_connected_callback=self._gui_is_connected
        )
        return self.gui
    
    def _set_group_id(self, group_id: str):
        """Set group ID and persist it."""
        self.group_id = group_id
        if self.node_id:
            save_node_state(self.node_id, group_id)
    
    def _get_next_node_id(self) -> int:
        """Get next available node ID."""
        existing_ids = set(self.node_registry.get_all_nodes().keys())
        while self.next_node_id in existing_ids or self.next_node_id == self.node_id:
            self.next_node_id += 1
        assigned_id = self.next_node_id
        self.next_node_id += 1
        return assigned_id
    
    def _send_message_to_nodes(self, msg):
        """Send message to all connected nodes."""
        if self.tcp_handler:
            self.tcp_handler.broadcast(msg)
    
    def _send_to_node(self, node_id: int, msg) -> bool:
        """Send message to a specific node."""
        if self.tcp_handler:
            return self.tcp_handler.send_to_node(node_id, msg)
        return False
    
    def _heartbeat_monitor_loop(self):
        """Monitor heartbeats and mark nodes as offline."""
        while self.running:
            try:
                self.node_registry.check_timeouts()
                
                # Update block table if capacity changed
                total_capacity = self.node_registry.get_total_capacity()
                current_size = len(self.block_table.get_all_blocks())
                if total_capacity != current_size:
                    self.block_table.resize(total_capacity)
                
                # Mark blocks of offline nodes as unavailable
                online_nodes = set(self.node_registry.get_online_nodes().keys())
                all_nodes = set(self.node_registry.get_all_nodes().keys())
                offline_nodes = all_nodes - online_nodes - {self.node_id}
                for node_id in offline_nodes:
                    self.block_table.mark_node_blocks_unavailable(node_id)
                
                time.sleep(3)  # Check every 3 seconds
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                time.sleep(3)
    
    def _mesh_maintenance_loop(self):
        """Maintain mesh topology by ensuring connections to all nodes."""
        while self.running:
            try:
                online_nodes = self.node_registry.get_online_nodes()
                if self.tcp_handler:
                    # Check if we have connections to all online nodes
                    with self.tcp_handler.lock:
                        connected_ids = set(self.tcp_handler.connections.keys())
                    
                    for node_id, node_info in online_nodes.items():
                        if node_id != self.node_id and node_id not in connected_ids:
                            # Try to establish connection
                            ip = node_info["ip"]
                            port = node_info.get("port", self.port)
                            logger.info(f"Establishing connection to node {node_id} at {ip}:{port}")
                            self.tcp_handler.connect_to_node(ip, port)
                
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"Error in mesh maintenance: {e}")
                time.sleep(5)
    
    # GUI callbacks
    def _gui_connect(self, ip: str) -> bool:
        """Handle GUI connect request."""
        try:
            # Validate IP
            if not ip or ip.strip() == "":
                logger.error("IP vacía")
                return False
            
            ip = ip.strip()
            
            # Don't allow connecting to localhost/127.0.0.1 (self)
            if ip in ["127.0.0.1", "localhost", "::1"]:
                logger.warning("No se puede conectar a localhost (es el mismo nodo)")
                return False
            
            if self.tcp_handler:
                logger.info(f"Intentando conectar a {ip}:{self.port}")
                success = self.tcp_handler.connect_to_node(ip, self.port)
                if success:
                    # Update block table size
                    total_capacity = self.node_registry.get_total_capacity()
                    self.block_table.resize(total_capacity)
                    logger.info(f"Conexión exitosa a {ip}")
                else:
                    logger.warning(f"Fallo al conectar a {ip}:{self.port}")
                return success
            return False
        except ConnectionRefusedError:
            logger.error(f"Conexión rechazada por {ip}:{self.port} - Verifique que el nodo esté ejecutándose")
            return False
        except TimeoutError:
            logger.error(f"Timeout al conectar a {ip}:{self.port} - Verifique la IP y el firewall")
            return False
        except OSError as e:
            logger.error(f"Error de red al conectar a {ip}:{e}")
            return False
        except Exception as e:
            logger.error(f"Error conectando a {ip}: {e}", exc_info=True)
            return False
    
    def _gui_upload(self, file_path: str) -> bool:
        """Handle GUI upload request."""
        try:
            return self.file_manager.upload_file(file_path, self.node_id)
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return False
    
    def _gui_download(self, file_name: str, save_path: str) -> bool:
        """Handle GUI download request."""
        try:
            return self.file_manager.download_file(file_name, save_path, self.node_id)
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            return False
    
    def _gui_delete(self, file_name: str) -> bool:
        """Handle GUI delete request."""
        try:
            return self.file_manager.delete_file(file_name, self.node_id)
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False
    
    def _gui_change_capacity(self, new_capacity: int) -> bool:
        """Handle GUI capacity change request."""
        try:
            # Validate capacity
            if new_capacity < 50 or new_capacity > 100:
                return False
            
            used_space = self.block_manager.get_used_space()
            if new_capacity < used_space:
                return False
            
            # Check if connected
            if self._gui_is_connected():
                return False  # Can't change capacity while connected
            
            # Update capacity
            self.capacity_mb = new_capacity
            self.block_manager.set_capacity(new_capacity)
            save_config(self.capacity_mb, self.port)
            
            # Update block table
            self.block_table.resize(new_capacity)
            
            return True
        except Exception as e:
            logger.error(f"Error changing capacity: {e}")
            return False
    
    def _gui_get_nodes(self) -> Dict[int, Dict[str, Any]]:
        """Get nodes for GUI."""
        return self.node_registry.get_all_nodes()
    
    def _gui_get_files(self) -> Dict[str, Dict[str, Any]]:
        """Get files for GUI."""
        return self.file_registry.get_all_files()
    
    def _gui_get_blocks(self) -> List[Dict[str, Any]]:
        """Get blocks for GUI."""
        return self.block_table.get_all_blocks()
    
    def _gui_is_connected(self) -> bool:
        """Check if node is connected to any other node."""
        online_nodes = self.node_registry.get_online_nodes()
        return len(online_nodes) > 1  # More than just self
    
    def stop(self):
        """Stop the P2P node."""
        logger.info("Stopping P2P node...")
        self.running = False
        
        if self.tcp_handler:
            self.tcp_handler.stop()
        if self.udp_handler:
            self.udp_handler.stop()
        
        logger.info("P2P node stopped")


def main():
    """Main entry point."""
    node = P2PNode()
    
    try:
        node.start()
        
        # Initialize and run GUI in main thread (Flet requires main thread)
        gui = node.init_gui()
        from gui.flet_ui import run_gui
        run_gui(gui)  # This blocks until GUI is closed
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        node.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        node.stop()
        raise


if __name__ == "__main__":
    main()

