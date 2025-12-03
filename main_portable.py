"""
Versión portable del sistema P2P sin interfaz gráfica.
Interfaz de consola con menú interactivo.
"""
import logging
import threading
import time
import uuid
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

# Configure logging (solo errores para consola)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
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

# Deshabilitar logs de otros módulos
logging.getLogger('network').setLevel(logging.WARNING)
logging.getLogger('storage').setLevel(logging.WARNING)
logging.getLogger('metadata').setLevel(logging.WARNING)
logging.getLogger('replication').setLevel(logging.WARNING)


class P2PNodeConsole:
    """P2P node con interfaz de consola."""
    
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
        
        # Initialize file manager with callbacks
        self.file_manager = FileManager(
            self.block_manager,
            self.block_table,
            self.file_registry,
            self.partitioner,
            send_message_callback=self._send_message_to_nodes,
            send_to_node_callback=self._send_to_node
        )
        
        # Network handlers
        self.tcp_handler: Optional[TCPHandler] = None
        self.udp_handler: Optional[UDPHandler] = None
        
        # Threads
        self.heartbeat_monitor_thread: Optional[threading.Thread] = None
        self.mesh_maintenance_thread: Optional[threading.Thread] = None
        self.running = False
        
        # Next node ID counter
        self.next_node_id = 1
    
    def start(self):
        """Start the P2P node."""
        print("=" * 60)
        print("Sistema P2P Distribuido - Versión Consola")
        print("=" * 60)
        
        # Assign node ID if not set
        if self.node_id is None:
            self.node_id = 1
            print(f"✓ Nodo asignado: ID={self.node_id}")
        else:
            print(f"✓ Nodo recuperado: ID={self.node_id}")
            if self.group_id:
                print(f"✓ Grupo: {self.group_id[:8]}...")
        
        # Add self to registry
        self.node_registry.add_node(
            self.node_id,
            "127.0.0.1",
            self.port,
            self.capacity_mb,
            self.block_manager.get_free_space()
        )
        
        # Initialize block table
        self.block_table.resize(self.capacity_mb)
        
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
        try:
            self.tcp_handler.start()
            self.udp_handler.start()
            print(f"✓ Servidor TCP iniciado en puerto {self.port}")
            print(f"✓ Servidor UDP iniciado en puerto {self.udp_port}")
        except Exception as e:
            print(f"✗ Error al iniciar servidores: {e}")
            return False
        
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
        
        print("✓ Sistema iniciado correctamente")
        print()
        return True
    
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
                total_capacity = self.node_registry.get_total_capacity()
                current_size = len(self.block_table.get_all_blocks())
                if total_capacity != current_size:
                    self.block_table.resize(total_capacity)
                online_nodes = set(self.node_registry.get_online_nodes().keys())
                all_nodes = set(self.node_registry.get_all_nodes().keys())
                offline_nodes = all_nodes - online_nodes - {self.node_id}
                for node_id in offline_nodes:
                    self.block_table.mark_node_blocks_unavailable(node_id)
                time.sleep(3)
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                time.sleep(3)
    
    def _mesh_maintenance_loop(self):
        """Maintain mesh topology."""
        while self.running:
            try:
                online_nodes = self.node_registry.get_online_nodes()
                if self.tcp_handler:
                    with self.tcp_handler.lock:
                        connected_ids = set(self.tcp_handler.connections.keys())
                    for node_id, node_info in online_nodes.items():
                        if node_id != self.node_id and node_id not in connected_ids:
                            ip = node_info["ip"]
                            port = node_info.get("port", self.port)
                            self.tcp_handler.connect_to_node(ip, port)
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error in mesh maintenance: {e}")
                time.sleep(5)
    
    def show_menu(self):
        """Show main menu."""
        print("\n" + "=" * 60)
        print("MENÚ PRINCIPAL")
        print("=" * 60)
        print(f"Nodo ID: {self.node_id}")
        print(f"Capacidad: {self.capacity_mb} MB | Usado: {self.block_manager.get_used_space()} MB | Libre: {self.block_manager.get_free_space()} MB")
        
        online_count = len(self.node_registry.get_online_nodes()) - 1  # Exclude self
        print(f"Nodos conectados: {online_count}")
        print()
        print("1. Conectar a otro nodo")
        print("2. Ver nodos del grupo")
        print("3. Subir archivo")
        print("4. Ver archivos")
        print("5. Descargar archivo")
        print("6. Eliminar archivo")
        print("7. Ver tabla de bloques")
        print("8. Cambiar capacidad")
        print("9. Estado del sistema")
        print("0. Salir")
        print("=" * 60)
    
    def connect_to_node(self):
        """Connect to another node."""
        ip = input("Ingrese la IP del nodo a conectar: ").strip()
        if not ip:
            print("✗ IP inválida")
            return
        
        print(f"Conectando a {ip}...")
        try:
            if self.tcp_handler:
                success = self.tcp_handler.connect_to_node(ip, self.port)
                if success:
                    total_capacity = self.node_registry.get_total_capacity()
                    self.block_table.resize(total_capacity)
                    print(f"✓ Conectado exitosamente a {ip}")
                else:
                    print(f"✗ Error al conectar a {ip}")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def show_nodes(self):
        """Show all nodes."""
        nodes = self.node_registry.get_all_nodes()
        if len(nodes) <= 1:
            print("No hay otros nodos conectados")
            return
        
        print("\n" + "-" * 60)
        print("NODOS DEL GRUPO")
        print("-" * 60)
        print(f"{'ID':<5} {'IP':<20} {'Estado':<10} {'Capacidad':<12} {'Libre':<10} {'Heartbeat':<10}")
        print("-" * 60)
        
        for node_id, info in sorted(nodes.items()):
            if node_id == self.node_id:
                continue
            status = info.get("status", "unknown")
            status_str = "✓ ONLINE" if status == "online" else "✗ OFFLINE"
            ip = info.get("ip", "N/A")
            capacity = info.get("total_capacity", 0)
            free = info.get("free_space", 0)
            last_hb = info.get("last_heartbeat", 0)
            if last_hb:
                time_ago = time.time() - last_hb
                hb_str = f"{time_ago:.1f}s"
            else:
                hb_str = "N/A"
            
            print(f"{node_id:<5} {ip:<20} {status_str:<10} {capacity:<12}MB {free:<10}MB {hb_str:<10}")
        
        print("-" * 60)
    
    def upload_file(self):
        """Upload a file."""
        file_path = input("Ingrese la ruta del archivo a subir: ").strip()
        if not file_path:
            print("✗ Ruta inválida")
            return
        
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            print(f"✗ Archivo no encontrado: {file_path}")
            return
        
        print(f"Subiendo {file_path_obj.name}...")
        try:
            success = self.file_manager.upload_file(str(file_path), self.node_id)
            if success:
                print(f"✓ Archivo subido exitosamente: {file_path_obj.name}")
            else:
                print(f"✗ Error al subir el archivo")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def show_files(self):
        """Show all files."""
        files = self.file_registry.get_all_files()
        if not files:
            print("No hay archivos en el sistema")
            return
        
        print("\n" + "-" * 80)
        print("ARCHIVOS")
        print("-" * 80)
        print(f"{'Nombre':<30} {'Tamaño':<15} {'Bloques':<10} {'Fecha Subida':<20}")
        print("-" * 80)
        
        for file_name, info in sorted(files.items()):
            size = info.get("size", 0)
            num_blocks = info.get("num_blocks", 0)
            upload_date = info.get("upload_date", 0)
            if upload_date:
                date_str = datetime.fromtimestamp(upload_date).strftime("%Y-%m-%d %H:%M:%S")
            else:
                date_str = "N/A"
            
            # Truncate long names
            display_name = file_name[:27] + "..." if len(file_name) > 30 else file_name
            print(f"{display_name:<30} {size:<15} {num_blocks:<10} {date_str:<20}")
        
        print("-" * 80)
    
    def download_file(self):
        """Download a file."""
        self.show_files()
        file_name = input("\nIngrese el nombre del archivo a descargar: ").strip()
        if not file_name:
            print("✗ Nombre inválido")
            return
        
        save_path = input("Ingrese la ruta donde guardar (Enter para Downloads): ").strip()
        if not save_path:
            downloads_path = Path.home() / "Downloads"
            save_path = str(downloads_path / file_name)
        else:
            save_path = str(Path(save_path) / file_name)
        
        print(f"Descargando {file_name}...")
        try:
            success = self.file_manager.download_file(file_name, save_path, self.node_id)
            if success:
                print(f"✓ Archivo descargado exitosamente: {save_path}")
            else:
                print(f"✗ Error al descargar el archivo")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def delete_file(self):
        """Delete a file."""
        self.show_files()
        file_name = input("\nIngrese el nombre del archivo a eliminar: ").strip()
        if not file_name:
            print("✗ Nombre inválido")
            return
        
        confirm = input(f"¿Está seguro de eliminar '{file_name}'? (s/n): ").strip().lower()
        if confirm != 's':
            print("Operación cancelada")
            return
        
        print(f"Eliminando {file_name}...")
        try:
            success = self.file_manager.delete_file(file_name, self.node_id)
            if success:
                print(f"✓ Archivo eliminado exitosamente")
            else:
                print(f"✗ Error al eliminar el archivo")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def show_blocks(self):
        """Show block table."""
        blocks = self.block_table.get_all_blocks()
        if not blocks:
            print("No hay bloques en el sistema")
            return
        
        used_blocks = [b for b in blocks if b.get("status") == "used"]
        free_blocks = [b for b in blocks if b.get("status") == "free"]
        unavailable_blocks = [b for b in blocks if b.get("status") == "unavailable"]
        
        print("\n" + "-" * 80)
        print("TABLA DE BLOQUES")
        print("-" * 80)
        print(f"Total de bloques: {len(blocks)}")
        print(f"  - Usados: {len(used_blocks)}")
        print(f"  - Libres: {len(free_blocks)}")
        print(f"  - No disponibles: {len(unavailable_blocks)}")
        print()
        
        # Show some sample blocks
        print("Muestra de bloques (primeros 20):")
        print(f"{'ID':<8} {'Tipo':<10} {'Nodo':<8} {'Archivo':<25} {'Estado':<12}")
        print("-" * 80)
        
        for i, block in enumerate(blocks[:20]):
            block_id = block.get("block_id", "N/A")
            block_type = block.get("type", "N/A")
            node_id = block.get("node_id", "N/A")
            file_name = block.get("file_name", "Libre")
            status = block.get("status", "unknown")
            
            # Truncate long names
            display_file = file_name[:23] + "..." if len(file_name) > 25 else file_name
            
            type_str = "ORIGINAL" if block_type == "original" else "COPIA" if block_type == "copy" else "N/A"
            status_str = "USADO" if status == "used" else "LIBRE" if status == "free" else "NO DISP"
            
            print(f"{block_id:<8} {type_str:<10} {node_id:<8} {display_file:<25} {status_str:<12}")
        
        if len(blocks) > 20:
            print(f"... y {len(blocks) - 20} bloques más")
        print("-" * 80)
    
    def change_capacity(self):
        """Change node capacity."""
        is_connected = len(self.node_registry.get_online_nodes()) > 1
        if is_connected:
            print("✗ No se puede cambiar la capacidad mientras está conectado a otros nodos")
            print("  Desconéctese primero o cierre las conexiones")
            return
        
        used_space = self.block_manager.get_used_space()
        print(f"\nCapacidad actual: {self.capacity_mb} MB")
        print(f"Espacio usado: {used_space} MB")
        print(f"Espacio libre: {self.block_manager.get_free_space()} MB")
        print(f"\nLa nueva capacidad debe estar entre 50-100 MB")
        if used_space > 0:
            print(f"Y no puede ser menor a {used_space} MB (espacio usado)")
            if self.capacity_mb == 100 and used_space == self.capacity_mb:
                print("⚠ La capacidad está al máximo (100 MB) y está llena. No se puede modificar.")
                return
        
        try:
            new_capacity = int(input("Ingrese la nueva capacidad (MB): ").strip())
            
            if new_capacity < 50 or new_capacity > 100:
                print("✗ La capacidad debe estar entre 50-100 MB")
                return
            
            if new_capacity < used_space:
                print(f"✗ La capacidad no puede ser menor al espacio usado ({used_space} MB)")
                if used_space <= 100:
                    print(f"  Puede ajustar a {used_space} MB o aumentar a 100 MB")
                return
            
            # Update capacity
            self.capacity_mb = new_capacity
            self.block_manager.set_capacity(new_capacity)
            save_config(self.capacity_mb, self.port)
            self.block_table.resize(new_capacity)
            
            print(f"✓ Capacidad cambiada a {new_capacity} MB")
        except ValueError:
            print("✗ Ingrese un número válido")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def show_status(self):
        """Show system status."""
        print("\n" + "=" * 60)
        print("ESTADO DEL SISTEMA")
        print("=" * 60)
        print(f"Nodo ID: {self.node_id}")
        print(f"Grupo ID: {self.group_id[:8] + '...' if self.group_id else 'Ninguno'}")
        print(f"Puerto TCP: {self.port}")
        print(f"Puerto UDP: {self.udp_port}")
        print()
        print("Capacidad:")
        print(f"  Total: {self.capacity_mb} MB")
        print(f"  Usado: {self.block_manager.get_used_space()} MB")
        print(f"  Libre: {self.block_manager.get_free_space()} MB")
        print()
        
        nodes = self.node_registry.get_all_nodes()
        online_nodes = self.node_registry.get_online_nodes()
        print(f"Nodos:")
        print(f"  Total: {len(nodes)}")
        print(f"  Online: {len(online_nodes)}")
        print(f"  Offline: {len(nodes) - len(online_nodes)}")
        print()
        
        files = self.file_registry.get_all_files()
        blocks = self.block_table.get_all_blocks()
        used_blocks = [b for b in blocks if b.get("status") == "used"]
        
        print(f"Archivos: {len(files)}")
        print(f"Bloques:")
        print(f"  Total: {len(blocks)}")
        print(f"  Usados: {len(used_blocks)}")
        print(f"  Libres: {len(blocks) - len(used_blocks)}")
        print()
        
        if self.tcp_handler:
            with self.tcp_handler.lock:
                connections = len(self.tcp_handler.connections)
            print(f"Conexiones TCP activas: {connections}")
        
        print("=" * 60)
    
    def run(self):
        """Run the console interface."""
        if not self.start():
            return
        
        try:
            while True:
                self.show_menu()
                choice = input("Seleccione una opción: ").strip()
                
                if choice == "0":
                    print("\nSaliendo...")
                    break
                elif choice == "1":
                    self.connect_to_node()
                elif choice == "2":
                    self.show_nodes()
                elif choice == "3":
                    self.upload_file()
                elif choice == "4":
                    self.show_files()
                elif choice == "5":
                    self.download_file()
                elif choice == "6":
                    self.delete_file()
                elif choice == "7":
                    self.show_blocks()
                elif choice == "8":
                    self.change_capacity()
                elif choice == "9":
                    self.show_status()
                else:
                    print("✗ Opción inválida")
                
                input("\nPresione Enter para continuar...")
        
        except KeyboardInterrupt:
            print("\n\nInterrupción recibida. Saliendo...")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the P2P node."""
        print("\nDeteniendo nodo...")
        self.running = False
        
        if self.tcp_handler:
            self.tcp_handler.stop()
        if self.udp_handler:
            self.udp_handler.stop()
        
        print("✓ Nodo detenido")


def main():
    """Main entry point."""
    node = P2PNodeConsole()
    node.run()


if __name__ == "__main__":
    main()

