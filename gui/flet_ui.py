"""
Flet GUI for P2P distributed storage system.
Modern interface with real-time updates.
"""
import flet as ft
import threading
import time
import os
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from datetime import datetime

logger = None  # Will be set by main


class P2PGUI:
    """Main GUI class for P2P storage system."""
    
    def __init__(self, node_id: int, capacity_mb: int, used_space_mb: int,
                 connect_callback: Callable, upload_callback: Callable,
                 download_callback: Callable, delete_callback: Callable,
                 change_capacity_callback: Callable,
                 get_nodes_callback: Callable, get_files_callback: Callable,
                 get_blocks_callback: Callable, is_connected_callback: Callable):
        """
        Initialize GUI.
        
        Args:
            node_id: Current node ID
            capacity_mb: Current capacity in MB
            used_space_mb: Used space in MB
            connect_callback: Function(ip: str) -> bool
            upload_callback: Function(file_path: str) -> bool
            download_callback: Function(file_name: str, save_path: str) -> bool
            delete_callback: Function(file_name: str) -> bool
            change_capacity_callback: Function(new_capacity: int) -> bool
            get_nodes_callback: Function() -> Dict
            get_files_callback: Function() -> Dict
            get_blocks_callback: Function() -> List
            is_connected_callback: Function() -> bool
        """
        self.node_id = node_id
        self.capacity_mb = capacity_mb
        self.used_space_mb = used_space_mb
        self.connect_callback = connect_callback
        self.upload_callback = upload_callback
        self.download_callback = download_callback
        self.delete_callback = delete_callback
        self.change_capacity_callback = change_capacity_callback
        self.get_nodes_callback = get_nodes_callback
        self.get_files_callback = get_files_callback
        self.get_blocks_callback = get_blocks_callback
        self.is_connected_callback = is_connected_callback
        
        self.page = None
        self.ip_input = None
        self.connect_button = None
        self.nodes_table = None
        self.files_table = None
        self.blocks_grid = None
        self.capacity_input = None
        self.change_capacity_button = None
        self.status_text = None
        self.running = True
    
    def run(self, page: ft.Page):
        """Run the GUI."""
        self.page = page
        page.title = "Sistema P2P Distribuido"
        page.window.width = 1400
        page.window.height = 900
        page.scroll = ft.ScrollMode.AUTO
        
        # Create UI
        self._create_ui()
        
        # Start update thread
        update_thread = threading.Thread(target=self._update_loop, daemon=True)
        update_thread.start()
    
    def _create_ui(self):
        """Create the UI components."""
        # Connection panel
        connection_panel = self._create_connection_panel()
        
        # Capacity panel
        capacity_panel = self._create_capacity_panel()
        
        # Nodes table
        self.nodes_table = self._create_nodes_table()
        
        # Files table
        self.files_table = self._create_files_table()
        
        # Blocks visualization
        self.blocks_grid = self._create_blocks_grid()
        
        # Status bar
        self.status_text = ft.Text("Listo", size=12)
        
        # Layout
        self.page.add(
            ft.Row([
                ft.Container(
                    content=ft.Column([
                        connection_panel,
                        capacity_panel,
                        ft.Divider(),
                        self.status_text
                    ], spacing=10),
                    width=300,
                    padding=10
                ),
                ft.VerticalDivider(),
                ft.Container(
                    content=ft.Column([
                        ft.Text("Nodos del Grupo", size=16, weight=ft.FontWeight.BOLD),
                        self.nodes_table,
                        ft.Divider(),
                        ft.Row([
                            ft.Text("Archivos", size=16, weight=ft.FontWeight.BOLD),
                            ft.ElevatedButton(
                                "Subir Archivo",
                                icon=ft.Icons.UPLOAD_FILE,
                                on_click=self._on_upload_click
                            )
                        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                        self.files_table,
                        ft.Divider(),
                        ft.Text("Tabla de Bloques", size=16, weight=ft.FontWeight.BOLD),
                        self.blocks_grid
                    ], spacing=10, scroll=ft.ScrollMode.AUTO),
                    expand=True,
                    padding=10
                )
            ], expand=True)
        )
    
    def _create_connection_panel(self):
        """Create connection panel."""
        self.ip_input = ft.TextField(
            label="IP del nodo",
            hint_text="Ej: 192.168.1.100",
            width=200
        )
        
        self.connect_button = ft.ElevatedButton(
            "Conectar",
            on_click=self._on_connect_click,
            width=200
        )
        
        return ft.Container(
            content=ft.Column([
                ft.Text("Conexión", size=14, weight=ft.FontWeight.BOLD),
                self.ip_input,
                self.connect_button
            ], spacing=5),
            padding=5
        )
    
    def _create_capacity_panel(self):
        """Create capacity panel."""
        capacity_info = ft.Text(f"Capacidad: {self.capacity_mb} MB\nUsado: {self.used_space_mb} MB\nLibre: {self.capacity_mb - self.used_space_mb} MB")
        
        self.capacity_input = ft.TextField(
            label="Nueva capacidad (MB)",
            hint_text="50-100",
            width=200,
            disabled=True
        )
        
        self.change_capacity_button = ft.ElevatedButton(
            "Cambiar Capacidad",
            on_click=self._on_change_capacity_click,
            width=200,
            disabled=True
        )
        
        return ft.Container(
            content=ft.Column([
                ft.Text("Capacidad", size=14, weight=ft.FontWeight.BOLD),
                capacity_info,
                self.capacity_input,
                self.change_capacity_button
            ], spacing=5),
            padding=5
        )
    
    def _create_nodes_table(self):
        """Create nodes table."""
        return ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("IP")),
                ft.DataColumn(ft.Text("Estado")),
                ft.DataColumn(ft.Text("Capacidad (MB)")),
                ft.DataColumn(ft.Text("Libre (MB)")),
                ft.DataColumn(ft.Text("Último Heartbeat"))
            ],
            rows=[]
        )
    
    def _create_files_table(self):
        """Create files table with actions."""
        return ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Nombre")),
                ft.DataColumn(ft.Text("Tamaño (bytes)")),
                ft.DataColumn(ft.Text("Bloques")),
                ft.DataColumn(ft.Text("Fecha Subida")),
                ft.DataColumn(ft.Text("Acciones"))
            ],
            rows=[]
        )
    
    def _create_blocks_grid(self):
        """Create blocks visualization grid."""
        return ft.GridView(
            runs_count=20,
            max_extent=50,
            child_aspect_ratio=1.0,
            spacing=2,
            run_spacing=2
        )
    
    def _on_connect_click(self, e):
        """Handle connect button click."""
        ip = self.ip_input.value.strip()
        if not ip:
            self._show_status("Error: Ingrese una IP válida", is_error=True)
            return
        
        self.connect_button.disabled = True
        self._show_status(f"Conectando a {ip}...")
        
        def connect():
            success = self.connect_callback(ip)
            self.page.run_task(lambda: self._on_connect_result(success, ip))
        
        threading.Thread(target=connect, daemon=True).start()
    
    def _on_connect_result(self, success: bool, ip: str):
        """Handle connect result."""
        self.connect_button.disabled = False
        if success:
            self._show_status(f"Conectado a {ip}")
            self.ip_input.value = ""
        else:
            self._show_status(f"Error al conectar a {ip}", is_error=True)
    
    def _on_change_capacity_click(self, e):
        """Handle change capacity button click."""
        try:
            new_capacity = int(self.capacity_input.value)
            if self.change_capacity_callback(new_capacity):
                self._show_status(f"Capacidad cambiada a {new_capacity} MB")
                self.capacity_input.value = ""
            else:
                self._show_status("Error: No se pudo cambiar la capacidad", is_error=True)
        except ValueError:
            self._show_status("Error: Ingrese un número válido", is_error=True)
    
    def _update_loop(self):
        """Update UI periodically."""
        while self.running:
            try:
                time.sleep(0.5)  # Update every 500ms
                if self.page:
                    self.page.run_task(self._update_ui)
            except Exception as e:
                if logger:
                    logger.error(f"Error in update loop: {e}")
    
    def _update_ui(self):
        """Update UI components."""
        try:
            # Update nodes table
            nodes = self.get_nodes_callback()
            self._update_nodes_table(nodes)
            
            # Update files table
            files = self.get_files_callback()
            self._update_files_table(files)
            
            # Update blocks grid
            blocks = self.get_blocks_callback()
            self._update_blocks_grid(blocks)
            
            # Update capacity panel
            is_connected = self.is_connected_callback()
            self.capacity_input.disabled = is_connected
            self.change_capacity_button.disabled = is_connected
            
        except Exception as e:
            if logger:
                logger.error(f"Error updating UI: {e}")
    
    def _update_nodes_table(self, nodes: Dict[int, Dict[str, Any]]):
        """Update nodes table."""
        rows = []
        for node_id, info in nodes.items():
            status = info.get("status", "unknown")
            status_color = ft.Colors.GREEN if status == "online" else ft.Colors.RED
            last_heartbeat = info.get("last_heartbeat", 0)
            if last_heartbeat:
                time_ago = time.time() - last_heartbeat
                heartbeat_str = f"{time_ago:.1f}s"
            else:
                heartbeat_str = "N/A"
            
            rows.append(ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(str(node_id))),
                    ft.DataCell(ft.Text(info.get("ip", "N/A"))),
                    ft.DataCell(ft.Text(status, color=status_color)),
                    ft.DataCell(ft.Text(str(info.get("total_capacity", 0)))),
                    ft.DataCell(ft.Text(str(info.get("free_space", 0)))),
                    ft.DataCell(ft.Text(heartbeat_str))
                ]
            ))
        
        self.nodes_table.rows = rows
        self.nodes_table.update()
    
    def _update_files_table(self, files: Dict[str, Dict[str, Any]]):
        """Update files table."""
        rows = []
        for file_name, info in files.items():
            size = info.get("size", 0)
            num_blocks = info.get("num_blocks", 0)
            upload_date = info.get("upload_date", 0)
            if upload_date:
                date_str = datetime.fromtimestamp(upload_date).strftime("%Y-%m-%d %H:%M:%S")
            else:
                date_str = "N/A"
            
            # Action buttons
            download_btn = ft.IconButton(
                ft.Icons.DOWNLOAD,
                on_click=lambda e, fn=file_name: self._on_download_click(fn),
                tooltip="Descargar"
            )
            delete_btn = ft.IconButton(
                ft.Icons.DELETE,
                on_click=lambda e, fn=file_name: self._on_delete_click(fn),
                tooltip="Eliminar",
                icon_color=ft.Colors.RED
            )
            
            rows.append(ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(file_name)),
                    ft.DataCell(ft.Text(str(size))),
                    ft.DataCell(ft.Text(str(num_blocks))),
                    ft.DataCell(ft.Text(date_str)),
                    ft.DataCell(ft.Row([download_btn, delete_btn], spacing=5))
                ]
            ))
        
        self.files_table.rows = rows
        self.files_table.update()
    
    def _update_blocks_grid(self, blocks: list):
        """Update blocks visualization grid."""
        self.blocks_grid.controls = []
        
        for block in blocks:
            block_id = block.get("block_id")
            block_type = block.get("type")
            status = block.get("status")
            node_id = block.get("node_id")
            file_name = block.get("file_name")
            
            # Determine color
            if status == "used":
                if block_type == "original":
                    color = ft.Colors.BLUE
                elif block_type == "copy":
                    color = ft.Colors.RED
                else:
                    color = ft.Colors.GREY
            elif status == "free":
                color = ft.Colors.GREY_300
            else:
                color = ft.Colors.ORANGE
            
            # Create tooltip text
            tooltip_text = f"ID: {block_id}\nTipo: {block_type or 'N/A'}\nNodo: {node_id or 'N/A'}\nArchivo: {file_name or 'Libre'}\nEstado: {status}"
            
            block_tile = ft.Container(
                content=ft.Tooltip(
                    message=tooltip_text,
                    content=ft.Container(
                        width=50,
                        height=50,
                        bgcolor=color,
                        border=ft.border.all(1, ft.Colors.BLACK)
                    )
                ),
                width=50,
                height=50
            )
            
            self.blocks_grid.controls.append(block_tile)
        
        self.blocks_grid.update()
    
    def _on_download_click(self, file_name: str):
        """Handle download button click."""
        # Open file dialog to choose save location
        # For now, save to Downloads folder
        downloads_path = Path.home() / "Downloads"
        save_path = downloads_path / file_name
        
        self._show_status(f"Descargando {file_name}...")
        
        def download():
            success = self.download_callback(file_name, str(save_path))
            self.page.run_task(lambda: self._on_download_result(success, file_name))
        
        threading.Thread(target=download, daemon=True).start()
    
    def _on_download_result(self, success: bool, file_name: str):
        """Handle download result."""
        if success:
            self._show_status(f"Archivo {file_name} descargado")
        else:
            self._show_status(f"Error al descargar {file_name}", is_error=True)
    
    def _on_delete_click(self, file_name: str):
        """Handle delete button click."""
        self._show_status(f"Eliminando {file_name}...")
        
        def delete():
            success = self.delete_callback(file_name)
            self.page.run_task(lambda: self._on_delete_result(success, file_name))
        
        threading.Thread(target=delete, daemon=True).start()
    
    def _on_delete_result(self, success: bool, file_name: str):
        """Handle delete result."""
        if success:
            self._show_status(f"Archivo {file_name} eliminado")
        else:
            self._show_status(f"Error al eliminar {file_name}", is_error=True)
    
    def _on_upload_click(self, e):
        """Handle upload button click."""
        # Flet file picker
        def on_file_selected(e: ft.FilePickerResultEvent):
            if e.files and len(e.files) > 0:
                file_path = e.files[0].path
                self._show_status(f"Subiendo {e.files[0].name}...")
                
                def upload():
                    success = self.upload_callback(file_path)
                    self.page.run_task(lambda: self._on_upload_result(success, e.files[0].name))
                
                threading.Thread(target=upload, daemon=True).start()
        
        file_picker = ft.FilePicker(on_result=on_file_selected)
        self.page.overlay.append(file_picker)
        file_picker.pick_files()
        self.page.update()
    
    def _on_upload_result(self, success: bool, file_name: str):
        """Handle upload result."""
        if success:
            self._show_status(f"Archivo {file_name} subido exitosamente")
        else:
            self._show_status(f"Error al subir {file_name}", is_error=True)
    
    def _show_status(self, message: str, is_error: bool = False):
        """Show status message."""
        if self.status_text:
            self.status_text.value = message
            self.status_text.color = ft.Colors.RED if is_error else ft.Colors.BLACK
            self.status_text.update()
    
    def add_upload_button(self):
        """Add upload button to files section."""
        upload_button = ft.ElevatedButton(
            "Subir Archivo",
            icon=ft.Icons.UPLOAD_FILE,
            on_click=self._on_upload_click
        )
        # This would be added to the files section
        # For now, we'll handle it in the main integration


def run_gui(gui_instance: P2PGUI):
    """Run the Flet app (must be called from main thread)."""
    try:
        # Flet must run in main thread, so we use it directly
        ft.app(target=gui_instance.run)
    except Exception as e:
        if logger:
            logger.error(f"Failed to start GUI: {e}")
        raise

