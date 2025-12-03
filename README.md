# Sistema de Almacenamiento Distribuido P2P Tolerante a Fallas

Sistema de almacenamiento distribuido completamente peer-to-peer (P2P) que aprovecha el espacio en disco de múltiples computadoras sin servidor central.

## Características

- **Topología P2P completa**: Sin servidor central, todos los nodos son iguales
- **Tolerancia a fallas**: Réplicas de bloques en múltiples nodos
- **Interfaz gráfica moderna**: GUI con Flet para gestión visual
- **Detección automática**: Descubrimiento de nodos y mantenimiento de malla
- **Reconexión automática**: Los nodos recuperan su posición al reconectarse
- **Persistencia**: Estado y metadatos guardados en archivos JSON

## Requisitos

- Python 3.8 o superior
- Flet (se instala automáticamente con requirements.txt)

## Instalación

1. Clonar o descargar el proyecto
2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Uso

### Versión con Interfaz Gráfica

Ejecuta:
```bash
python main.py
```

Se abrirá una ventana con la interfaz gráfica (Flet).

### Versión Consola (Portable)

Para usar sin interfaz gráfica, ejecuta:
```bash
python main_portable.py
```

Se mostrará un menú interactivo en consola con todas las funcionalidades:
- Conectar a otro nodo
- Ver nodos del grupo
- Subir archivo
- Ver archivos
- Descargar archivo
- Eliminar archivo
- Ver tabla de bloques
- Cambiar capacidad
- Estado del sistema

### Conectar nodos

1. En el primer nodo, introduce la IP del segundo nodo y presiona "Conectar"
2. Los primeros dos nodos se asignarán automáticamente como `nodo1` y `nodo2`
3. Un tercer nodo que se conecte a cualquiera de ellos se convertirá en `nodo3`, y así sucesivamente
4. Todos los nodos establecerán conexiones directas entre sí (topología de malla)

### Operaciones

- **Subir archivo**: Presiona "Subir Archivo" y selecciona un archivo. Se dividirá en bloques de 1 MB y se distribuirá con réplicas
- **Descargar archivo**: Presiona el botón de descarga junto al archivo en la tabla
- **Eliminar archivo**: Presiona el botón de eliminar junto al archivo
- **Cambiar capacidad**: Solo cuando estés desconectado. La capacidad debe estar entre 50-100 MB y no puede ser menor al espacio usado

### Visualización

- **Tabla de nodos**: Muestra todos los nodos del grupo con su estado, capacidad y espacio libre
- **Tabla de archivos**: Lista todos los archivos con sus atributos
- **Tabla de bloques**: Visualización de todos los bloques del sistema
  - Azul = bloque original
  - Rojo = bloque copia
  - Gris = bloque libre
  - Naranja = bloque no disponible (nodo offline)

## Arquitectura

El sistema está organizado en capas:

- **Network**: Comunicación TCP/UDP (TCP para archivos/metadatos, UDP para heartbeats)
- **Storage**: Gestión de bloques físicos y operaciones de archivos
- **Metadata**: Registros de nodos, bloques y archivos
- **Replication**: Estrategia de asignación de bloques y réplicas
- **GUI**: Interfaz gráfica con Flet

## Archivos de Configuración

- `config.json`: Capacidad del nodo (50-100 MB) y puerto TCP (8888)
- `node_state.json`: ID del nodo y grupo al que pertenece
- `metadata.json`: Metadatos de archivos y bloques locales

## Directorio de Almacenamiento

Los bloques se almacenan en:
- Windows: `C:\Users\<usuario>\espacioCompartido`
- Linux/Mac: `~/espacioCompartido`

## Protocolo de Comunicación

- **TCP puerto 8888**: Comunicación de archivos, metadatos y conexiones
- **UDP puerto 8889**: Heartbeats cada 3 segundos para detección de fallos

## Tolerancia a Fallas

- Cada bloque tiene al menos una réplica en un nodo diferente
- Si un nodo falla, los archivos siguen siendo accesibles desde las réplicas
- Los nodos se marcan como offline después de 9 segundos sin heartbeat
- Al reconectarse, un nodo recupera automáticamente su posición en el grupo

## Notas

- La capacidad solo puede cambiarse cuando el nodo está desconectado
- Si la capacidad es 60 MB y hay 55 MB usados, solo se puede cambiar a 55 MB (ajustar) o 100 MB
- Si la capacidad es 100 MB y está llena, no se puede modificar
- Los archivos se dividen en bloques de 1 MB (el último puede ser menor)

