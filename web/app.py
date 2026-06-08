"""
Flask web application for distributed file storage system
"""
import os
import sys
import time
import logging
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_socketio import SocketIO, emit
import threading

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.coordinator import CoordinatorService
from core.storage_node import StorageNode
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.recovery_manager import RecoveryManager
from replication.replication_manager import ReplicationManager, ConsistencyLevel
from time_sync.ntp_client import NTPClient
from consensus.raft_node import RaftNode
from consensus.consensus_manager import ConsensusManager
import config

# Configure logging
logging.basicConfig(level=getattr(logging, config.Config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'distributed-file-system-2024'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables
coordinator = None
storage_nodes = []
raft_nodes = []
failure_detector = None
recovery_manager = None
replication_manager = None
ntp_client = None

def initialize_system():
    """Initialize the distributed file storage system"""
    global coordinator, storage_nodes, raft_nodes, failure_detector, recovery_manager, replication_manager, ntp_client
    
    logger.info("Initializing distributed file storage system...")
    
    # Create storage nodes
    storage_nodes = []
    for i in range(config.Config.NODE_COUNT):
        node_id = f"Node-{i}"
        port = config.Config.BASE_PORT + i
        storage_dir = os.path.join(os.getcwd(), config.Config.STORAGE_DIR)
        
        node = StorageNode(node_id, storage_dir, port)
        storage_nodes.append(node)
        logger.info(f"Created storage node: {node_id} on port {port}")
    
    # Create Raft nodes
    raft_nodes = []
    for i, node in enumerate(storage_nodes):
        # Use Raft RPC ports for peer communication
        peers = [(n.host, config.Config.BASE_PORT + 1000 + j) for j, n in enumerate(storage_nodes) if j != i]
        raft_rpc_port = config.Config.BASE_PORT + 1000 + i
        raft_node = RaftNode(
            node.node_id, 
            peers,
            raft_rpc_port
        )
        raft_nodes.append(raft_node)
        logger.info(f"Created Raft node: {node.node_id} with peers: {peers}")
    
    # Start Raft nodes with staggered delays to prevent simultaneous elections
    for i, raft_node in enumerate(raft_nodes):
        raft_rpc_port = config.Config.BASE_PORT + 1000 + i
        raft_node.start(raft_rpc_port)  # Raft RPC ports
        logger.info(f"Started Raft node {raft_node.node_id} on RPC port {raft_rpc_port}")
        time.sleep(2.0)  # Increased staggered delay between node starts
    
    # Create coordinator
    coordinator = CoordinatorService(
        storage_nodes=storage_nodes,
        replication_factor=config.Config.REPLICATION_FACTOR,
        consistency_level=config.Config.CONSISTENCY_LEVEL
    )
    
    # Create replication manager
    replication_manager = ReplicationManager(
        storage_nodes=storage_nodes,
        replication_factor=config.Config.REPLICATION_FACTOR,
        consistency_level=ConsistencyLevel(config.Config.CONSISTENCY_LEVEL)
    )
    
    # Create failure detector
    failure_detector = FailureDetector(
        storage_nodes=storage_nodes,
        heartbeat_interval=config.Config.HEARTBEAT_INTERVAL,
        timeout=config.Config.HEARTBEAT_TIMEOUT
    )
    
    # Create recovery manager
    recovery_manager = RecoveryManager(
        storage_nodes=storage_nodes,
        replication_factor=config.Config.REPLICATION_FACTOR
    )
    
    # Create NTP client
    ntp_client = NTPClient(
        servers=config.Config.NTP_SERVERS,
        sync_interval=config.Config.SYNC_INTERVAL
    )
    
    # Create consensus manager
    consensus_manager = ConsensusManager(raft_nodes)
    
    # Set component dependencies
    coordinator.set_components(failure_detector, replication_manager, ntp_client, consensus_manager)
    failure_detector.recovery_manager = recovery_manager
    
    # Start components
    failure_detector.start()
    ntp_client.start()
    
    logger.info("System initialization completed")

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    """Get system status with proper offline node handling"""
    try:
        system_status = coordinator.get_system_status()
        
        # Add Raft status with offline detection
        raft_status = []
        for i, raft_node in enumerate(raft_nodes):
            try:
                # Check if node is still running
                if not raft_node.running:
                    # Node is stopped - mark as offline
                    status = {
                        'node_id': raft_node.node_id,
                        'state': 'OFFLINE',
                        'term': raft_node.current_term,
                        'leader': None,
                        'log_length': len(raft_node.log),
                        'commit_index': raft_node.commit_index,
                        'voted_for': raft_node.voted_for,
                        'last_heartbeat': 999999,  # Very high number to indicate offline
                        'storage_node_id': storage_nodes[i].node_id
                    }
                else:
                    # Node is running - get actual status
                    status = raft_node.get_status()
                    status['storage_node_id'] = storage_nodes[i].node_id
                    
                raft_status.append(status)
                
            except Exception as e:
                # Node is unreachable - mark as offline
                logger.warning(f"Raft node {i} unreachable: {e}")
                offline_status = {
                    'node_id': f'Node-{i}',
                    'state': 'OFFLINE',
                    'term': 0,
                    'leader': None,
                    'log_length': 0,
                    'commit_index': -1,
                    'voted_for': None,
                    'last_heartbeat': 999999,
                    'storage_node_id': storage_nodes[i].node_id if i < len(storage_nodes) else f'Node-{i}'
                }
                raft_status.append(offline_status)
        
        # Add failure detector status
        failure_status = failure_detector.get_system_health()
        
        # Add NTP status
        ntp_status = ntp_client.get_sync_status()
        
        return jsonify({
            'system': system_status,
            'raft': raft_status,
            'failure_detector': failure_status,
            'ntp': ntp_status,
            'timestamp': time.time()
        })
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/files')
def api_files():
    """Get list of files"""
    try:
        files = coordinator.list_files()
        return jsonify({'files': files})
    except Exception as e:
        logger.error(f"Error getting files: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def api_upload():
    """Upload a file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Read file content
        content = file.read()
        if len(content) > config.Config.MAX_FILE_SIZE:
            return jsonify({'error': 'File too large'}), 400
        
        # Upload file
        file_id = coordinator.upload_file(file.filename, content)
        if file_id:
            return jsonify({'success': True, 'file_id': file_id})
        else:
            return jsonify({'error': 'Upload failed'}), 500
            
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def api_download(filename):
    """Download a file"""
    try:
        content = coordinator.download_file(filename)
        if content is not None:
            from flask import Response
            return Response(
                content,
                mimetype='application/octet-stream',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delete/<filename>', methods=['DELETE'])
def api_delete(filename):
    """Delete a file"""
    try:
        success = coordinator.delete_file(filename)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Delete failed'}), 500
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/nodes/<node_id>/stop', methods=['POST'])
def api_stop_node(node_id):
    """Stop a specific node"""
    try:
        for i, node in enumerate(storage_nodes):
            if node.node_id == node_id:
                # Stop both storage node and Raft node
                node.stop()
                if i < len(raft_nodes):
                    raft_nodes[i].stop()
                return jsonify({'success': True})
        return jsonify({'error': 'Node not found'}), 404
    except Exception as e:
        logger.error(f"Error stopping node: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/nodes/<node_id>/start', methods=['POST'])
def api_start_node(node_id):
    """Start a specific node"""
    try:
        for i, node in enumerate(storage_nodes):
            if node.node_id == node_id:
                # Start both storage node and Raft node
                node.start()
                if i < len(raft_nodes):
                    raft_rpc_port = config.Config.BASE_PORT + 1000 + i
                    raft_nodes[i].start(raft_rpc_port)
                return jsonify({'success': True})
        return jsonify({'error': 'Node not found'}), 404
    except Exception as e:
        logger.error(f"Error starting node: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/consistency/<level>', methods=['POST'])
def api_set_consistency(level):
    """Set consistency level"""
    try:
        if level.upper() in ['ONE', 'QUORUM', 'ALL']:
            replication_manager.update_consistency_level(ConsistencyLevel(level.upper()))
            coordinator.consistency_level = level.upper()
            return jsonify({'success': True, 'consistency_level': level.upper()})
        else:
            return jsonify({'error': 'Invalid consistency level'}), 400
    except Exception as e:
        logger.error(f"Error setting consistency level: {e}")
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info(f"Client connected: {request.sid}")
    emit('status', {'message': 'Connected to distributed file system'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info(f"Client disconnected: {request.sid}")

def broadcast_status():
    """Broadcast system status to connected clients"""
    while True:
        try:
            if coordinator:
                status = coordinator.get_system_status()
                socketio.emit('system_status', status)
            time.sleep(5)  # Broadcast every 5 seconds
        except Exception as e:
            logger.error(f"Error broadcasting status: {e}")
            time.sleep(5)

if __name__ == '__main__':
    # Initialize system
    initialize_system()
    
    # Start status broadcasting thread
    status_thread = threading.Thread(target=broadcast_status, daemon=True)
    status_thread.start()
    
    # Start Flask app
    logger.info(f"Starting web server on {config.Config.WEB_HOST}:{config.Config.WEB_PORT}")
    socketio.run(
        app,
        host=config.Config.WEB_HOST,
        port=config.Config.WEB_PORT,
        debug=config.Config.DEBUG
    )
