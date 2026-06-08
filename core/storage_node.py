"""
Storage node implementation for distributed file storage
"""
import os
import json
import socket
import threading
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

from core.file_metadata import FileMetadata, VectorClock, calculate_checksum

logger = logging.getLogger(__name__)

class StorageNode:
    """Individual storage node in the distributed system"""
    
    def __init__(self, node_id: str, storage_dir: str, port: int, host: str = 'localhost'):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage_dir = Path(storage_dir) / node_id
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Node state
        self.active = True
        self.metadata_file = self.storage_dir / 'metadata.json'
        self.files: Dict[str, FileMetadata] = {}
        
        # RPC server
        self.rpc_server = None
        self.rpc_thread = None
        self.server_socket = None
        
        # Raft state
        self.raft_state = {
            'role': 'follower',
            'leader_id': None,
            'term': 0,
            'voted_for': None,
            'log': [],
            'commit_index': -1,
            'last_applied': -1
        }
        
        # Threading
        self.lock = threading.RLock()
        
        # Load existing metadata
        self._load_metadata()
        
        # Start RPC server
        self.start_rpc_server()
    
    def _load_metadata(self):
        """Load file metadata from disk"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    for file_id, meta_data in data.items():
                        self.files[file_id] = FileMetadata.from_dict(meta_data)
                logger.info(f"Loaded {len(self.files)} files from metadata")
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}")
    
    def _save_metadata(self):
        """Save file metadata to disk"""
        try:
            data = {file_id: meta.to_dict() for file_id, meta in self.files.items()}
            with open(self.metadata_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def start_rpc_server(self):
        """Start RPC server for inter-node communication"""
        self.rpc_thread = threading.Thread(target=self._rpc_server_loop, daemon=True)
        self.rpc_thread.start()
        time.sleep(0.1)  # Give server time to start
    
    def _rpc_server_loop(self):
        """RPC server main loop"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            logger.info(f"RPC server listening on {self.host}:{self.port}")
            
            while self.active:
                try:
                    self.server_socket.settimeout(1.0)
                    conn, addr = self.server_socket.accept()
                    threading.Thread(target=self._handle_rpc_request, args=(conn, addr), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.active:
                        logger.error(f"RPC server error: {e}")
        except Exception as e:
            logger.error(f"Failed to start RPC server: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def _handle_rpc_request(self, conn: socket.socket, addr: Tuple[str, int]):
        """Handle incoming RPC request"""
        try:
            data = conn.recv(4096)
            if not data:
                return
            
            message = json.loads(data.decode())
            response = self._process_message(message)
            
            conn.sendall(json.dumps(response).encode())
        except Exception as e:
            logger.error(f"Error handling RPC request from {addr}: {e}")
            try:
                conn.sendall(json.dumps({'error': str(e)}).encode())
            except:
                pass
        finally:
            conn.close()
    
    def _process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming message and return response"""
        msg_type = message.get('type')
        
        if msg_type == 'heartbeat':
            return {'status': 'ok', 'timestamp': time.time()}
        
        elif msg_type == 'store_file':
            return self._handle_store_file(message)
        
        elif msg_type == 'retrieve_file':
            return self._handle_retrieve_file(message)
        
        elif msg_type == 'delete_file':
            return self._handle_delete_file(message)
        
        elif msg_type == 'list_files':
            return self._handle_list_files(message)
        
        elif msg_type == 'raft_request_vote':
            return self._handle_raft_request_vote(message)
        
        elif msg_type == 'raft_append_entries':
            return self._handle_raft_append_entries(message)
        
        else:
            return {'error': f'Unknown message type: {msg_type}'}
    
    def _handle_store_file(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file storage request"""
        try:
            file_id = message['file_id']
            content = bytes.fromhex(message['content'])
            metadata = FileMetadata.from_dict(message['metadata'])
            
            success = self.store_file(file_id, content, metadata)
            return {'success': success}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_retrieve_file(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file retrieval request"""
        try:
            file_id = message['file_id']
            content, metadata = self.retrieve_file(file_id)
            
            if content is not None:
                return {
                    'success': True,
                    'content': content.hex(),
                    'metadata': metadata.to_dict()
                }
            else:
                return {'success': False, 'error': 'File not found'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_delete_file(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file deletion request"""
        try:
            file_id = message['file_id']
            success = self.delete_file(file_id)
            return {'success': success}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_list_files(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle list files request"""
        try:
            files = self.list_files()
            return {'success': True, 'files': files}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_raft_request_vote(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Raft vote request"""
        # This will be implemented in the consensus module
        return {'vote_granted': False, 'term': self.raft_state['term']}
    
    def _handle_raft_append_entries(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Raft append entries"""
        # This will be implemented in the consensus module
        return {'success': False, 'term': self.raft_state['term']}
    
    def store_file(self, file_id: str, content: bytes, metadata: FileMetadata) -> bool:
        """Store a file with metadata"""
        if not self.active:
            return False
        
        try:
            with self.lock:
                # Write file content
                file_path = self.storage_dir / file_id
                with open(file_path, 'wb') as f:
                    f.write(content)
                
                # Store metadata
                self.files[file_id] = metadata
                self._save_metadata()
                
                logger.info(f"Stored file {file_id} ({len(content)} bytes)")
                return True
        except Exception as e:
            logger.error(f"Failed to store file {file_id}: {e}")
            return False
    
    def retrieve_file(self, file_id: str) -> Tuple[Optional[bytes], Optional[FileMetadata]]:
        """Retrieve a file and its metadata"""
        if not self.active:
            return None, None
        
        try:
            with self.lock:
                if file_id not in self.files:
                    return None, None
                
                file_path = self.storage_dir / file_id
                if not file_path.exists():
                    return None, None
                
                with open(file_path, 'rb') as f:
                    content = f.read()
                
                metadata = self.files[file_id]
                return content, metadata
        except Exception as e:
            logger.error(f"Failed to retrieve file {file_id}: {e}")
            return None, None
    
    def delete_file(self, file_id: str) -> bool:
        """Delete a file and its metadata"""
        if not self.active:
            return False
        
        try:
            with self.lock:
                if file_id not in self.files:
                    return False
                
                file_path = self.storage_dir / file_id
                if file_path.exists():
                    file_path.unlink()
                
                del self.files[file_id]
                self._save_metadata()
                
                logger.info(f"Deleted file {file_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to delete file {file_id}: {e}")
            return False
    
    def list_files(self) -> List[str]:
        """List all file IDs stored on this node"""
        with self.lock:
            return list(self.files.keys())
    
    def has_file(self, file_id: str) -> bool:
        """Check if node has a specific file"""
        with self.lock:
            return file_id in self.files
    
    def get_file_metadata(self, file_id: str) -> Optional[FileMetadata]:
        """Get metadata for a specific file"""
        with self.lock:
            return self.files.get(file_id)
    
    def is_active(self) -> bool:
        """Check if node is active"""
        return self.active
    
    def start(self):
        """Start the storage node (mark it as active)"""
        with self.lock:
            self.active = True
            # Restart RPC server if needed
            if not self.rpc_thread or not self.rpc_thread.is_alive():
                self.start_rpc_server()
        logger.info(f"Storage node {self.node_id} started")
    
    def stop(self):
        """Stop the storage node"""
        with self.lock:
            self.active = False
            if self.server_socket:
                self.server_socket.close()
            # Stop Raft node when storage node stops
            if hasattr(self, 'raft_node') and self.raft_node:
                self.raft_node.stop()
            logger.info(f"Storage node {self.node_id} stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status information"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'host': self.host,
                'port': self.port,
                'active': self.active,
                'file_count': len(self.files),
                'storage_dir': str(self.storage_dir),
                'raft_state': self.raft_state.copy()
            }
