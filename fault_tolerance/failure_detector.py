"""
Real failure detection system for distributed file storage
"""
import socket
import json
import time
import threading
import logging
from typing import List, Set, Dict, Any, Optional
from dataclasses import dataclass

from core.storage_node import StorageNode

logger = logging.getLogger(__name__)

@dataclass
class NodeStatus:
    """Node status information"""
    node_id: str
    host: str
    port: int
    last_heartbeat: float
    is_active: bool
    consecutive_failures: int
    last_error: Optional[str] = None

class FailureDetector:
    """Real failure detection with actual network communication"""
    
    def __init__(self, storage_nodes: List[StorageNode], 
                 heartbeat_interval: int = 5, timeout: int = 15):
        self.storage_nodes = storage_nodes
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout
        
        # Node status tracking
        self.node_status: Dict[str, NodeStatus] = {}
        self.failed_nodes: Set[str] = set()
        
        # Threading
        self.running = False
        self.heartbeat_thread = None
        self.detection_thread = None
        self.lock = threading.RLock()
        
        # Initialize node status
        for node in storage_nodes:
            self.node_status[node.node_id] = NodeStatus(
                node_id=node.node_id,
                host=node.host,
                port=node.port,
                last_heartbeat=time.time(),
                is_active=True,
                consecutive_failures=0
            )
        
        logger.info(f"Failure detector initialized for {len(storage_nodes)} nodes")
    
    def start(self):
        """Start the failure detection system"""
        if self.running:
            return
        
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.detection_thread = threading.Thread(target=self._detection_loop, daemon=True)
        
        self.heartbeat_thread.start()
        self.detection_thread.start()
        
        logger.info("Failure detector started")
    
    def stop(self):
        """Stop the failure detection system"""
        self.running = False
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
        if self.detection_thread:
            self.detection_thread.join(timeout=2)
        
        logger.info("Failure detector stopped")
    
    def _heartbeat_loop(self):
        """Main heartbeat sending loop"""
        while self.running:
            try:
                self._send_heartbeats()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(1)
    
    def _detection_loop(self):
        """Main failure detection loop"""
        while self.running:
            try:
                self._detect_failures()
                time.sleep(1)  # Check every second
            except Exception as e:
                logger.error(f"Error in detection loop: {e}")
                time.sleep(1)
    
    def _send_heartbeats(self):
        """Send heartbeats to all nodes"""
        current_time = time.time()
        
        for node in self.storage_nodes:
            if not self.running:
                break
            
            try:
                success = self._send_heartbeat_to_node(node)
                
                with self.lock:
                    status = self.node_status[node.node_id]
                    if success:
                        status.last_heartbeat = current_time
                        status.consecutive_failures = 0
                        status.last_error = None
                        
                        # Mark as recovered if it was failed
                        if node.node_id in self.failed_nodes:
                            self.failed_nodes.remove(node.node_id)
                            status.is_active = True
                            logger.info(f"Node {node.node_id} recovered")
                    else:
                        status.consecutive_failures += 1
                        status.last_error = "Heartbeat failed"
                        
            except Exception as e:
                with self.lock:
                    status = self.node_status[node.node_id]
                    status.consecutive_failures += 1
                    status.last_error = str(e)
                    logger.debug(f"Heartbeat to {node.node_id} failed: {e}")
    
    def _send_heartbeat_to_node(self, node: StorageNode) -> bool:
        """Send heartbeat to a specific node"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((node.host, node.port))
                
                message = {
                    'type': 'heartbeat',
                    'timestamp': time.time(),
                    'detector_id': 'main_detector'
                }
                
                s.sendall(json.dumps(message).encode())
                response = s.recv(1024)
                
                if response:
                    response_data = json.loads(response.decode())
                    return response_data.get('status') == 'ok'
                
                return False
                
        except socket.timeout:
            return False
        except ConnectionRefusedError:
            return False
        except Exception as e:
            logger.debug(f"Error sending heartbeat to {node.node_id}: {e}")
            return False
    
    def _detect_failures(self):
        """Detect node failures based on heartbeat timeouts"""
        current_time = time.time()
        newly_failed = []
        
        with self.lock:
            for node_id, status in self.node_status.items():
                if not status.is_active:
                    continue
                
                time_since_heartbeat = current_time - status.last_heartbeat
                
                if time_since_heartbeat > self.timeout:
                    if node_id not in self.failed_nodes:
                        newly_failed.append(node_id)
                        self.failed_nodes.add(node_id)
                        status.is_active = False
                        logger.warning(f"Node {node_id} marked as failed "
                                     f"(no heartbeat for {time_since_heartbeat:.1f}s)")
        
        # Handle newly failed nodes
        for node_id in newly_failed:
            self._handle_node_failure(node_id)
    
    def _handle_node_failure(self, node_id: str):
        """Handle a node failure"""
        logger.warning(f"Handling failure of node {node_id}")
        
        # Find the failed node
        failed_node = None
        for node in self.storage_nodes:
            if node.node_id == node_id:
                failed_node = node
                break
        
        if not failed_node:
            logger.error(f"Failed node {node_id} not found in storage nodes")
            return
        
        # Trigger recovery if we have a recovery manager
        if hasattr(self, 'recovery_manager') and self.recovery_manager:
            threading.Thread(
                target=self.recovery_manager.handle_node_failure,
                args=(failed_node,),
                daemon=True
            ).start()
    
    def is_node_failed(self, node_id: str) -> bool:
        """Check if a node is currently failed"""
        with self.lock:
            return node_id in self.failed_nodes
    
    def get_active_nodes(self) -> List[StorageNode]:
        """Get list of currently active nodes"""
        with self.lock:
            return [node for node in self.storage_nodes 
                   if node.node_id not in self.failed_nodes]
    
    def get_failed_nodes(self) -> List[str]:
        """Get list of currently failed node IDs"""
        with self.lock:
            return list(self.failed_nodes)
    
    def get_node_status(self, node_id: str) -> Optional[NodeStatus]:
        """Get status of a specific node"""
        with self.lock:
            return self.node_status.get(node_id)
    
    def get_all_node_status(self) -> Dict[str, NodeStatus]:
        """Get status of all nodes"""
        with self.lock:
            return self.node_status.copy()
    
    def force_node_failure(self, node_id: str):
        """Force a node to be marked as failed (for testing)"""
        with self.lock:
            if node_id in self.node_status:
                self.failed_nodes.add(node_id)
                self.node_status[node_id].is_active = False
                logger.info(f"Node {node_id} force-marked as failed")
    
    def recover_node(self, node_id: str):
        """Mark a node as recovered"""
        with self.lock:
            if node_id in self.failed_nodes:
                self.failed_nodes.remove(node_id)
                if node_id in self.node_status:
                    self.node_status[node_id].is_active = True
                    self.node_status[node_id].consecutive_failures = 0
                    self.node_status[node_id].last_error = None
                logger.info(f"Node {node_id} marked as recovered")
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health metrics"""
        with self.lock:
            total_nodes = len(self.storage_nodes)
            active_nodes = total_nodes - len(self.failed_nodes)
            
            return {
                'total_nodes': total_nodes,
                'active_nodes': active_nodes,
                'failed_nodes': len(self.failed_nodes),
                'health_percentage': (active_nodes / total_nodes) * 100 if total_nodes > 0 else 0,
                'failed_node_ids': list(self.failed_nodes),
                'node_statuses': {
                    node_id: {
                        'is_active': status.is_active,
                        'last_heartbeat': status.last_heartbeat,
                        'consecutive_failures': status.consecutive_failures,
                        'last_error': status.last_error
                    }
                    for node_id, status in self.node_status.items()
                }
            }
