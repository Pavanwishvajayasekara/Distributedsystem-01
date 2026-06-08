"""
Recovery manager for handling node failures and data recovery
"""
import time
import logging
from typing import List, Dict, Set, Optional
import threading

from core.storage_node import StorageNode
from core.file_metadata import FileMetadata

logger = logging.getLogger(__name__)

class RecoveryManager:
    """Manages data recovery and node synchronization"""
    
    def __init__(self, storage_nodes: List[StorageNode], replication_factor: int = 3):
        self.storage_nodes = storage_nodes
        self.replication_factor = replication_factor
        self.recovery_lock = threading.RLock()
        self.recovery_in_progress: Set[str] = set()
        
        logger.info("Recovery manager initialized")
    
    def handle_node_failure(self, failed_node: StorageNode):
        """Handle the failure of a node"""
        node_id = failed_node.node_id
        
        with self.recovery_lock:
            if node_id in self.recovery_in_progress:
                logger.info(f"Recovery already in progress for node {node_id}")
                return
            
            self.recovery_in_progress.add(node_id)
        
        try:
            logger.info(f"Starting recovery for failed node {node_id}")
            self._recover_failed_node(failed_node)
        except Exception as e:
            logger.error(f"Error during recovery of node {node_id}: {e}")
        finally:
            with self.recovery_lock:
                self.recovery_in_progress.discard(node_id)
    
    def _recover_failed_node(self, failed_node: StorageNode):
        """Recover data from a failed node"""
        # Get all files that were stored on the failed node
        failed_files = self._get_files_on_node(failed_node)
        
        if not failed_files:
            logger.info(f"No files to recover from node {failed_node.node_id}")
            return
        
        logger.info(f"Recovering {len(failed_files)} files from node {failed_node.node_id}")
        
        # Find active nodes for replication
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        
        if not active_nodes:
            logger.error("No active nodes available for recovery")
            return
        
        # Recover each file
        recovered_count = 0
        for file_id in failed_files:
            try:
                if self._recover_file(file_id, active_nodes):
                    recovered_count += 1
            except Exception as e:
                logger.error(f"Failed to recover file {file_id}: {e}")
        
        logger.info(f"Recovery completed: {recovered_count}/{len(failed_files)} files recovered")
    
    def _get_files_on_node(self, node: StorageNode) -> List[str]:
        """Get list of files stored on a specific node"""
        # This is a simplified version - in reality, we'd need to track
        # which files are stored on which nodes
        try:
            return node.list_files()
        except Exception as e:
            logger.error(f"Error getting files from node {node.node_id}: {e}")
            return []
    
    def _recover_file(self, file_id: str, active_nodes: List[StorageNode]) -> bool:
        """Recover a specific file to active nodes"""
        # Find a node that has the file
        source_node = None
        for node in active_nodes:
            if node.has_file(file_id):
                source_node = node
                break
        
        if not source_node:
            logger.warning(f"No source found for file {file_id}")
            return False
        
        # Get file content and metadata
        content, metadata = source_node.retrieve_file(file_id)
        if content is None or metadata is None:
            logger.warning(f"Could not retrieve file {file_id} from source")
            return False
        
        # Replicate to other active nodes
        replication_count = 0
        for node in active_nodes:
            if node == source_node:
                continue
            
            if not node.has_file(file_id):
                if node.store_file(file_id, content, metadata):
                    replication_count += 1
                    logger.debug(f"Replicated file {file_id} to node {node.node_id}")
        
        # Check if we have enough replicas
        required_replicas = min(self.replication_factor - 1, len(active_nodes))
        success = replication_count >= required_replicas
        
        if success:
            logger.info(f"File {file_id} recovered with {replication_count} replicas")
        else:
            logger.warning(f"File {file_id} recovery incomplete: {replication_count}/{required_replicas} replicas")
        
        return success
    
    def handle_node_recovery(self, recovered_node: StorageNode):
        """Handle a node coming back online"""
        node_id = recovered_node.node_id
        
        with self.recovery_lock:
            if node_id in self.recovery_in_progress:
                logger.info(f"Recovery already in progress for node {node_id}")
                return
            
            self.recovery_in_progress.add(node_id)
        
        try:
            logger.info(f"Starting synchronization for recovered node {node_id}")
            self._synchronize_recovered_node(recovered_node)
        except Exception as e:
            logger.error(f"Error during synchronization of node {node_id}: {e}")
        finally:
            with self.recovery_lock:
                self.recovery_in_progress.discard(node_id)
    
    def _synchronize_recovered_node(self, recovered_node: StorageNode):
        """Synchronize a recovered node with the current state"""
        # Get all files in the system
        all_files = set()
        for node in self.storage_nodes:
            if node.is_active() and node != recovered_node:
                all_files.update(node.list_files())
        
        if not all_files:
            logger.info("No files to synchronize")
            return
        
        logger.info(f"Synchronizing {len(all_files)} files to recovered node {recovered_node.node_id}")
        
        # Synchronize each file
        synchronized_count = 0
        for file_id in all_files:
            try:
                if self._synchronize_file(file_id, recovered_node):
                    synchronized_count += 1
            except Exception as e:
                logger.error(f"Failed to synchronize file {file_id}: {e}")
        
        logger.info(f"Synchronization completed: {synchronized_count}/{len(all_files)} files synchronized")
    
    def _synchronize_file(self, file_id: str, target_node: StorageNode) -> bool:
        """Synchronize a specific file to a target node"""
        # Find a source node that has the file
        source_node = None
        for node in self.storage_nodes:
            if node.is_active() and node != target_node and node.has_file(file_id):
                source_node = node
                break
        
        if not source_node:
            logger.warning(f"No source found for file {file_id}")
            return False
        
        # Get file content and metadata
        content, metadata = source_node.retrieve_file(file_id)
        if content is None or metadata is None:
            logger.warning(f"Could not retrieve file {file_id} from source")
            return False
        
        # Store on target node
        success = target_node.store_file(file_id, content, metadata)
        if success:
            logger.debug(f"Synchronized file {file_id} to node {target_node.node_id}")
        
        return success
    
    def get_recovery_status(self) -> Dict[str, any]:
        """Get current recovery status"""
        with self.recovery_lock:
            return {
                'recovery_in_progress': list(self.recovery_in_progress),
                'replication_factor': self.replication_factor,
                'total_nodes': len(self.storage_nodes),
                'active_nodes': len([n for n in self.storage_nodes if n.is_active()])
            }
