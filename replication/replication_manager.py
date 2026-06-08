"""
Replication manager for distributed file storage
"""
import hashlib
import time
import logging
import threading
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum

from core.storage_node import StorageNode
from core.file_metadata import FileMetadata, VectorClock

logger = logging.getLogger(__name__)

class ConsistencyLevel(Enum):
    """Consistency levels for replication"""
    ONE = "ONE"           # At least one replica
    QUORUM = "QUORUM"     # Majority of replicas
    ALL = "ALL"           # All replicas

class ReplicationManager:
    """Manages data replication across storage nodes"""
    
    def __init__(self, storage_nodes: List[StorageNode], 
                 replication_factor: int = 3,
                 consistency_level: ConsistencyLevel = ConsistencyLevel.QUORUM):
        self.storage_nodes = storage_nodes
        self.replication_factor = min(replication_factor, len(storage_nodes))
        self.consistency_level = consistency_level
        
        # Vector clocks for conflict resolution
        self.vector_clocks: Dict[str, VectorClock] = {}
        self.clock_lock = threading.RLock()
        
        logger.info(f"Replication manager initialized: "
                   f"factor={self.replication_factor}, level={self.consistency_level.value}")
    
    def write_file(self, file_id: str, content: bytes, metadata: FileMetadata) -> bool:
        """Write a file with replication"""
        try:
            # Select target nodes for replication
            target_nodes = self._select_replica_nodes(file_id)
            if not target_nodes:
                logger.error("No target nodes available for replication")
                return False
            
            # Update vector clock
            client_id = metadata.created_by
            with self.clock_lock:
                if file_id not in self.vector_clocks:
                    self.vector_clocks[file_id] = VectorClock(client_id)
                self.vector_clocks[file_id] = self.vector_clocks[file_id].increment(client_id)
                metadata.vector_clock = self.vector_clocks[file_id].to_dict()
            
            # Write to selected nodes
            successful_writes = 0
            for node in target_nodes:
                try:
                    if node.store_file(file_id, content, metadata):
                        successful_writes += 1
                        logger.debug(f"Replicated file {file_id} to node {node.node_id}")
                except Exception as e:
                    logger.error(f"Failed to write to node {node.node_id}: {e}")
            
            # Check if we have enough successful writes
            required_writes = self._get_required_writes()
            success = successful_writes >= required_writes
            
            if success:
                # Update metadata with replica information
                metadata.replicas = [node.node_id for node in target_nodes[:successful_writes]]
                logger.info(f"File {file_id} replicated successfully: "
                           f"{successful_writes}/{len(target_nodes)} nodes")
            else:
                logger.error(f"File {file_id} replication failed: "
                           f"{successful_writes}/{required_writes} required writes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in write_file: {e}")
            return False
    
    def read_file(self, file_id: str) -> Tuple[Optional[bytes], Optional[FileMetadata]]:
        """Read a file with consistency guarantees"""
        try:
            # Find nodes that have the file
            replica_nodes = self._get_replica_nodes(file_id)
            if not replica_nodes:
                logger.warning(f"No replicas found for file {file_id}")
                return None, None
            
            # Read from replica nodes
            results = []
            for node in replica_nodes:
                try:
                    content, metadata = node.retrieve_file(file_id)
                    if content is not None and metadata is not None:
                        results.append((content, metadata, node.node_id))
                except Exception as e:
                    logger.debug(f"Error reading from node {node.node_id}: {e}")
            
            if not results:
                logger.warning(f"Could not read file {file_id} from any replica")
                return None, None
            
            # Check if we have enough reads for consistency
            required_reads = self._get_required_reads()
            if len(results) < required_reads:
                logger.warning(f"Insufficient reads for consistency: "
                             f"{len(results)}/{required_reads}")
                # Still return the best result we have
            
            # Resolve conflicts if multiple versions exist
            if len(results) > 1:
                content, metadata = self._resolve_conflicts(results)
            else:
                content, metadata, _ = results[0]
            
            logger.debug(f"File {file_id} read successfully")
            return content, metadata
            
        except Exception as e:
            logger.error(f"Error in read_file: {e}")
            return None, None
    
    def delete_file(self, file_id: str, client_id: str) -> bool:
        """Delete a file from all replicas"""
        try:
            # Find all nodes that have the file
            replica_nodes = self._get_replica_nodes(file_id)
            if not replica_nodes:
                logger.warning(f"No replicas found for file {file_id}")
                return True  # Consider it deleted if no replicas exist
            
            # Delete from all replica nodes
            successful_deletes = 0
            for node in replica_nodes:
                try:
                    if node.delete_file(file_id):
                        successful_deletes += 1
                        logger.debug(f"Deleted file {file_id} from node {node.node_id}")
                except Exception as e:
                    logger.error(f"Failed to delete from node {node.node_id}: {e}")
            
            # Check if we have enough successful deletes
            required_deletes = self._get_required_deletes()
            success = successful_deletes >= required_deletes
            
            if success:
                # Remove from vector clocks
                with self.clock_lock:
                    self.vector_clocks.pop(file_id, None)
                
                logger.info(f"File {file_id} deleted successfully: "
                           f"{successful_deletes}/{len(replica_nodes)} nodes")
            else:
                logger.error(f"File {file_id} deletion failed: "
                           f"{successful_deletes}/{required_deletes} required deletes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in delete_file: {e}")
            return False
    
    def _select_replica_nodes(self, file_id: str) -> List[StorageNode]:
        """Select nodes for file replication using consistent hashing"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        if not active_nodes:
            return []
        
        # Use consistent hashing to select nodes
        hash_value = int(hashlib.md5(file_id.encode()).hexdigest(), 16)
        start_index = hash_value % len(active_nodes)
        
        # Select nodes in order starting from the hash position
        selected_nodes = []
        for i in range(self.replication_factor):
            node_index = (start_index + i) % len(active_nodes)
            selected_nodes.append(active_nodes[node_index])
        
        return selected_nodes
    
    def _get_replica_nodes(self, file_id: str) -> List[StorageNode]:
        """Get all nodes that have a specific file"""
        replica_nodes = []
        for node in self.storage_nodes:
            if node.is_active() and node.has_file(file_id):
                replica_nodes.append(node)
        return replica_nodes
    
    def _resolve_conflicts(self, results: List[Tuple[bytes, FileMetadata, str]]) -> Tuple[bytes, FileMetadata]:
        """Resolve conflicts between multiple versions of a file"""
        if not results:
            return None, None
        
        if len(results) == 1:
            return results[0][0], results[0][1]
        
        # Use vector clock comparison for conflict resolution
        best_result = results[0]
        best_metadata = best_result[1]
        
        for content, metadata, node_id in results[1:]:
            if self._is_newer_version(metadata, best_metadata):
                best_result = (content, metadata, node_id)
                best_metadata = metadata
        
        logger.debug(f"Resolved conflict for file {best_metadata.file_id} "
                    f"using version from node {best_result[2]}")
        
        return best_result[0], best_result[1]
    
    def _is_newer_version(self, metadata1: FileMetadata, metadata2: FileMetadata) -> bool:
        """Compare two file versions to determine which is newer"""
        # First compare timestamps
        if metadata1.timestamp != metadata2.timestamp:
            return metadata1.timestamp > metadata2.timestamp
        
        # If timestamps are equal, compare vector clocks
        vc1 = VectorClock.from_dict(metadata1.vector_clock)
        vc2 = VectorClock.from_dict(metadata2.vector_clock)
        
        comparison = vc1.compare(vc2)
        return comparison in ['after', 'concurrent']
    
    def _get_required_writes(self) -> int:
        """Get required number of successful writes based on consistency level"""
        if self.consistency_level == ConsistencyLevel.ONE:
            return 1
        elif self.consistency_level == ConsistencyLevel.QUORUM:
            return (self.replication_factor // 2) + 1
        elif self.consistency_level == ConsistencyLevel.ALL:
            return self.replication_factor
        else:
            return 1
    
    def _get_required_reads(self) -> int:
        """Get required number of successful reads based on consistency level"""
        return self._get_required_writes()
    
    def _get_required_deletes(self) -> int:
        """Get required number of successful deletes based on consistency level"""
        return self._get_required_writes()
    
    def get_replication_status(self) -> Dict[str, Any]:
        """Get replication status information"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        
        return {
            'replication_factor': self.replication_factor,
            'consistency_level': self.consistency_level.value,
            'total_nodes': len(self.storage_nodes),
            'active_nodes': len(active_nodes),
            'tracked_files': len(self.vector_clocks)
        }
    
    def update_consistency_level(self, new_level: ConsistencyLevel):
        """Update the consistency level"""
        self.consistency_level = new_level
        logger.info(f"Consistency level updated to {new_level.value}")
    
    def get_file_replicas(self, file_id: str) -> List[str]:
        """Get list of node IDs that have a specific file"""
        replica_nodes = self._get_replica_nodes(file_id)
        return [node.node_id for node in replica_nodes]
