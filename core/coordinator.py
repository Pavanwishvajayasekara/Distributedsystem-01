"""
Main coordinator service for the distributed file storage system
"""
import time
import logging
from typing import List, Dict, Optional, Any
from threading import Lock

from core.file_metadata import FileMetadata, VectorClock, generate_file_id, calculate_checksum
from core.storage_node import StorageNode

logger = logging.getLogger(__name__)

class CoordinatorService:
    """Main coordinator that manages the distributed file system"""
    
    def __init__(self, storage_nodes: List[StorageNode], replication_factor: int = 3, 
                 consistency_level: str = "QUORUM"):
        self.storage_nodes = storage_nodes
        self.replication_factor = min(replication_factor, len(storage_nodes))
        self.consistency_level = consistency_level
        
        # File registry
        self.filename_registry: Dict[str, str] = {}  # filename -> file_id
        self.file_metadata: Dict[str, FileMetadata] = {}  # file_id -> metadata
        
        # Threading
        self.lock = Lock()
        
        # Initialize components (will be injected later)
        self.failure_detector = None
        self.replication_manager = None
        self.time_synchronizer = None
        self.consensus_manager = None
        
        logger.info(f"Coordinator initialized with {len(storage_nodes)} nodes, "
                   f"replication factor: {self.replication_factor}, "
                   f"consistency: {self.consistency_level}")
    
    def set_components(self, failure_detector, replication_manager, 
                      time_synchronizer, consensus_manager):
        """Set the component dependencies"""
        self.failure_detector = failure_detector
        self.replication_manager = replication_manager
        self.time_synchronizer = time_synchronizer
        self.consensus_manager = consensus_manager
    
    def upload_file(self, filename: str, content: bytes, client_id: str = "web_client") -> Optional[str]:
        """Upload a file to the distributed system with consensus"""
        try:
            # Generate file ID and metadata
            file_id = generate_file_id(filename, content)
            checksum = calculate_checksum(content)
            timestamp = time.time()
            
            # Create vector clock
            vector_clock = VectorClock(client_id)
            vector_clock = vector_clock.increment(client_id)
            
            # Create file metadata
            metadata = FileMetadata(
                file_id=file_id,
                filename=filename,
                size=len(content),
                timestamp=timestamp,
                vector_clock=vector_clock.to_dict(),
                checksum=checksum,
                replicas=[],
                created_by=client_id,
                last_modified=timestamp
            )
            
            # Step 1: Propose operation through consensus (if available)
            consensus_success = True
            if self.consensus_manager and self.consensus_manager.can_perform_operation():
                logger.info(f"Proposing file upload through consensus: {filename}")
                consensus_success = self.consensus_manager.propose_file_operation(
                    "UPLOAD", file_id, filename, metadata.to_dict()
                )
                
                if not consensus_success:
                    logger.error(f"Consensus failed for file upload: {filename}")
                    return None
            else:
                logger.warning(f"No consensus available, proceeding with direct upload: {filename}")
            
            # Step 2: Execute storage operation (if consensus succeeded)
            if consensus_success:
                if self.replication_manager:
                    success = self.replication_manager.write_file(file_id, content, metadata)
                else:
                    success = self._direct_write(file_id, content, metadata)
                
                if success:
                    with self.lock:
                        self.filename_registry[filename] = file_id
                        self.file_metadata[file_id] = metadata
                
                    logger.info(f"File '{filename}' uploaded successfully as {file_id}")
                    return file_id
                else:
                    logger.error(f"Failed to upload file '{filename}'")
                    return None
            else:
                logger.error(f"Consensus rejected file upload for: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"Error uploading file '{filename}': {e}")
            return None
    
    def download_file(self, filename: str) -> Optional[bytes]:
        """Download a file from the distributed system"""
        try:
            with self.lock:
                file_id = self.filename_registry.get(filename)
                if not file_id:
                    return None
            
            # Use replication manager to read file
            if self.replication_manager:
                content, metadata = self.replication_manager.read_file(file_id)
            else:
                content, metadata = self._direct_read(file_id)
            
            if content is not None:
                logger.info(f"File '{filename}' downloaded successfully")
                return content
            else:
                logger.error(f"File '{filename}' not found")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading file '{filename}': {e}")
            return None
    
    def delete_file(self, filename: str, client_id: str = "web_client") -> bool:
        """Delete a file from the distributed system with consensus"""
        try:
            with self.lock:
                file_id = self.filename_registry.get(filename)
                if not file_id:
                    return False
                metadata = self.file_metadata.get(file_id)
            
            # Step 1: Propose deletion through consensus (if available)
            consensus_success = True
            if self.consensus_manager and self.consensus_manager.can_perform_operation():
                logger.info(f"Proposing file deletion through consensus: {filename}")
                consensus_success = self.consensus_manager.propose_file_operation(
                    "DELETE", file_id, filename, metadata.to_dict() if metadata else {}
                )
                
                if not consensus_success:
                    logger.error(f"Consensus failed for file deletion: {filename}")
                    return False
            else:
                logger.warning(f"No consensus available, proceeding with direct deletion: {filename}")
            
            # Step 2: Execute deletion operation (if consensus succeeded)
            if consensus_success:
                if self.replication_manager:
                    success = self.replication_manager.delete_file(file_id, client_id)
                else:
                    success = self._direct_delete(file_id)
                
                if success:
                    with self.lock:
                        self.filename_registry.pop(filename, None)
                        self.file_metadata.pop(file_id, None)
                    
                    logger.info(f"File '{filename}' deleted successfully")
                    return True
                else:
                    logger.error(f"Failed to delete file '{filename}'")
                    return False
            else:
                logger.error(f"Consensus rejected file deletion for: {filename}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting file '{filename}': {e}")
            return False
    
    def list_files(self) -> List[Dict[str, Any]]:
        """List all files in the system"""
        with self.lock:
            files = []
            for filename, file_id in self.filename_registry.items():
                metadata = self.file_metadata.get(file_id)
                if metadata:
                    files.append({
                        'filename': filename,
                        'file_id': file_id,
                        'size': metadata.size,
                        'created_by': metadata.created_by,
                        'last_modified': metadata.last_modified,
                        'replicas': len(metadata.replicas)
                    })
            return files
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        
        return {
            'total_nodes': len(self.storage_nodes),
            'active_nodes': len(active_nodes),
            'failed_nodes': len(self.storage_nodes) - len(active_nodes),
            'total_files': len(self.filename_registry),
            'replication_factor': self.replication_factor,
            'consistency_level': self.consistency_level,
            'nodes': [node.get_status() for node in self.storage_nodes]
        }
    
    def _direct_write(self, file_id: str, content: bytes, metadata: FileMetadata) -> bool:
        """Direct write without replication manager (fallback)"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        if not active_nodes:
            return False
        
        successful_writes = 0
        for node in active_nodes[:self.replication_factor]:
            if node.store_file(file_id, content, metadata):
                successful_writes += 1
        
        return successful_writes >= self._get_required_acks()
    
    def _direct_read(self, file_id: str) -> tuple:
        """Direct read without replication manager (fallback)"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        
        for node in active_nodes:
            content, metadata = node.retrieve_file(file_id)
            if content is not None:
                return content, metadata
        
        return None, None
    
    def _direct_delete(self, file_id: str) -> bool:
        """Direct delete without consensus manager (fallback)"""
        active_nodes = [node for node in self.storage_nodes if node.is_active()]
        if not active_nodes:
            return False
        
        successful_deletes = 0
        for node in active_nodes:
            if node.delete_file(file_id):
                successful_deletes += 1
        
        return successful_deletes >= self._get_required_acks()
    
    def _get_required_acks(self) -> int:
        """Get required acknowledgments based on consistency level"""
        if self.consistency_level == "ONE":
            return 1
        elif self.consistency_level == "QUORUM":
            return (self.replication_factor // 2) + 1
        elif self.consistency_level == "ALL":
            return self.replication_factor
        else:
            return 1
