"""
Consensus manager that coordinates Raft operations for the distributed file system
"""
import logging
import time
from typing import List, Dict, Any, Optional
from threading import Lock

logger = logging.getLogger(__name__)

class ConsensusManager:
    """Manages consensus operations across Raft nodes"""
    
    def __init__(self, raft_nodes: List):
        self.raft_nodes = raft_nodes
        self.lock = Lock()
        
        logger.info(f"Consensus manager initialized with {len(raft_nodes)} Raft nodes")
    
    def get_leader_node(self):
        """Get the current leader node"""
        for node in self.raft_nodes:
            if node.is_leader():
                return node
        return None
    
    def propose_file_operation(self, operation_type: str, file_id: str, 
                              filename: str, metadata: Dict[str, Any] = None) -> bool:
        """Propose a file operation for consensus"""
        try:
            leader = self.get_leader_node()
            if not leader:
                logger.warning("No leader available for consensus operation")
                return False
            
            # Create operation for consensus
            operation = {
                'type': operation_type,
                'file_id': file_id,
                'filename': filename,
                'timestamp': time.time(),
                'metadata': metadata or {}
            }
            
            # Propose through leader
            success = leader.propose_operation(operation)
            
            if success:
                logger.info(f"Consensus achieved for {operation_type} operation on {filename}")
            else:
                logger.error(f"Consensus failed for {operation_type} operation on {filename}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in consensus operation: {e}")
            return False
    
    def can_perform_operation(self) -> bool:
        """Check if consensus operations can be performed (leader available)"""
        return self.get_leader_node() is not None
    
    def get_consensus_status(self) -> Dict[str, Any]:
        """Get current consensus status"""
        leader = self.get_leader_node()
        
        return {
            'has_leader': leader is not None,
            'leader_id': leader.node_id if leader else None,
            'total_nodes': len(self.raft_nodes),
            'active_nodes': len([n for n in self.raft_nodes if n.running])
        }