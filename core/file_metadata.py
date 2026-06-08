"""
File metadata models and utilities
"""
import time
import hashlib
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import json

@dataclass
class FileMetadata:
    """File metadata structure"""
    file_id: str
    filename: str
    size: int
    timestamp: float
    vector_clock: Dict[str, int]
    checksum: str
    replicas: list
    created_by: str
    last_modified: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileMetadata':
        """Create from dictionary"""
        return cls(**data)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'FileMetadata':
        """Create from JSON string"""
        return cls.from_dict(json.loads(json_str))

class VectorClock:
    """Vector clock implementation for conflict resolution"""
    
    def __init__(self, node_id: str = None):
        self.clocks = {}
        if node_id:
            self.clocks[node_id] = 0
    
    def increment(self, node_id: str) -> 'VectorClock':
        """Increment clock for a node"""
        new_clock = VectorClock()
        new_clock.clocks = self.clocks.copy()
        new_clock.clocks[node_id] = new_clock.clocks.get(node_id, 0) + 1
        return new_clock
    
    def update(self, other: 'VectorClock') -> 'VectorClock':
        """Update with another vector clock (element-wise max)"""
        new_clock = VectorClock()
        all_nodes = set(self.clocks.keys()) | set(other.clocks.keys())
        new_clock.clocks = {
            node: max(self.clocks.get(node, 0), other.clocks.get(node, 0))
            for node in all_nodes
        }
        return new_clock
    
    def compare(self, other: 'VectorClock') -> str:
        """Compare two vector clocks
        Returns: 'before', 'after', 'concurrent', or 'equal'
        """
        if self.clocks == other.clocks:
            return 'equal'
        
        all_nodes = set(self.clocks.keys()) | set(other.clocks.keys())
        
        # Check if this clock is before the other
        before = all(self.clocks.get(node, 0) <= other.clocks.get(node, 0) 
                    for node in all_nodes)
        
        # Check if this clock is after the other
        after = all(self.clocks.get(node, 0) >= other.clocks.get(node, 0) 
                   for node in all_nodes)
        
        if before and not after:
            return 'before'
        elif after and not before:
            return 'after'
        else:
            return 'concurrent'
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary"""
        return self.clocks.copy()
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> 'VectorClock':
        """Create from dictionary"""
        vc = cls()
        vc.clocks = data.copy()
        return vc

def generate_file_id(filename: str, content: bytes) -> str:
    """Generate unique file ID based on filename and content"""
    content_hash = hashlib.sha256(content).hexdigest()
    timestamp = str(int(time.time() * 1000))
    return hashlib.sha256(f"{filename}_{content_hash}_{timestamp}".encode()).hexdigest()

def calculate_checksum(content: bytes) -> str:
    """Calculate SHA-256 checksum of content"""
    return hashlib.sha256(content).hexdigest()
