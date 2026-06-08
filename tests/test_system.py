"""
Comprehensive test suite for the distributed file storage system
"""
import os
import sys
import time
import tempfile
import shutil
import unittest
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.storage_node import StorageNode
from core.coordinator import CoordinatorService
from core.file_metadata import FileMetadata, VectorClock
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.recovery_manager import RecoveryManager
from replication.replication_manager import ReplicationManager, ConsistencyLevel
from time_sync.ntp_client import NTPClient
from consensus.raft_node import RaftNode

class TestDistributedFileSystem(unittest.TestCase):
    """Test cases for the distributed file storage system"""
    
    def setUp(self):
        """Set up test environment"""
        # Create temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.storage_dir = os.path.join(self.test_dir, 'storage')
        
        # Create storage nodes
        self.storage_nodes = []
        for i in range(3):
            node_id = f"TestNode-{i}"
            port = 6000 + i
            node = StorageNode(node_id, self.storage_dir, port)
            self.storage_nodes.append(node)
        
        # Create coordinator
        self.coordinator = CoordinatorService(
            storage_nodes=self.storage_nodes,
            replication_factor=2,
            consistency_level="QUORUM"
        )
        
        # Create replication manager
        self.replication_manager = ReplicationManager(
            storage_nodes=self.storage_nodes,
            replication_factor=2,
            consistency_level=ConsistencyLevel.QUORUM
        )
        
        # Create failure detector
        self.failure_detector = FailureDetector(
            storage_nodes=self.storage_nodes,
            heartbeat_interval=1,
            timeout=3
        )
        
        # Create recovery manager
        self.recovery_manager = RecoveryManager(
            storage_nodes=self.storage_nodes,
            replication_factor=2
        )
        
        # Create NTP client
        self.ntp_client = NTPClient(sync_interval=3600)
    
    def tearDown(self):
        """Clean up test environment"""
        # Stop all components
        self.failure_detector.stop()
        self.ntp_client.stop()
        
        for node in self.storage_nodes:
            node.stop()
        
        # Remove test directory
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_storage_node_basic_operations(self):
        """Test basic storage node operations"""
        node = self.storage_nodes[0]
        
        # Test file storage
        file_id = "test_file_1"
        content = b"Hello, World!"
        metadata = FileMetadata(
            file_id=file_id,
            filename="test.txt",
            size=len(content),
            timestamp=time.time(),
            vector_clock=VectorClock("test_client").to_dict(),
            checksum="test_checksum",
            replicas=[],
            created_by="test_client",
            last_modified=time.time()
        )
        
        # Store file
        success = node.store_file(file_id, content, metadata)
        self.assertTrue(success)
        
        # Check if file exists
        self.assertTrue(node.has_file(file_id))
        
        # Retrieve file
        retrieved_content, retrieved_metadata = node.retrieve_file(file_id)
        self.assertEqual(retrieved_content, content)
        self.assertEqual(retrieved_metadata.file_id, file_id)
        
        # Delete file
        success = node.delete_file(file_id)
        self.assertTrue(success)
        self.assertFalse(node.has_file(file_id))
    
    def test_replication_manager_write_read(self):
        """Test replication manager write and read operations"""
        file_id = "test_replication_file"
        content = b"Replication test content"
        metadata = FileMetadata(
            file_id=file_id,
            filename="replication_test.txt",
            size=len(content),
            timestamp=time.time(),
            vector_clock=VectorClock("test_client").to_dict(),
            checksum="test_checksum",
            replicas=[],
            created_by="test_client",
            last_modified=time.time()
        )
        
        # Write file with replication
        success = self.replication_manager.write_file(file_id, content, metadata)
        self.assertTrue(success)
        
        # Check that file is replicated
        replicas = self.replication_manager.get_file_replicas(file_id)
        self.assertGreaterEqual(len(replicas), 1)
        
        # Read file
        retrieved_content, retrieved_metadata = self.replication_manager.read_file(file_id)
        self.assertEqual(retrieved_content, content)
        self.assertEqual(retrieved_metadata.file_id, file_id)
    
    def test_failure_detector(self):
        """Test failure detection"""
        # Start failure detector
        self.failure_detector.start()
        time.sleep(2)  # Let it run for a bit
        
        # Check initial status
        health = self.failure_detector.get_system_health()
        self.assertEqual(health['total_nodes'], 3)
        self.assertEqual(health['active_nodes'], 3)
        
        # Simulate node failure
        failed_node = self.storage_nodes[0]
        failed_node.stop()
        time.sleep(5)  # Wait for detection
        
        # Check that failure is detected
        health = self.failure_detector.get_system_health()
        self.assertEqual(health['active_nodes'], 2)
        self.assertEqual(health['failed_nodes'], 1)
        self.assertIn(failed_node.node_id, health['failed_node_ids'])
    
    def test_ntp_client_sync(self):
        """Test NTP client synchronization"""
        # Start NTP client
        self.ntp_client.start()
        time.sleep(2)  # Let it sync
        
        # Check sync status
        status = self.ntp_client.get_sync_status()
        self.assertTrue(status['is_synced'])
        self.assertIsNotNone(status['offset'])
        self.assertIsNotNone(status['delay'])
    
    def test_coordinator_operations(self):
        """Test coordinator file operations"""
        # Upload file
        filename = "coordinator_test.txt"
        content = b"Coordinator test content"
        file_id = self.coordinator.upload_file(filename, content)
        self.assertIsNotNone(file_id)
        
        # Download file
        downloaded_content = self.coordinator.download_file(filename)
        self.assertEqual(downloaded_content, content)
        
        # List files
        files = self.coordinator.list_files()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], filename)
        
        # Delete file
        success = self.coordinator.delete_file(filename)
        self.assertTrue(success)
        
        # Check file is deleted
        files = self.coordinator.list_files()
        self.assertEqual(len(files), 0)
    
    def test_vector_clock_operations(self):
        """Test vector clock operations"""
        vc1 = VectorClock("client1")
        vc1 = vc1.increment("client1")
        
        vc2 = VectorClock("client2")
        vc2 = vc2.increment("client2")
        
        # Test comparison
        comparison = vc1.compare(vc2)
        self.assertEqual(comparison, "concurrent")
        
        # Test update
        vc3 = vc1.update(vc2)
        self.assertGreaterEqual(vc3.clocks.get("client1", 0), 1)
        self.assertGreaterEqual(vc3.clocks.get("client2", 0), 1)
    
    def test_consistency_levels(self):
        """Test different consistency levels"""
        # Test ONE consistency
        self.replication_manager.consistency_level = ConsistencyLevel.ONE
        required_writes = self.replication_manager._get_required_writes()
        self.assertEqual(required_writes, 1)
        
        # Test QUORUM consistency
        self.replication_manager.consistency_level = ConsistencyLevel.QUORUM
        required_writes = self.replication_manager._get_required_writes()
        self.assertEqual(required_writes, 2)  # (2 // 2) + 1 = 2
        
        # Test ALL consistency
        self.replication_manager.consistency_level = ConsistencyLevel.ALL
        required_writes = self.replication_manager._get_required_writes()
        self.assertEqual(required_writes, 2)  # replication_factor = 2
    
    def test_system_integration(self):
        """Test system integration"""
        # Set up component dependencies
        self.coordinator.set_components(
            self.failure_detector,
            self.replication_manager,
            self.ntp_client,
            None
        )
        
        # Start components
        self.failure_detector.start()
        self.ntp_client.start()
        
        # Test file operations through coordinator
        filename = "integration_test.txt"
        content = b"Integration test content"
        
        # Upload
        file_id = self.coordinator.upload_file(filename, content)
        self.assertIsNotNone(file_id)
        
        # Download
        downloaded_content = self.coordinator.download_file(filename)
        self.assertEqual(downloaded_content, content)
        
        # Get system status
        status = self.coordinator.get_system_status()
        self.assertEqual(status['total_nodes'], 3)
        self.assertEqual(status['active_nodes'], 3)
        self.assertEqual(status['total_files'], 1)

def run_tests():
    """Run all tests"""
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDistributedFileSystem)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
