#!/usr/bin/env python3

import json
import socket
import threading
import time
import logging
import random
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

class RaftState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE" 
    LEADER = "LEADER"

@dataclass
class LogEntry:
    term: int
    index: int
    command: str

class RaftNode:
    def __init__(self, node_id: str, peers: List[Tuple[str, int]], rpc_port: int):
        self.node_id = node_id
        self.peers = peers
        self.rpc_port = rpc_port
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        self.state = RaftState.FOLLOWER
        self.leader_id: Optional[str] = None
        
        # Leader state (volatile)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing configuration
        self.running = False
        self.election_timeout_min = 5.0   # 5 seconds minimum
        self.election_timeout_max = 10.0  # 10 seconds maximum  
        self.heartbeat_interval = 2.0     # 2 seconds between heartbeats
        self.election_timeout = self._randomize_election_timeout()
        self.last_heartbeat_received = time.time()
        self.last_heartbeat_sent = 0.0
        
        # Threading
        self.lock = threading.RLock()  # Reentrant lock for nested calls
        self.election_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.rpc_thread: Optional[threading.Thread] = None
        self.server_socket: Optional[socket.socket] = None
        
        # State management flags
        self.election_in_progress = False
        self.shutdown_requested = False
        
        logger.info(f"Raft node {self.node_id} initialized with {len(self.peers)} peers")
    
    def _randomize_election_timeout(self) -> float:
        """Return a randomized election timeout with extra jitter"""
        # Use wider range and extra jitter
        base_timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        
        # Add extra offset based on system time + node hash
        import hashlib
        seed_string = f"{self.node_id}{time.time()}{random.random()}"
        hash_val = int(hashlib.md5(seed_string.encode()).hexdigest()[:8], 16)
        random_offset = (hash_val % 5000) / 1000.0  # 0-5 second random offset
        
        timeout = base_timeout + random_offset
        logger.debug(f"Node {self.node_id} election timeout: {timeout:.1f}s")
        return timeout
    
    def _reset_election_timeout(self):
        """Reset election timeout with new randomization"""
        self.election_timeout = self._randomize_election_timeout()
        self.last_heartbeat_received = time.time()
        logger.debug(f"Node {self.node_id} reset election timeout to {self.election_timeout:.1f}s")
    
    def start(self, rpc_port: int):
        """Start the Raft node with proper initialization"""
        with self.lock:
            if self.running:
                logger.warning(f"Node {self.node_id} already running")
                return
                
            self.running = True
            self.shutdown_requested = False
            self.state = RaftState.FOLLOWER
            # Reset term and vote for fresh start
            self.current_term = 0
            self.voted_for = None
            self.leader_id = None
            self.election_in_progress = False
            self._reset_election_timeout()
        
        # Start RPC server first
        self._start_rpc_server(rpc_port)
        
        # Small delay to let RPC server start
        time.sleep(0.1)
        
        # Start election timer thread
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True, name=f"Election-{self.node_id}")
        self.election_thread.start()
        
    
    def stop(self):
        """Stop the Raft node with clean shutdown"""
        logger.info(f"Stopping Raft node {self.node_id}")
        
        with self.lock:
            self.running = False
            self.shutdown_requested = True
            # Don't change state or term - let other nodes handle leader election naturally
        
        # Close server socket to stop accepting connections
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        # Wait for threads to finish with timeout
        threads = [self.election_thread, self.heartbeat_thread, self.rpc_thread]
        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=2)
        
        logger.info(f"Raft node {self.node_id} stopped")
    
    def _start_rpc_server(self, port: int):
        """Start RPC server for Raft communication"""
        self.rpc_thread = threading.Thread(target=self._rpc_server_loop, args=(port,), daemon=True, name=f"RPC-{self.node_id}")
        self.rpc_thread.start()
        time.sleep(0.5)  # Give server time to start
    
    def _rpc_server_loop(self, port: int):
        """RPC server main loop"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('localhost', port))
            self.server_socket.listen(10)
            logger.info(f"Raft RPC server listening on port {port}")
            
            while self.running and not self.shutdown_requested:
                try:
                    self.server_socket.settimeout(1.0)  # 1 second timeout
                    conn, addr = self.server_socket.accept()
                    # Handle each request in separate thread
                    handler_thread = threading.Thread(target=self._handle_rpc_request, args=(conn, addr), daemon=True)
                    handler_thread.start()
                except socket.timeout:
                    continue
                except OSError:
                    # Socket closed during shutdown
                    break
                except Exception as e:
                    if self.running:
                        logger.error(f"RPC server error: {e}")
                        time.sleep(0.1)
                        
        except Exception as e:
            logger.error(f"Failed to start RPC server on port {port}: {e}")
        finally:
            if self.server_socket:
                try:
                    self.server_socket.close()
                except:
                    pass
    
    def _handle_rpc_request(self, conn: socket.socket, addr: Tuple[str, int]):
        """Handle incoming RPC request"""
        try:
            # Set receive timeout
            conn.settimeout(5.0)
            data = conn.recv(4096)
            if not data:
                return
                
            message = json.loads(data.decode())
            response = {}
            
            request_type = message.get('type')
            if request_type == 'RequestVote':
                response = self._handle_request_vote(message)
            elif request_type == 'AppendEntries':
                response = self._handle_append_entries(message)
            else:
                logger.warning(f"Unknown RPC type: {request_type}")
                return
            
            if response:
                response_data = json.dumps(response).encode()
                conn.sendall(response_data)
                
        except Exception as e:
            logger.debug(f"Error handling RPC request from {addr}: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _election_loop(self):
        """Election timer loop with randomized delays"""
        logger.info(f"Election loop started for node {self.node_id}")
        
        # Generate randomized wait time with multiple factors
        base_wait = random.uniform(2.0, 12.0)
        
        # Add randomization based on node creation time and system state
        time_microseconds = int(time.time() * 1000000) % 10000
        node_random_factor = random.random() * 5.0
        system_entropy = (time_microseconds / 10000.0) * 3.0
        
        # Sometimes give Node-1 or Node-2 advantage to break Node-0 bias
        if random.random() < 0.6:  # 60% chance to favor other nodes
            if self.node_id == "Node-1":
                base_wait -= 2.0  # Node-1 gets advantage
            elif self.node_id == "Node-2":
                base_wait -= 1.5  # Node-2 gets some advantage
            elif self.node_id == "Node-0":
                base_wait += 1.0  # Node-0 gets slight penalty
        
        initial_wait = max(2.0, base_wait + node_random_factor + system_entropy)
            
        logger.info(f"Node {self.node_id} waiting {initial_wait:.1f}s before first election check")
        time.sleep(initial_wait)
        
        while self.running and not self.shutdown_requested:
            try:
                time.sleep(1.0)  # Check every 1 second for faster response
                
                with self.lock:
                    if not self.running or self.shutdown_requested:
                        break
                        
                    # Only followers and candidates should start elections  
                    if self.state == RaftState.LEADER:
                        continue
                    
                    # Skip if election already in progress
                    if self.election_in_progress:
                        continue
                    
                    # Check if election timeout has occurred
                    time_since_heartbeat = time.time() - self.last_heartbeat_received
                    if time_since_heartbeat > self.election_timeout:
                        logger.info(f"Election timeout reached ({time_since_heartbeat:.2f}s), starting election")
                        self._start_election()
                        
            except Exception as e:
                logger.error(f"Error in election loop for {self.node_id}: {e}")
                time.sleep(1)
                
        logger.info(f"Election loop stopped for node {self.node_id}")
    
    def _start_election(self):
        """Start a new election"""
        # This method is called with lock already held
        if self.election_in_progress:
            logger.debug(f"Election already in progress for {self.node_id}")
            return
            
        self.election_in_progress = True
        
        # Become candidate
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._reset_election_timeout()
        
        election_term = self.current_term
        logger.info(f"Node {self.node_id} starting election for term {election_term}")
        
        # Start election in separate thread to avoid blocking
        election_thread = threading.Thread(target=self._conduct_election, args=(election_term,), daemon=True)
        election_thread.start()
    
    def _conduct_election(self, election_term: int):
        """Conduct the election process"""
        try:
            # Count our own vote
            votes_received = 1
            total_nodes = len(self.peers) + 1
            majority_needed = (total_nodes // 2) + 1
            
            logger.debug(f"Node {self.node_id} needs {majority_needed}/{total_nodes} votes")
            
            # Request votes from all peers concurrently
            vote_threads = []
            vote_results = []
            
            for peer_host, peer_port in self.peers:
                vote_thread = threading.Thread(
                    target=self._request_vote_worker,
                    args=(peer_host, peer_port, election_term, vote_results),
                    daemon=True
                )
                vote_threads.append(vote_thread)
                vote_thread.start()
            
            # Wait for all vote requests with timeout
            for thread in vote_threads:
                thread.join(timeout=5.0)  # 5 second timeout per vote
            
            # Count votes
            for vote_granted in vote_results:
                if vote_granted:
                    votes_received += 1
            
            # Process election result with lock
            with self.lock:
                self.election_in_progress = False
                
                # Check if we're still candidate and in the same term
                if (self.state != RaftState.CANDIDATE or 
                    self.current_term != election_term or 
                    not self.running):
                    logger.info(f"Election aborted - state changed during voting")
                    return
                
                # Check if we won the election
                if votes_received >= majority_needed:
                    logger.info(f"Majority achieved: {votes_received}/{majority_needed} votes - becoming leader")
                    self._become_leader()
                else:
                    logger.info(f"Election lost: {votes_received}/{majority_needed} votes")
                    self._become_follower(election_term)
                    
        except Exception as e:
            logger.error(f"Error during election for {self.node_id}: {e}")
            with self.lock:
                self.election_in_progress = False
                self._become_follower(self.current_term)
    
    def _request_vote_worker(self, peer_host: str, peer_port: int, election_term: int, results: List[bool]):
        """Worker thread for requesting votes"""
        try:
            vote_granted = self._request_vote_from_peer(peer_host, peer_port, election_term)
            results.append(vote_granted)
        except Exception as e:
            logger.debug(f"Vote request worker error for {peer_host}:{peer_port}: {e}")
            results.append(False)
    
    def _request_vote_from_peer(self, peer_host: str, peer_port: int, election_term: int) -> bool:
        """Request vote from a specific peer"""
        try:
            # Prepare vote request
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else -1
            
            message = {
                'type': 'RequestVote',
                'term': election_term,
                'candidate_id': self.node_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            }
            
            # Send vote request with timeout
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(4.0)  # 4 second timeout
                s.connect((peer_host, peer_port))
                s.sendall(json.dumps(message).encode())
                
                response = s.recv(1024)
                if response:
                    data = json.loads(response.decode())
                    
                    # Update term if we see a higher one
                    response_term = data.get('term', 0)
                    if response_term > election_term:
                        with self.lock:
                            if response_term > self.current_term:
                                logger.info(f"Discovered higher term {response_term}, stepping down")
                                self._become_follower(response_term)
                        return False
                    
                    vote_granted = data.get('vote_granted', False)
                    if vote_granted:
                        logger.info(f"Vote granted for {self.node_id} in term {election_term}")
                    else:
                        logger.info(f"Vote denied for {self.node_id} in term {election_term}")
                    
                    return vote_granted
                    
        except Exception as e:
            logger.debug(f"RequestVote to {peer_host}:{peer_port} failed: {e}")
            
        return False
    
    def _handle_request_vote(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle RequestVote RPC - THREAD SAFE"""
        with self.lock:
            term = message.get('term', 0)
            candidate_id = message.get('candidate_id', '')
            last_log_index = message.get('last_log_index', -1)
            last_log_term = message.get('last_log_term', -1)
            
            # Default response
            response = {
                'term': self.current_term,
                'vote_granted': False
            }
            
            # If candidate's term is higher, update our term and become follower
            if term > self.current_term:
                self._become_follower(term)
                response['term'] = self.current_term
            
            # Reject if candidate's term is stale
            if term < self.current_term:
                logger.info(f"Rejected vote for {candidate_id} - stale term {term} < {self.current_term}")
                return response
            
            # Check if we've already voted in this term
            if self.voted_for is not None and self.voted_for != candidate_id:
                logger.info(f"Already voted for {self.voted_for} in term {term}")
                return response
            
            # Check if candidate's log is at least as up-to-date as ours
            our_last_log_term = self.log[-1].term if self.log else -1
            our_last_log_index = len(self.log) - 1 if self.log else -1
            
            log_ok = (last_log_term > our_last_log_term or 
                     (last_log_term == our_last_log_term and last_log_index >= our_last_log_index))
            
            if log_ok:
                # Grant vote
                self.voted_for = candidate_id
                self._reset_election_timeout()  # Reset timeout when granting vote
                response['vote_granted'] = True
                logger.info(f"Voted for {candidate_id} in term {term}")
            else:
                logger.info(f"Rejected vote for {candidate_id} - outdated log")
            
            return response
    
    def _become_leader(self):
        """Become leader; called with lock held"""
        if self.state == RaftState.LEADER:
            logger.warning(f"Node {self.node_id} already leader")
            return
            
        self.state = RaftState.LEADER
        self.leader_id = self.node_id
        self.voted_for = None  # Clear vote for next term
        
        # Initialize leader state
        next_index = len(self.log)
        for peer_host, peer_port in self.peers:
            peer_id = f"{peer_host}:{peer_port}"
            self.next_index[peer_id] = next_index
            self.match_index[peer_id] = -1
        
        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")
        
        # Start sending heartbeats immediately
        self._start_heartbeat_thread()
    
    def _become_follower(self, term: int):
        """Become follower; called with lock held"""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            
        self.state = RaftState.FOLLOWER
        self.leader_id = None
        self._reset_election_timeout()
        
        # Stop heartbeat thread if we were leader
        self._stop_heartbeat_thread()
        
        logger.info(f"Node {self.node_id} became follower for term {self.current_term}")
    
    def _start_heartbeat_thread(self):
        """Start heartbeat thread for leader"""
        self._stop_heartbeat_thread()  # Stop any existing thread
        
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, 
            daemon=True, 
            name=f"Heartbeat-{self.node_id}"
        )
        self.heartbeat_thread.start()
        logger.info(f"Started heartbeat thread for leader {self.node_id}")
    
    def _stop_heartbeat_thread(self):
        """Stop heartbeat thread"""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            # Thread will stop when it checks self.state
            self.heartbeat_thread.join(timeout=2)
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats as leader"""
        logger.info(f"Heartbeat loop started for leader {self.node_id}")
        
        while self.running and not self.shutdown_requested:
            with self.lock:
                if self.state != RaftState.LEADER:
                    logger.info(f"No longer leader, stopping heartbeats for {self.node_id}")
                    break
            
            # Send heartbeats to all peers
            self._send_heartbeats()
            
            # Wait for next heartbeat interval
            time.sleep(self.heartbeat_interval)
        
        logger.info(f"Heartbeat loop stopped for {self.node_id}")
    
    def _send_heartbeats(self):
        """Send AppendEntries (heartbeat) to all peers - PARALLEL"""
        with self.lock:
            if self.state != RaftState.LEADER:
                return
                
            current_term = self.current_term
            leader_id = self.node_id
        
        # Send heartbeats to all peers in parallel
        heartbeat_threads = []
        for peer_host, peer_port in self.peers:
            thread = threading.Thread(
                target=self._send_heartbeat_to_peer,
                args=(peer_host, peer_port, current_term, leader_id),
                daemon=True
            )
            heartbeat_threads.append(thread)
            thread.start()
        
        # Wait for all heartbeats to complete (with timeout)
        for thread in heartbeat_threads:
            thread.join(timeout=3.0)
            
        self.last_heartbeat_sent = time.time()
    
    def _send_heartbeat_to_peer(self, peer_host: str, peer_port: int, term: int, leader_id: str):
        """Send heartbeat to specific peer"""
        try:
            # Prepare heartbeat message (empty AppendEntries)
            message = {
                'type': 'AppendEntries',
                'term': term,
                'leader_id': leader_id,
                'prev_log_index': -1,
                'prev_log_term': -1,
                'entries': [],
                'leader_commit': self.commit_index
            }
            
            # Send heartbeat with timeout
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3.0)
                s.connect((peer_host, peer_port))
                s.sendall(json.dumps(message).encode())
                
                response = s.recv(1024)
                if response:
                    data = json.loads(response.decode())
                    
                    # Check for higher term
                    response_term = data.get('term', 0)
                    if response_term > term:
                        with self.lock:
                            if response_term > self.current_term:
                                logger.info(f"Heartbeat response shows higher term {response_term}, stepping down")
                                self._become_follower(response_term)
                    
                    # If heartbeat was rejected due to higher term, we're no longer leader
                    success = data.get('success', False)
                    if not success and response_term > term:
                        logger.info(f"Heartbeat rejected by {peer_host}:{peer_port} due to higher term")
                    
        except Exception as e:
            logger.debug(f"Heartbeat to {peer_host}:{peer_port} failed: {e}")
            # Failed heartbeats are normal during network issues or node failures
    
    def _handle_append_entries(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle AppendEntries RPC (heartbeat) - THREAD SAFE"""
        with self.lock:
            term = message.get('term', 0)
            leader_id = message.get('leader_id', '')
            
            # Default response
            response = {
                'term': self.current_term,
                'success': False
            }
            
            # If leader's term is higher, update our term and become follower
            if term > self.current_term:
                logger.info(f"Received AppendEntries with higher term {term} from {leader_id}")
                self._become_follower(term)
                response['term'] = self.current_term
            
            # Reject if leader's term is stale
            if term < self.current_term:
                logger.info(f"Rejecting AppendEntries from {leader_id} - stale term {term} < {self.current_term}")
                return response
            
            # Valid heartbeat from current leader
            if term == self.current_term:
                self.leader_id = leader_id
                self._reset_election_timeout()  # Reset election timeout
                
                # If we were candidate, become follower
                if self.state == RaftState.CANDIDATE:
                    logger.info(f"Received valid heartbeat while candidate, becoming follower")
                    self._become_follower(term)
                
                response['success'] = True
                logger.debug(f"Accepted heartbeat from leader {leader_id} in term {term}")
            
            return response
    
    def get_state(self) -> Dict[str, Any]:
        """Get current node state - THREAD SAFE"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.current_term,
                'leader': self.leader_id,
                'log_length': len(self.log),
                'commit_index': self.commit_index,
                'voted_for': self.voted_for,
                'last_heartbeat': time.time() - self.last_heartbeat_received
            }
    
    def get_status(self) -> Dict[str, Any]:
        """Get current node status"""
        return self.get_state()
    
    def is_leader(self) -> bool:
        """Check if this node is leader"""
        with self.lock:
            return self.state == RaftState.LEADER
    
    def get_leader(self) -> Optional[str]:
        """Get current leader ID"""
        with self.lock:
            return self.leader_id
    
    def propose_operation(self, operation: Dict[str, Any]) -> bool:
        """Propose an operation for consensus (only leader can do this)"""
        with self.lock:
            if self.state != RaftState.LEADER:
                logger.warning(f"Only leader can propose operations, current state: {self.state}")
                return False
            
            # Create log entry for the operation
            log_entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                command=json.dumps(operation)
            )
            
            # Add to log
            self.log.append(log_entry)
            logger.info(f"Leader {self.node_id} proposed operation: {operation['type']}")
            
            # In a full implementation, we would replicate this to followers
            # For now, we'll commit it locally as proof of concept
            self.commit_index = len(self.log) - 1
            
            return True
    
    def get_leader_node(self, raft_nodes: List['RaftNode']) -> Optional['RaftNode']:
        """Get the current leader node from a list of raft nodes"""
        with self.lock:
            current_leader_id = self.leader_id
            
        if not current_leader_id:
            return None
            
        for node in raft_nodes:
            if node.node_id == current_leader_id:
                return node
        
        return None