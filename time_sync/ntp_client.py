"""
NTP client for time synchronization
"""
import socket
import struct
import time
import threading
import logging
from typing import List, Optional, Tuple
import statistics

logger = logging.getLogger(__name__)

class NTPClient:
    """NTP client for time synchronization with multiple servers"""
    
    def __init__(self, servers: List[str] = None, sync_interval: int = 3600):
        self.servers = servers or ['pool.ntp.org', 'time.google.com', 'time.cloudflare.com']
        self.sync_interval = sync_interval
        
        # Time synchronization state
        self.offset = 0.0  # Time offset from NTP servers
        self.delay = 0.0   # Network delay
        self.last_sync = 0.0
        self.sync_accuracy = 0.0
        self.is_synced = False
        
        # Threading
        self.running = False
        self.sync_thread = None
        self.lock = threading.RLock()
        
        logger.info(f"NTP client initialized with servers: {self.servers}")
    
    def start(self):
        """Start periodic time synchronization"""
        if self.running:
            return
        
        self.running = True
        self.sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self.sync_thread.start()
        
        # Perform initial sync
        self.sync()
        
        logger.info("NTP client started")
    
    def stop(self):
        """Stop time synchronization"""
        self.running = False
        if self.sync_thread:
            self.sync_thread.join(timeout=2)
        logger.info("NTP client stopped")
    
    def _sync_loop(self):
        """Main synchronization loop"""
        while self.running:
            try:
                self.sync()
                time.sleep(self.sync_interval)
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def sync(self) -> bool:
        """Synchronize with NTP servers"""
        try:
            logger.info("Starting NTP synchronization...")
            
            # Query all servers
            results = []
            for server in self.servers:
                try:
                    offset, delay = self._query_ntp_server(server)
                    if offset is not None and delay is not None:
                        results.append((offset, delay, server))
                        logger.debug(f"NTP query to {server}: offset={offset:.6f}s, delay={delay:.6f}s")
                except Exception as e:
                    logger.warning(f"Failed to query NTP server {server}: {e}")
            
            if not results:
                logger.error("No NTP servers responded")
                return False
            
            # Select best result (lowest delay)
            best_offset, best_delay, best_server = min(results, key=lambda x: x[1])
            
            # Update synchronization state
            with self.lock:
                self.offset = best_offset
                self.delay = best_delay
                self.last_sync = time.time()
                self.sync_accuracy = self._calculate_accuracy(results)
                self.is_synced = True
            
            logger.info(f"NTP sync successful: offset={best_offset:.6f}s, "
                       f"delay={best_delay:.6f}s, server={best_server}")
            
            return True
            
        except Exception as e:
            logger.error(f"NTP synchronization failed: {e}")
            return False
    
    def _query_ntp_server(self, server: str) -> Tuple[Optional[float], Optional[float]]:
        """Query a specific NTP server"""
        try:
            # Create NTP request packet
            ntp_packet = bytearray(48)
            ntp_packet[0] = 0x1B  # Version 3, client mode
            
            # Send request
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                client.settimeout(5)
                
                # Record time before sending
                t1 = time.time()
                
                # Send to NTP server
                addr = socket.gethostbyname(server)
                client.sendto(ntp_packet, (addr, 123))
                
                # Receive response
                response, _ = client.recvfrom(48)
                
                # Record time after receiving
                t4 = time.time()
                
                # Parse response
                unpacked = struct.unpack('!12I', response)
                t2 = self._ntp_to_unix_time(unpacked[8], unpacked[9])   # Receive time
                t3 = self._ntp_to_unix_time(unpacked[10], unpacked[11])  # Transmit time
                
                # Calculate offset and delay
                delay = (t4 - t1) - (t3 - t2)
                offset = ((t2 - t1) + (t3 - t4)) / 2
                
                return offset, delay
                
        except Exception as e:
            logger.debug(f"Error querying NTP server {server}: {e}")
            return None, None
    
    def _ntp_to_unix_time(self, seconds: int, fraction: int) -> float:
        """Convert NTP timestamp to Unix time"""
        # NTP epoch is Jan 1, 1900, Unix epoch is Jan 1, 1970
        # 2208988800 is the number of seconds between the two epochs
        return seconds + fraction / 2**32 - 2208988800
    
    def _calculate_accuracy(self, results: List[Tuple[float, float, str]]) -> float:
        """Calculate synchronization accuracy based on multiple results"""
        if len(results) < 2:
            return 0.0
        
        # Calculate standard deviation of offsets
        offsets = [result[0] for result in results]
        return statistics.stdev(offsets) if len(offsets) > 1 else 0.0
    
    def get_time(self) -> float:
        """Get current time adjusted for NTP offset"""
        with self.lock:
            return time.time() + self.offset
    
    def get_offset(self) -> float:
        """Get current time offset"""
        with self.lock:
            return self.offset
    
    def get_delay(self) -> float:
        """Get current network delay"""
        with self.lock:
            return self.delay
    
    def get_sync_status(self) -> dict:
        """Get synchronization status"""
        with self.lock:
            return {
                'is_synced': self.is_synced,
                'offset': self.offset,
                'delay': self.delay,
                'last_sync': self.last_sync,
                'sync_accuracy': self.sync_accuracy,
                'time_since_sync': time.time() - self.last_sync if self.last_sync > 0 else None
            }
    
    def is_clock_skew_detected(self, threshold: float = 1.0) -> bool:
        """Check if clock skew is detected above threshold"""
        with self.lock:
            return abs(self.offset) > threshold
    
    def get_clock_skew_info(self) -> dict:
        """Get detailed clock skew information"""
        with self.lock:
            return {
                'offset_seconds': self.offset,
                'offset_milliseconds': self.offset * 1000,
                'is_skewed': self.is_clock_skew_detected(),
                'skew_severity': self._get_skew_severity(),
                'recommendation': self._get_skew_recommendation()
            }
    
    def _get_skew_severity(self) -> str:
        """Get clock skew severity level"""
        abs_offset = abs(self.offset)
        if abs_offset < 0.1:
            return "minimal"
        elif abs_offset < 1.0:
            return "low"
        elif abs_offset < 5.0:
            return "moderate"
        elif abs_offset < 10.0:
            return "high"
        else:
            return "critical"
    
    def _get_skew_recommendation(self) -> str:
        """Get recommendation for clock skew"""
        severity = self._get_skew_severity()
        
        if severity == "minimal":
            return "Clock is well synchronized"
        elif severity == "low":
            return "Minor clock adjustment recommended"
        elif severity == "moderate":
            return "Clock synchronization needed"
        elif severity == "high":
            return "Immediate clock synchronization required"
        else:
            return "Critical clock skew - system may be unstable"
