"""
Configuration settings for the Distributed File Storage System
"""
import os

class Config:
    # System Configuration
    NODE_COUNT = 3
    REPLICATION_FACTOR = 3
    CONSISTENCY_LEVEL = "QUORUM"  # ONE, QUORUM, ALL
    
    # Network Configuration
    BASE_PORT = 5001
    HEARTBEAT_INTERVAL = 5  # seconds
    HEARTBEAT_TIMEOUT = 15  # seconds
    RPC_TIMEOUT = 10  # seconds
    
    # Storage Configuration
    STORAGE_DIR = "storage"
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
    
    # Time Synchronization
    NTP_SERVERS = ['pool.ntp.org', 'time.google.com', 'time.cloudflare.com']
    SYNC_INTERVAL = 3600  # 1 hour
    
    # Raft Configuration - Optimized for fast leader election
    ELECTION_TIMEOUT_MIN = 5000   # milliseconds (5 seconds) 
    ELECTION_TIMEOUT_MAX = 10000  # milliseconds (10 seconds)
    HEARTBEAT_INTERVAL_RAFT = 2000  # milliseconds (2 seconds)
    
    # Web Interface
    WEB_HOST = '0.0.0.0'  # Server binds to all interfaces
    WEB_PORT = 5000
    DEBUG = False  # Disabled to prevent auto-reload during testing
    
    # Browser URL (different from server bind address)
    BROWSER_URL = 'http://localhost:5000'
    
    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
