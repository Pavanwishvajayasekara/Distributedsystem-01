#!/usr/bin/env python3
"""
Main entry point for the Distributed File Storage System
"""
import os
import sys
import logging
import signal
import time
import webbrowser
import threading
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web.app import initialize_system, coordinator, storage_nodes, raft_nodes, failure_detector, ntp_client
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.Config.LOG_LEVEL),
    format=config.Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received, cleaning up...")
    
    # Stop all components
    if failure_detector:
        failure_detector.stop()
    
    if ntp_client:
        ntp_client.stop()
    
    for raft_node in raft_nodes:
        raft_node.stop()
    
    for storage_node in storage_nodes:
        storage_node.stop()
    
    logger.info("Cleanup completed")
    sys.exit(0)

def main():
    """Main function"""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create storage directory
    storage_dir = Path(config.Config.STORAGE_DIR)
    storage_dir.mkdir(exist_ok=True)
    
    logger.info("Starting Distributed File Storage System...")
    logger.info(f"Configuration: {config.Config.NODE_COUNT} nodes, "
               f"replication factor: {config.Config.REPLICATION_FACTOR}, "
               f"consistency: {config.Config.CONSISTENCY_LEVEL}")
    
    try:
        # Initialize system
        initialize_system()
        
        # Start web server
        from web.app import app, socketio
        web_url = config.Config.BROWSER_URL  # Use localhost for browser
        server_host = config.Config.WEB_HOST  # Use 0.0.0.0 for server binding
        logger.info(f"Web interface available at {web_url}")
        
        # Auto-open browser once server starts
        def open_browser():
            time.sleep(2)  # Wait briefly for server to start
            logger.info("Opening web interface in your default browser...")
            webbrowser.open(web_url)

        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()
        
        socketio.run(
            app,
            host=server_host,  # Use 0.0.0.0 for server binding
            port=config.Config.WEB_PORT,
            debug=config.Config.DEBUG,
            allow_unsafe_werkzeug=True  # Allow Werkzeug for development
        )
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        signal_handler(None, None)

if __name__ == "__main__":
    main()
