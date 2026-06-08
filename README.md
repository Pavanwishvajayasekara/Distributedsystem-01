# Distributed File Storage System

# Group Members

- **IT23585126** - I.N.Hewawitharana (IT23585126@my.sliit.lk) 
- **IT23619258** - K.A.M.K.Kaluarachchi (IT23619258@my.sliit.lk) 
- **IT23750838** - G.P.W.Jayasekara (IT23750838@my.sliit.lk) 
- **IT23596634** - D.M.Ravidu Ravisara (IT23596634@my.sliit.lk) 


A comprehensive, fault-tolerant distributed file storage system implementing all four core requirements:

1. **Fault Tolerance** - Real failure detection and recovery
2. **Data Replication & Consistency** - Multiple consistency levels with vector clocks
3. **Time Synchronization** - NTP-based clock synchronization
4. **Consensus & Agreement** - Raft consensus algorithm

## Features

### Core Functionality
- **Real Network Communication** - Actual TCP sockets, no simulation
- **Fault Tolerance** - Heartbeat-based failure detection with automatic recovery
- **Data Replication** - Configurable replication factor with consistent hashing
- **Consistency Models** - ONE, QUORUM, and ALL consistency levels
- **Vector Clocks** - Conflict resolution for concurrent operations
- **Time Synchronization** - NTP client with multiple servers and clock skew detection
- **Raft Consensus** - Complete Raft implementation with leader election
- **Modern Web Interface** - Real-time monitoring dashboard

### Technical Highlights
- **Production Ready** - Real networking, proper error handling, comprehensive logging
- **Scalable Architecture** - Modular design with clear separation of concerns
- **Comprehensive Testing** - Full test suite covering all components
- **Real-time Monitoring** - WebSocket-based live updates
- **Configuration Management** - Centralized configuration system

## System Architecture

```
DistributedFileSystem/
├── main.py                     # Entry point
├── config.py                   # Configuration
├── requirements.txt            # Dependencies
├── core/                       # Core components
│   ├── coordinator.py          # Main coordinator
│   ├── storage_node.py         # Storage nodes
│   └── file_metadata.py        # File metadata & vector clocks
├── fault_tolerance/            # Fault tolerance
│   ├── failure_detector.py     # Real failure detection
│   └── recovery_manager.py     # Data recovery
├── replication/                # Data replication
│   └── replication_manager.py  # Replication & consistency
├── time_sync/                  # Time synchronization
│   └── ntp_client.py          # NTP client
├── consensus/                  # Consensus algorithm
│   └── raft_node.py           # Raft implementation
├── web/                        # Web interface
│   ├── app.py                 # Flask application
│   └── templates/
│       └── index.html         # Dashboard
└── tests/                      # Test suite
    └── test_system.py         # Comprehensive tests
```

## Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DistributedFileSystem
   ```

2. (Recommended) create a fresh virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Run the system:
   ```bash
   python main.py
   ```

4. Open your browser and navigate to:
   ```
   http://localhost:5000
   ```

## Usage

### Web Interface
The system provides a modern web interface with:
- **Real-time System Monitoring** - Live node status and health metrics
- **File Management** - Upload, download, and delete files
- **Consistency Control** - Switch between ONE, QUORUM, and ALL consistency
- **Node Management** - Start/stop individual nodes
- **Raft Status** - Monitor consensus algorithm state
- **System Statistics** - Overall system health and performance

### API Endpoints
- `GET /api/status` - Get system status
- `GET /api/files` - List all files
- `POST /api/upload` - Upload a file
- `GET /api/download/<filename>` - Download a file
- `DELETE /api/delete/<filename>` - Delete a file
- `POST /api/nodes/<node_id>/stop` - Stop a node
- `POST /api/nodes/<node_id>/start` - Start a node
- `POST /api/consistency/<level>` - Set consistency level

## Configuration

Edit `config.py` to customize:
- Number of storage nodes
- Replication factor
- Consistency level
- Network ports
- Heartbeat intervals
- NTP servers
- Logging levels

## Testing

Run the comprehensive test suite:
```bash
python tests/test_system.py
```

The test suite covers:
- Storage node operations
- Replication and consistency
- Failure detection and recovery
- Time synchronization
- Vector clock operations
- System integration

## Key Improvements Over Reference Implementation

### 1. Real Network Communication
- **Before**: Simulated RPC calls
- **After**: Actual TCP socket communication with proper error handling

### 2. Proper Fault Tolerance
- **Before**: Basic heartbeat simulation
- **After**: Real failure detection with automatic recovery and data redistribution

### 3. Enhanced Time Synchronization
- **Before**: Basic NTP with no error handling
- **After**: Multi-server NTP with clock skew detection and fallback mechanisms

### 4. Complete Raft Implementation
- **Before**: Simulated consensus
- **After**: Full Raft algorithm with real leader election and log replication

### 5. Modern Web Interface
- **Before**: Basic HTML with limited functionality
- **After**: Modern, responsive dashboard with real-time updates and comprehensive monitoring

### 6. Production-Ready Features
- **Before**: Prototype-level implementation
- **After**: Comprehensive logging, error handling, configuration management, and testing

## Technical Specifications

### Fault Tolerance
- Real TCP heartbeat monitoring
- Configurable timeout intervals
- Automatic data recovery
- Node failure simulation
- Health monitoring dashboard

### Data Replication
- Consistent hashing for load balancing
- Vector clock conflict resolution
- Configurable replication factor
- Multiple consistency levels
- Real-time replica tracking

### Time Synchronization
- Multi-server NTP implementation
- Clock skew detection and reporting
- Automatic fallback mechanisms
- Network delay compensation
- Synchronization accuracy metrics

### Consensus Algorithm
- Complete Raft implementation
- Real leader election
- Log replication with persistence
- Split-brain prevention
- Term management and state transitions

## Performance Characteristics

- **Latency**: Sub-second response times for most operations
- **Throughput**: Handles multiple concurrent operations
- **Scalability**: Supports multiple storage nodes
- **Reliability**: Automatic failure recovery
- **Consistency**: Configurable consistency guarantees

## Monitoring and Observability

- Real-time system status
- Node health monitoring
- File operation tracking
- Consensus algorithm state
- Time synchronization status
- Performance metrics

## Future Enhancements

- Data encryption at rest and in transit
- Horizontal scaling with dynamic node addition
- Advanced conflict resolution algorithms
- Performance optimization
- Enhanced security features
- Cloud deployment support

## License

This project is developed for educational purposes as part of the SE2062 - Distributed Systems course.


**Note**: This implementation represents a significant improvement over reference implementations, providing production-ready features, real networking, comprehensive testing, and modern user interface while maintaining all required functionality.
