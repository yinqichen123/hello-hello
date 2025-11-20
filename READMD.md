# HelloHello - Replicated Post Server

A distributed, replicated post server using ZooKeeper for leader election and coordination, with gRPC for inter-server communication.

## Features

- ✅ **Leader Election**: Automatic leader election using ZooKeeper
- ✅ **Data Replication**: Star topology replication with strong consistency
- ✅ **Automatic Failover**: New leader elected when current leader fails
- ✅ **Read-After-Write Consistency**: Clients always see their writes
- ✅ **HTTP 308 Redirects**: Non-leader nodes redirect writes to leader
- ✅ **Automatic Sync**: New nodes automatically sync historical data
- ✅ **Token-based Authentication**: Secure post creation with HMAC tokens

## Architecture

### Components

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Client    │      │   Client    │      │   Client    │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       │ HTTP               │ HTTP               │ HTTP
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Server 1   │◄────►│  Server 2   │◄────►│  Server 3   │
│  (Leader)   │ gRPC │  (Replica)  │ gRPC │  (Replica)  │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                            │
                            ▼
                    ┌─────────────┐
                    │  ZooKeeper  │
                    └─────────────┘
```

### ZooKeeper Node Structure

```
/
├── /peers/
│   ├── server1  (data: "localhost:8080")
│   ├── server2  (data: "localhost:8081")
│   └── server3  (data: "localhost:8082")
├── /leader  (data: "server1", zxid: 123456)
└── /replicas  (data: "server1,server2,server3")
```

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- ZooKeeper 3.6+

## Quick Start

### 1. Start ZooKeeper

```bash
# Using Homebrew (macOS)
brew install zookeeper
zkServer start

# Using Docker
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.6
```

### 2. Build the Project

```bash
mvn clean compile
```

This will:
- Download all dependencies
- Generate gRPC code from `.proto` files
- Compile Java sources

### 3. Start Three Server Instances

In three separate terminals:

```bash
# Terminal 1 - Server 1
mvn spring-boot:run -Dspring-boot.run.profiles=server1

# Terminal 2 - Server 2
mvn spring-boot:run -Dspring-boot.run.profiles=server2

# Terminal 3 - Server 3
mvn spring-boot:run -Dspring-boot.run.profiles=server3
```

## API Usage

### 1. Get Authentication Token

```bash
curl "http://localhost:8080/getToken?superPass=superpass123&user=alice"
```

Response:
```
Vcs/X8treEsGGr9SdC8//5MYJy/OBpnsAXRlkZWVlxM=
```

### 2. Create a Post

```bash
TOKEN="Vcs/X8treEsGGr9SdC8//5MYJy/OBpnsAXRlkZWVlxM="

curl -X POST http://localhost:8080/posts \
  -H "Content-Type: application/json" \
  -d "{
    \"author\": \"alice\",
    \"message\": \"Hello, distributed world!\",
    \"token\": \"$TOKEN\"
  }"
```

Response:
```json
{
  "id": 1,
  "author": "alice",
  "message": "Hello, distributed world!",
  "txn": 1,
  "timestamp": 1699564800000,
  "committed": true
}
```

### 3. Read Posts

```bash
# Can read from any replica
curl "http://localhost:8080/posts?page=0&size=10"
curl "http://localhost:8081/posts?page=0&size=10"
curl "http://localhost:8082/posts?page=0&size=10"
```

## Testing Replication

### Test 1: Write Redirection

```bash
# Try to write to a non-leader (will get 308 redirect)
curl -v -X POST http://localhost:8081/posts \
  -H "Content-Type: application/json" \
  -d "{
    \"author\": \"alice\",
    \"message\": \"Test\",
    \"token\": \"$TOKEN\"
  }"

# Response will be:
# HTTP/1.1 308 Permanent Redirect
# Location: http://localhost:8080/posts
```

### Test 2: Leader Failover

```bash
# 1. Identify the leader
curl http://localhost:8080/posts

# 2. Stop the leader process (Ctrl+C in its terminal)

# 3. Watch logs - a new leader will be elected

# 4. Try creating a post - should still work with new leader
curl -X POST http://localhost:8081/posts \
  -H "Content-Type: application/json" \
  -d "{
    \"author\": \"alice\",
    \"message\": \"After failover\",
    \"token\": \"$TOKEN\"
  }"

# 5. Restart the old leader - it will rejoin as a replica
mvn spring-boot:run -Dspring-boot.run.profiles=server1
```

### Test 3: Data Consistency

```bash
# Create multiple posts
for i in {1..10}; do
  curl -X POST http://localhost:8080/posts \
    -H "Content-Type: application/json" \
    -d "{
      \"author\": \"alice\",
      \"message\": \"Message $i\",
      \"token\": \"$TOKEN\"
    }"
done

# Verify all replicas have the same data
curl "http://localhost:8080/posts?size=20" | jq '.content | length'
curl "http://localhost:8081/posts?size=20" | jq '.content | length'
curl "http://localhost:8082/posts?size=20" | jq '.content | length'
```

## Configuration

### Server Configuration Files

Each server needs its own configuration:

**`application-server1.properties`:**
```properties
server.port=8080
serverId=server1
grpc.port=9090
spring.datasource.url=jdbc:h2:file:/tmp/hellohello-server1
```

**`application-server2.properties`:**
```properties
server.port=8081
serverId=server2
grpc.port=9091
spring.datasource.url=jdbc:h2:file:/tmp/hellohello-server2
```

### Key Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serverId` | Unique identifier for this server | (required) |
| `server.port` | HTTP port | 8080 |
| `server.address` | Server hostname/IP | localhost |
| `grpc.port` | gRPC port for replication | 9090 |
| `zkConnectString` | ZooKeeper connection string | localhost:2181 |
| `zkNamespace` | ZooKeeper path prefix | (empty) |
| `superPass` | Super password for token generation | superpass123 |
| `token.secret` | Secret key for token signing | secret123 |

## Protocol Details

### gRPC Replication Protocol

Defined in `src/main/proto/postreplica.proto`:

```protobuf
service PostReplicaService {
  rpc newPost(NewPostRequest) returns (NewPostReply);
  rpc getLastTxn(GetLastTxnRequest) returns (GetLastTxnReply);
  rpc deleteAfter(DeleteAfterRequest) returns (DeleteAfterReply);
  rpc commitUpTo(CommitUpToRequest) returns (CommitUpToReply);
}
```

### Replication Flow

1. **Client sends POST** to any server
2. **Non-leader redirects** with HTTP 308 to leader
3. **Leader receives POST**:
    - Assigns transaction ID
    - Saves locally (uncommitted)
    - Replicates via gRPC to all replicas
    - Marks as committed on success
    - Returns response to client
4. **Replicas receive gRPC**:
    - Validate leader zxid
    - Check for missing transactions
    - Save post
    - Return success/failure

### Startup Synchronization

When a new server joins or rejoins:

1. Server registers in `/peers`
2. Leader detects new peer
3. Leader calls `getLastTxn()` on peer
4. Leader sends missing transactions via `newPost()`
5. Leader adds peer to `/replicas`
6. Peer can now serve read requests

## Monitoring

### Check ZooKeeper State

```bash
# Connect to ZooKeeper CLI
zkCli.sh

# View peers
ls /peers
get /peers/server1

# View leader
get /leader

# View replicas
get /replicas
```

### Check Server Logs

Look for key log messages:

```
INFO  Successfully became leader! (zxid=123456)
INFO  Syncing peer server2 at localhost:8081
INFO  Successfully synced peer server2
INFO  Added server2 to replicas. Current replicas: [server1, server2]
INFO  Replicating post txn=5 to 2 replicas
```

## Troubleshooting

### Problem: gRPC code not generated

```bash
# Manually trigger protobuf compilation
mvn protobuf:compile
mvn protobuf:compile-custom
```

### Problem: ZooKeeper connection refused

```bash
# Check if ZooKeeper is running
zkServer status

# Check if port 2181 is listening
lsof -i :2181
```

### Problem: Port already in use

```bash
# Find process using port 8080
lsof -i :8080

# Kill the process or change ports in configuration
```

### Problem: Database locked

```bash
# Clear H2 database files
rm /tmp/hellohello-server*

# Or configure different database paths
```

### Problem: No leader elected

Check that at least one server is in `/replicas`:
- First server to start joins `/replicas` automatically
- Or manually add server to `/replicas` via ZooKeeper CLI

## Development

### Project Structure

```
src/main/java/edu/sjsu/cmpe172/hellohello/
├── HelloHelloApplication.java       # Main application entry
├── HelloController.java             # HTTP REST endpoints
├── PostItem.java                    # Post entity
├── PostRepository.java              # JPA repository
├── TokenService.java                # Token generation/validation
├── ZooKeeperService.java           # ZooKeeper coordination
├── GrpcServerService.java          # gRPC server setup
├── PostReplicaServiceImpl.java     # gRPC service implementation
├── ReplicationService.java         # Leader replication logic
├── LeaderStatus.java               # Enum: WATCHING/WAITING/LEADING
└── ZooKeeperStatus.java            # Enum: CONNECTED/DISCONNECTED
```

### Running Tests

```bash
# Run all tests
mvn test

# Run with coverage
mvn test jacoco:report
```

### Building JAR

```bash
# Build executable JAR
mvn clean package

# Run JAR
java -jar target/hellohello-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=server1
```

## License

This project is for educational purposes (CMPE 172 course assignment).

## Contributors

- Your Name
- Course: CMPE 172
- Semester: Spring 2025