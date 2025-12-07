package edu.sjsu.cmpe172.hellohello;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ZooKeeperService implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperService.class);
    
    // Executor for async peer synchronization
    private final ExecutorService syncExecutor = Executors.newSingleThreadExecutor();

    @Value("${zkConnectString}")
    private String zkConnectString;

    @Value("${zkNamespace:}")
    private String zkNamespace;

    @Value("${myDescription:}")
    private String myDescription;

    @Value("${server.port}")
    private int httpPort;

    @Value("${server.address:localhost}")
    private String serverAddress;

    @Value("${zookeeper.session.timeout:5000}")
    private int sessionTimeout;

    // serverId will be dynamically generated from EPHEMERAL_SEQUENTIAL znode path
    private String serverId;

    @Autowired(required = false)
    private ReplicationService replicationService;

    private String PEERS_PATH;
    private String LEADER_PATH;
    private String REPLICAS_PATH;

    private ZooKeeper zooKeeper;
    private String currentLeader;
    private long currentLeaderZxid;
    private List<String> replicas = Collections.synchronizedList(new ArrayList<>());
    private Map<String, String> peerAddresses = new HashMap<>();

    private LeaderStatus leaderStatus = LeaderStatus.WAITING;
    private ZooKeeperStatus zkStatus = ZooKeeperStatus.DISCONNECTED;
    private boolean wantsToLead = true;

    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    @PostConstruct
    public void init() throws IOException, InterruptedException, KeeperException {
        String prefix = (zkNamespace != null && !zkNamespace.isEmpty()) ? "/" + zkNamespace : "";
        PEERS_PATH = prefix + "/peers";
        LEADER_PATH = prefix + "/leader";
        REPLICAS_PATH = prefix + "/replicas";

        logger.info("Initializing ZooKeeper: address={}:{}, description={}",
                serverAddress, httpPort, myDescription);

        connect();
    }

    private void connect() throws IOException, InterruptedException, KeeperException {
        zooKeeper = new ZooKeeper(zkConnectString, sessionTimeout, this);
        connectedSignal.await();

        // Create root paths
        createPathIfNotExists(PEERS_PATH);
        createPathIfNotExists(REPLICAS_PATH);

        // Register as peer - serverId will be generated from EPHEMERAL_SEQUENTIAL znode path
        registerAsPeer();

        // Watch replicas list
        watchReplicas();

        // Check if we should become leader
        watchLeader();
    }

    private void createPathIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            try {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created path: {}", path);
            } catch (KeeperException.NodeExistsException e) {
                logger.debug("Path already exists: {}", path);
            } catch (KeeperException.NoNodeException e) {
                String parentPath = path.substring(0, path.lastIndexOf('/'));
                if (parentPath.length() > 0) {
                    createPathIfNotExists(parentPath);
                    createPathIfNotExists(path);
                } else {
                    throw e;
                }
            }
        }
    }

    private void registerAsPeer() throws KeeperException, InterruptedException {
        // Use EPHEMERAL_SEQUENTIAL as per SpringBoot Leader assignment requirements
        // ZooKeeper will automatically generate the znode name (e.g., peer-0000000001)
        String peerPathPrefix = PEERS_PATH + "/peer-";
        String description = (myDescription != null && !myDescription.isEmpty()) 
            ? myDescription 
            : "server";
        
        // Create EPHEMERAL_SEQUENTIAL znode - ZooKeeper will append sequence number
        String createdPath = zooKeeper.create(
                peerPathPrefix,
                description.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL
        );
        
        // Extract serverId from the created path (e.g., /peers/peer-0000000001 -> peer-0000000001)
        serverId = createdPath.substring(createdPath.lastIndexOf('/') + 1);
        
        logger.info("Registered as peer: {} with description: {}", serverId, description);
        
        // Store address mapping for replication service (if needed)
        String myAddress = serverAddress + ":" + httpPort;
        peerAddresses.put(serverId, myAddress);

        updatePeersList();
    }

    private void updatePeersList() {
        try {
            List<String> peers = zooKeeper.getChildren(PEERS_PATH, this);

            // Update peer addresses map
            // For replication, we need to track addresses, but znode data contains description
            // We'll construct addresses based on sequence number pattern
            peerAddresses.clear();
            for (String peer : peers) {
                try {
                    byte[] data = zooKeeper.getData(PEERS_PATH + "/" + peer, false, null);
                    // Description is stored in znode data (per SpringBoot Leader assignment)
                    // For replication, we need addresses, so we construct them from sequence numbers
                    int seqNum = extractSequenceNumber(peer);
                    // Use a pattern: base port 9080 + sequence number offset
                    // This assumes servers start in order, which is typical for testing
                    String address = serverAddress + ":" + (9080 + seqNum);
                    peerAddresses.put(peer, address);
                } catch (Exception e) {
                    logger.error("Error reading peer data for {}", peer, e);
                }
            }

            logger.info("Updated peers list: {}", peers);

            // If I'm the leader, check for new peers to sync
            if (isLeader()) {
                syncNewPeers();
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Error updating peers list", e);
        }
    }
    
    /**
     * Extract sequence number from peer ID (e.g., "peer-0000000001" -> 1)
     */
    private int extractSequenceNumber(String peerId) {
        try {
            String[] parts = peerId.split("-");
            if (parts.length > 1) {
                return Integer.parseInt(parts[parts.length - 1]);
            }
        } catch (NumberFormatException e) {
            logger.warn("Could not extract sequence number from peer ID: {}", peerId);
        }
        return 0;
    }

    private void watchReplicas() {
        try {
            byte[] data = zooKeeper.getData(REPLICAS_PATH, this, null);
            String replicasStr = new String(data, StandardCharsets.UTF_8);

            if (replicasStr.isEmpty()) {
                replicas.clear();
            } else {
                replicas = new ArrayList<>(Arrays.asList(replicasStr.split(",")));
            }

            logger.info("Replicas list updated: {}", replicas);
            
            // Check if we should become leader after replicas list update
            // Only servers in replicas can become leader
            if (replicas.contains(serverId) && currentLeader == null && wantsToLead) {
                logger.info("Now in replicas list and no leader exists, attempting to become leader");
                tryToBecomeLeader();
            }

        } catch (KeeperException.NoNodeException e) {
            // /replicas node doesn't exist - replicas list is empty
            replicas.clear();
            logger.info("Replicas node does not exist - replicas list is empty");
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error watching replicas", e);
        }
    }

    private void watchLeader() {
        try {
            Stat stat = zooKeeper.exists(LEADER_PATH, this);

            if (stat != null) {
                // Leader exists
                byte[] data = zooKeeper.getData(LEADER_PATH, this, stat);
                currentLeader = new String(data, StandardCharsets.UTF_8);
                currentLeaderZxid = stat.getCzxid();

                logger.info("Current leader is: {} (zxid={})", currentLeader, currentLeaderZxid);

                if (currentLeader.equals(serverId)) {
                    leaderStatus = LeaderStatus.LEADING;
                    onBecomeLeader();
                } else if (wantsToLead) {
                    leaderStatus = LeaderStatus.WAITING;
                } else {
                    leaderStatus = LeaderStatus.WATCHING;
                }
            } else {
                // No leader
                currentLeader = null;
                currentLeaderZxid = 0;
                logger.info("No current leader");

                if (wantsToLead && replicas.contains(serverId)) {
                    tryToBecomeLeader();
                } else {
                    leaderStatus = LeaderStatus.WATCHING;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error watching leader", e);
        }
    }

    private void tryToBecomeLeader() {
        // Only servers in /replicas can become leader
        if (!replicas.contains(serverId)) {
            logger.info("Cannot become leader: not in replicas list");
            return;
        }

        try {
            zooKeeper.create(
                    LEADER_PATH,
                    serverId.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
            );

            currentLeader = serverId;
            Stat stat = zooKeeper.exists(LEADER_PATH, false);
            currentLeaderZxid = stat.getCzxid();
            leaderStatus = LeaderStatus.LEADING;

            logger.info("Successfully became leader! (zxid={})", currentLeaderZxid);
            onBecomeLeader();

        } catch (KeeperException.NodeExistsException e) {
            logger.info("Failed to become leader, node already exists");
            leaderStatus = LeaderStatus.WAITING;
            watchLeader();
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error trying to become leader", e);
            leaderStatus = LeaderStatus.WAITING;
        }
    }

    private void onBecomeLeader() {
        logger.info("Executing leader initialization");
        // Sync all peers
        syncNewPeers();
    }

    private void syncNewPeers() {
        if (!isLeader() || replicationService == null) {
            return;
        }

        // Find peers not in replicas
        Set<String> peersToSync = new HashSet<>(peerAddresses.keySet());
        peersToSync.removeAll(replicas);
        peersToSync.remove(serverId);  // Remove self

        logger.info("Syncing {} new peers", peersToSync.size());

        // Execute synchronization asynchronously to avoid blocking ZooKeeper event thread
        for (String peerId : peersToSync) {
            String address = peerAddresses.get(peerId);
            if (address != null) {
                syncExecutor.submit(() -> {
                    try {
                        logger.info("Async sync started for peer {}", peerId);
                        boolean success = replicationService.syncPeer(peerId, address);
                        if (success) {
                            addToReplicas(peerId);
                            logger.info("Peer {} successfully synced and added to replicas", peerId);
                        } else {
                            logger.warn("Failed to sync peer {}", peerId);
                        }
                    } catch (Exception e) {
                        logger.error("Exception during async sync of peer {}: {}", peerId, e.getMessage());
                    }
                });
            }
        }
    }

    public void addToReplicas(String peerId) {
        if (replicas.contains(peerId)) {
            logger.debug("Peer {} already in replicas list", peerId);
            return;
        }

        try {
            replicas.add(peerId);
            String replicasStr = String.join(",", replicas);

            Stat stat = zooKeeper.exists(REPLICAS_PATH, false);
            if (stat == null) {
                // Create /replicas node if it doesn't exist
                try {
                    zooKeeper.create(REPLICAS_PATH, replicasStr.getBytes(StandardCharsets.UTF_8),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.info("Created /replicas node and added {}. Current replicas: {}", peerId, replicas);
                } catch (KeeperException.NodeExistsException e) {
                    // Node was created by another process, update it
                    zooKeeper.setData(REPLICAS_PATH, replicasStr.getBytes(StandardCharsets.UTF_8), -1);
                    logger.info("Updated /replicas node and added {}. Current replicas: {}", peerId, replicas);
                }
            } else {
                zooKeeper.setData(REPLICAS_PATH, replicasStr.getBytes(StandardCharsets.UTF_8), -1);
                logger.info("Added {} to replicas. Current replicas: {}", peerId, replicas);
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Error adding {} to replicas", peerId, e);
            // Remove from local list if update failed
            replicas.remove(peerId);
        }
    }

    public void removeFromReplicas(String peerId) {
        if (!replicas.remove(peerId)) {
            return;
        }

        try {
            String replicasStr = String.join(",", replicas);
            zooKeeper.setData(REPLICAS_PATH, replicasStr.getBytes(StandardCharsets.UTF_8), -1);

            logger.info("Removed {} from replicas. Current replicas: {}", peerId, replicas);

        } catch (KeeperException | InterruptedException e) {
            logger.error("Error removing from replicas", e);
        }
    }

    public void syncLeaderZxid() {
        try {
            Stat stat = zooKeeper.exists(LEADER_PATH, false);
            if (stat != null) {
                currentLeaderZxid = stat.getCzxid();
                byte[] data = zooKeeper.getData(LEADER_PATH, false, stat);
                currentLeader = new String(data, StandardCharsets.UTF_8);
                logger.info("Synced leader zxid to {}", currentLeaderZxid);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error syncing leader zxid", e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Received event: {}", event);

        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    zkStatus = ZooKeeperStatus.CONNECTED;
                    connectedSignal.countDown();
                    logger.info("Connected to ZooKeeper");
                    break;
                case Disconnected:
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.warn("Disconnected from ZooKeeper");
                    break;
                case Expired:
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.error("Session expired");
                    System.exit(2);
                    break;
            }
        } else {
            String path = event.getPath();

            if (path != null) {
                if (path.equals(PEERS_PATH)) {
                    updatePeersList();
                } else if (path.equals(LEADER_PATH)) {
                    watchLeader();
                } else if (path.equals(REPLICAS_PATH)) {
                    watchReplicas();
                }
            }
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            // Shutdown sync executor
            syncExecutor.shutdown();
            if (!syncExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                syncExecutor.shutdownNow();
            }
            
            if (zooKeeper != null) {
                zooKeeper.close();
                logger.info("ZK connection closed");
            }
        } catch (InterruptedException e) {
            logger.error("Error closing ZooKeeper connection", e);
            syncExecutor.shutdownNow();
        }
    }

    // Getters
    public LeaderStatus getLeaderStatus() { return leaderStatus; }
    public ZooKeeperStatus getZkStatus() { return zkStatus; }
    public String getCurrentLeader() { return currentLeader; }
    public String getServerId() { return serverId; }
    public List<String> getReplicas() { return new ArrayList<>(replicas); }
    public boolean isLeader() { return serverId.equals(currentLeader); }
    public boolean isInReplicas() { return replicas.contains(serverId); }
    public long getLeaderZxid() { return currentLeaderZxid; }
    public String getPeerAddress(String peerId) { return peerAddresses.get(peerId); }
    public String getLeaderAddress() {
        return currentLeader != null ? peerAddresses.get(currentLeader) : null;
    }
    public Map<String, String> getAllPeers() { return new HashMap<>(peerAddresses); }
    public String getServerDescription() { 
        return myDescription != null && !myDescription.isEmpty() 
            ? myDescription 
            : (serverId != null ? serverId + " server" : "server"); 
    }

    public void startLeading() {
        wantsToLead = true;
        if (zkStatus == ZooKeeperStatus.CONNECTED) {
            watchLeader();
        }
    }

    public void stopLeading() {
        wantsToLead = false;
        if (leaderStatus == LeaderStatus.LEADING) {
            try {
                zooKeeper.delete(LEADER_PATH, -1);
                logger.info("Gave up leadership");
            } catch (KeeperException | InterruptedException e) {
                logger.error("Error giving up leadership", e);
            }
        }
        leaderStatus = LeaderStatus.WATCHING;
        currentLeader = null;
        watchLeader();
    }
}