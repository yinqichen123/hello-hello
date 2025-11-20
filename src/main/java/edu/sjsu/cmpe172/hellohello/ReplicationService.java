package edu.sjsu.cmpe172.hellohello;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class ReplicationService {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationService.class);
    private static final int GRPC_TIMEOUT_SECONDS = 5;

    @Autowired
    private ZooKeeperService zooKeeperService;

    @Autowired
    private PostRepository postRepository;

    // Cache of gRPC channels to replicas
    private Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    /**
     * Replicate a post to all replicas
     * Returns true if successfully replicated to all, false otherwise
     */
    public boolean replicatePost(PostItem post) {
        if (!zooKeeperService.isLeader()) {
            logger.error("Cannot replicate: not the leader");
            return false;
        }

        List<String> replicas = zooKeeperService.getReplicas();
        String myServerId = zooKeeperService.getServerId();
        long leaderZxid = zooKeeperService.getLeaderZxid();
        long lastCommittedTxn = getLastCommittedTxn();

        logger.info("Replicating post txn={} to {} replicas", post.getTxn(), replicas.size());

        Set<String> failedReplicas = new HashSet<>();

        for (String serverId : replicas) {
            if (serverId.equals(myServerId)) {
                continue;  // Skip self
            }

            try {
                String peerAddress = zooKeeperService.getPeerAddress(serverId);
                if (peerAddress == null) {
                    logger.warn("No address found for replica {}", serverId);
                    failedReplicas.add(serverId);
                    continue;
                }

                PostReplicaServiceGrpc.PostReplicaServiceBlockingStub stub = getStub(peerAddress);

                NewPostRequest request = NewPostRequest.newBuilder()
                        .setMessage(post.getMessage())
                        .setAuthor(post.getAuthor())
                        .setTimestamp(post.getTimestamp())
                        .setTxn(post.getTxn())
                        .setLeaderZxid(leaderZxid)
                        .setLastCommittedTxn(lastCommittedTxn)
                        .build();

                NewPostReply reply = stub.withDeadlineAfter(GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .newPost(request);

                if (reply.getStatus() != AddPostStatus.ADD_SUCCESS) {
                    logger.warn("Replica {} rejected post: {}", serverId, reply.getStatus());
                    failedReplicas.add(serverId);
                }

            } catch (Exception e) {
                logger.error("Failed to replicate to {}: {}", serverId, e.getMessage());
                failedReplicas.add(serverId);
            }
        }

        // Remove failed replicas
        for (String failedId : failedReplicas) {
            zooKeeperService.removeFromReplicas(failedId);
        }

        return failedReplicas.isEmpty();
    }

    /**
     * Sync a new peer with the leader's data
     */
    public boolean syncPeer(String serverId, String peerAddress) {
        logger.info("Syncing peer {} at {}", serverId, peerAddress);

        try {
            PostReplicaServiceGrpc.PostReplicaServiceBlockingStub stub = getStub(peerAddress);

            // Get peer's last transaction
            GetLastTxnReply lastTxnReply = stub.withDeadlineAfter(GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .getLastTxn(GetLastTxnRequest.newBuilder().build());

            long peerLastTxn = lastTxnReply.getLastTxn();
            long myLastTxn = postRepository.findMaxTxn().orElse(0L);

            logger.info("Peer {} has txn={}, I have txn={}", serverId, peerLastTxn, myLastTxn);

            long leaderZxid = zooKeeperService.getLeaderZxid();

            // If peer has extra transactions, delete them
            if (peerLastTxn > myLastTxn) {
                logger.info("Peer has extra transactions, deleting after {}", myLastTxn);
                DeleteAfterRequest deleteRequest = DeleteAfterRequest.newBuilder()
                        .setTxn(myLastTxn)
                        .setLeaderZxid(leaderZxid)
                        .build();

                stub.withDeadlineAfter(GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .deleteAfter(deleteRequest);
            }

            // Send missing transactions
            if (peerLastTxn < myLastTxn) {
                logger.info("Sending {} missing transactions to peer", myLastTxn - peerLastTxn);
                long lastCommittedTxn = getLastCommittedTxn();

                for (long txn = peerLastTxn + 1; txn <= myLastTxn; txn++) {
                    Optional<PostItem> postOpt = postRepository.findByTxn(txn);
                    if (postOpt.isEmpty()) {
                        logger.error("Missing transaction {} in my database!", txn);
                        return false;
                    }

                    PostItem post = postOpt.get();
                    NewPostRequest request = NewPostRequest.newBuilder()
                            .setMessage(post.getMessage())
                            .setAuthor(post.getAuthor())
                            .setTimestamp(post.getTimestamp())
                            .setTxn(post.getTxn())
                            .setLeaderZxid(leaderZxid)
                            .setLastCommittedTxn(lastCommittedTxn)
                            .build();

                    NewPostReply reply = stub.withDeadlineAfter(GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .newPost(request);

                    if (reply.getStatus() != AddPostStatus.ADD_SUCCESS) {
                        logger.error("Failed to sync txn {} to peer: {}", txn, reply.getStatus());
                        return false;
                    }
                }
            }

            logger.info("Successfully synced peer {}", serverId);
            return true;

        } catch (Exception e) {
            logger.error("Error syncing peer {}: {}", serverId, e.getMessage());
            return false;
        }
    }

    /**
     * Get or create a gRPC stub for a peer address
     */
    private PostReplicaServiceGrpc.PostReplicaServiceBlockingStub getStub(String address) {
        ManagedChannel channel = channels.computeIfAbsent(address, addr -> {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) + 1000 : 9090;  // gRPC port = HTTP port + 1000

            return ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();
        });

        return PostReplicaServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Get the last committed transaction ID
     */
    private long getLastCommittedTxn() {
        // For simplicity, we commit transactions immediately after successful replication
        return postRepository.findMaxTxn().orElse(0L);
    }

    /**
     * Cleanup channels
     */
    public void shutdown() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
        }
        channels.clear();
    }
}