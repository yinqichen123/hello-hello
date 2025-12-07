package edu.sjsu.cmpe172.hellohello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;

@RestController
public class HelloController {

    @Autowired
    private PostRepository postRepository;

    @Autowired
    private TokenService tokenService;

    @Autowired
    private ZooKeeperService zooKeeperService;

    @Autowired
    private ReplicationService replicationService;

    @Value("${superPass}")
    private String superPass;

    private long nextTxn = 1;

    @GetMapping("/getToken")
    public ResponseEntity<String> getToken(@RequestParam String superPass, @RequestParam String user) {
        if (!this.superPass.equals(superPass)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid superPass");
        }
        String token = tokenService.generateToken(user);
        return ResponseEntity.ok(token);
    }

    @GetMapping("/posts")
    public ResponseEntity<?> getPosts(Pageable pageable) {
        // Check if this server is in replicas list
        if (!zooKeeperService.isInReplicas()) {
            // Redirect to leader with 308
            String leaderAddress = zooKeeperService.getLeaderAddress();
            if (leaderAddress != null) {
                String redirectUrl = "http://" + leaderAddress + "/posts";

                HttpHeaders headers = new HttpHeaders();
                headers.setLocation(URI.create(redirectUrl));

                return new ResponseEntity<>(headers, HttpStatus.PERMANENT_REDIRECT);
            }
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("Server not ready and no leader available");
        }

        // Return only committed posts
        Page<PostItem> posts = postRepository.findAllCommitted(pageable);
        return ResponseEntity.ok(posts);
    }

    @PostMapping("/posts")
    public ResponseEntity<?> createPost(@RequestBody PostRequest postRequest) {
        if (postRequest.getAuthor() == null || postRequest.getMessage() == null || postRequest.getToken() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Missing required fields");
        }

        if (!tokenService.validateToken(postRequest.getAuthor(), postRequest.getToken())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }

        // Check if this server is the leader
        if (!zooKeeperService.isLeader()) {
            // Redirect to leader with 308
            String leaderAddress = zooKeeperService.getLeaderAddress();
            if (leaderAddress != null) {
                String redirectUrl = "http://" + leaderAddress + "/posts";

                HttpHeaders headers = new HttpHeaders();
                headers.setLocation(URI.create(redirectUrl));

                return new ResponseEntity<>(headers, HttpStatus.PERMANENT_REDIRECT);
            }
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("No leader available");
        }

        // I am the leader, create and replicate the post
        synchronized (this) {
            // Get next transaction ID
            Long maxTxn = postRepository.findMaxTxn().orElse(0L);
            long txn = Math.max(maxTxn + 1, nextTxn);
            nextTxn = txn + 1;

            // Create post
            PostItem post = new PostItem(
                    txn,
                    postRequest.getAuthor(),
                    postRequest.getMessage(),
                    System.currentTimeMillis()
            );

            // Save locally first (uncommitted)
            postRepository.save(post);

            // Replicate to all replicas
            boolean success = replicationService.replicatePost(post);

            if (success) {
                // Mark as committed
                post.setCommitted(true);
                postRepository.save(post);

                return ResponseEntity.ok(post);
            } else {
                // Some replicas failed, but we already removed them
                // Still commit the post
                post.setCommitted(true);
                postRepository.save(post);

                return ResponseEntity.ok(post);
            }
        }
    }

    // ========== Management Endpoints for Testing ==========

    @GetMapping("/leader")
    public ResponseEntity<?> getLeaderStatus() {
        java.util.Map<String, Object> status = new java.util.LinkedHashMap<>();
        status.put("status", zooKeeperService.getLeaderStatus().toString());
        status.put("zookeeper", zooKeeperService.getZkStatus().toString());
        status.put("leader", zooKeeperService.getCurrentLeader());
        status.put("myid", zooKeeperService.getServerId());
        status.put("description", zooKeeperService.getServerDescription());
        status.put("peers", new java.util.ArrayList<>(zooKeeperService.getAllPeers().keySet()));
        return ResponseEntity.ok(status);
    }

    @PostMapping("/leader/lead")
    public ResponseEntity<?> startLeading() {
        zooKeeperService.startLeading();
        return ResponseEntity.ok("Started attempting to lead");
    }

    @PostMapping("/leader/watch")
    public ResponseEntity<?> stopLeading() {
        zooKeeperService.stopLeading();
        return ResponseEntity.ok("Stopped leading, now watching");
    }

    @PostMapping("/leader/stop")
    public ResponseEntity<?> stopLeadingAlias() {
        zooKeeperService.stopLeading();
        return ResponseEntity.ok("Stopped leading");
    }

    @PostMapping("/replicas/add")
    public ResponseEntity<?> addToReplicas(@RequestParam String serverId) {
        if (!zooKeeperService.isLeader()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("Only leader can add to replicas");
        }
        zooKeeperService.addToReplicas(serverId);
        return ResponseEntity.ok("Added " + serverId + " to replicas");
    }

    @PostMapping("/replicas/remove")
    public ResponseEntity<?> removeFromReplicas(@RequestParam String serverId) {
        if (!zooKeeperService.isLeader()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("Only leader can remove from replicas");
        }
        zooKeeperService.removeFromReplicas(serverId);
        return ResponseEntity.ok("Removed " + serverId + " from replicas");
    }

    @PostMapping("/replicas/join")
    public ResponseEntity<?> joinReplicas() {
        // Manual initialization: allow a server to add itself to replicas
        // This can be used when replicas is empty or when the server is not yet in the list
        List<String> currentReplicas = zooKeeperService.getReplicas();
        String myId = zooKeeperService.getServerId();
        
        if (currentReplicas.contains(myId)) {
            return ResponseEntity.ok("Already in replicas list");
        }
        
        // If replicas is empty, this is the first server - allow it to join
        if (currentReplicas.isEmpty()) {
            zooKeeperService.addToReplicas(myId);
            return ResponseEntity.ok("Initialized: added self to replicas (first server)");
        }
        
        // If there's already a leader, only the leader can add servers
        if (zooKeeperService.isLeader()) {
            zooKeeperService.addToReplicas(myId);
            return ResponseEntity.ok("Added self to replicas (as leader)");
        }
        
        // If there's no leader but replicas is not empty, allow joining
        if (zooKeeperService.getCurrentLeader() == null || zooKeeperService.getCurrentLeader().isEmpty()) {
            zooKeeperService.addToReplicas(myId);
            return ResponseEntity.ok("Added self to replicas (no leader exists)");
        }
        
        // Otherwise, need leader to add
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("Cannot join: replicas list is not empty and there is a leader. " +
                      "Only leader can add servers. Current leader: " + zooKeeperService.getCurrentLeader());
    }
}