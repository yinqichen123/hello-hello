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
}