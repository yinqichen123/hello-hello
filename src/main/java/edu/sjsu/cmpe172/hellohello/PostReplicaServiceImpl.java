package edu.sjsu.cmpe172.hellohello;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class PostReplicaServiceImpl extends PostReplicaServiceGrpc.PostReplicaServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(PostReplicaServiceImpl.class);

    @Autowired
    private PostRepository postRepository;

    @Autowired
    private ZooKeeperService zooKeeperService;

    @Override
    public void newPost(NewPostRequest request, StreamObserver<NewPostReply> responseObserver) {
        logger.info("Received newPost request: txn={}, author={}, leaderZxid={}",
                request.getTxn(), request.getAuthor(), request.getLeaderZxid());

        try {
            // Check leader zxid
            long myLeaderZxid = zooKeeperService.getLeaderZxid();

            if (request.getLeaderZxid() < myLeaderZxid) {
                // Old leader
                logger.warn("Rejecting request from old leader: {} < {}",
                        request.getLeaderZxid(), myLeaderZxid);
                NewPostReply reply = NewPostReply.newBuilder()
                        .setStatus(AddPostStatus.ADD_NOT_MY_LEADER)
                        .setLastVersion(getLastTxn())
                        .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }

            if (request.getLeaderZxid() > myLeaderZxid) {
                // New leader, sync
                logger.info("Syncing leader zxid from {} to {}", myLeaderZxid, request.getLeaderZxid());
                zooKeeperService.syncLeaderZxid();
            }

            // Check for missing transactions
            long lastTxn = getLastTxn();
            if (request.getTxn() > lastTxn + 1) {
                logger.warn("Missing transaction: expected {}, got {}", lastTxn + 1, request.getTxn());
                NewPostReply reply = NewPostReply.newBuilder()
                        .setStatus(AddPostStatus.ADD_MISSING_TXN)
                        .setLastVersion(lastTxn)
                        .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }

            // Add the post
            PostItem post = new PostItem(
                    request.getTxn(),
                    request.getAuthor(),
                    request.getMessage(),
                    request.getTimestamp()
            );

            // If this transaction has already been committed, set it as committed
            if (request.getTxn() <= request.getLastCommittedTxn()) {
                post.setCommitted(true);
            }

            postRepository.save(post);
            logger.info("Successfully added post with txn={}", request.getTxn());

            NewPostReply reply = NewPostReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_SUCCESS)
                    .setLastVersion(request.getTxn())
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error processing newPost", e);
            NewPostReply reply = NewPostReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_FAILED)
                    .setLastVersion(getLastTxn())
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getLastTxn(GetLastTxnRequest request, StreamObserver<GetLastTxnReply> responseObserver) {
        long lastTxn = getLastTxn();
        logger.info("getLastTxn: returning {}", lastTxn);

        GetLastTxnReply reply = GetLastTxnReply.newBuilder()
                .setLastTxn(lastTxn)
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAfter(DeleteAfterRequest request, StreamObserver<DeleteAfterReply> responseObserver) {
        logger.info("deleteAfter: txn={}, leaderZxid={}", request.getTxn(), request.getLeaderZxid());

        try {
            // Check leader zxid
            long myLeaderZxid = zooKeeperService.getLeaderZxid();

            if (request.getLeaderZxid() < myLeaderZxid) {
                DeleteAfterReply reply = DeleteAfterReply.newBuilder()
                        .setStatus(AddPostStatus.ADD_NOT_MY_LEADER)
                        .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }

            if (request.getLeaderZxid() > myLeaderZxid) {
                zooKeeperService.syncLeaderZxid();
            }

            postRepository.deleteByTxnGreaterThan(request.getTxn());
            logger.info("Deleted posts after txn={}", request.getTxn());

            DeleteAfterReply reply = DeleteAfterReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_SUCCESS)
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error in deleteAfter", e);
            DeleteAfterReply reply = DeleteAfterReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_FAILED)
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void commitUpTo(CommitUpToRequest request, StreamObserver<CommitUpToReply> responseObserver) {
        logger.info("commitUpTo: txn={}, leaderZxid={}", request.getTxn(), request.getLeaderZxid());

        try {
            // Check leader zxid
            long myLeaderZxid = zooKeeperService.getLeaderZxid();

            if (request.getLeaderZxid() < myLeaderZxid) {
                CommitUpToReply reply = CommitUpToReply.newBuilder()
                        .setStatus(AddPostStatus.ADD_NOT_MY_LEADER)
                        .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }

            if (request.getLeaderZxid() > myLeaderZxid) {
                zooKeeperService.syncLeaderZxid();
            }

            postRepository.commitUpToTxn(request.getTxn());
            logger.info("Committed posts up to txn={}", request.getTxn());

            CommitUpToReply reply = CommitUpToReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_SUCCESS)
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error in commitUpTo", e);
            CommitUpToReply reply = CommitUpToReply.newBuilder()
                    .setStatus(AddPostStatus.ADD_FAILED)
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    private long getLastTxn() {
        return postRepository.findMaxTxn().orElse(0L);
    }
}