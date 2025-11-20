package edu.sjsu.cmpe172.hellohello;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface PostRepository extends JpaRepository<PostItem, Long> {

    // Find the maximum transaction ID
    @Query("SELECT MAX(p.txn) FROM PostItem p")
    Optional<Long> findMaxTxn();

    // Find posts by transaction ID
    Optional<PostItem> findByTxn(Long txn);

    // Delete all posts after a certain transaction ID
    @Modifying
    @Transactional
    @Query("DELETE FROM PostItem p WHERE p.txn > ?1")
    void deleteByTxnGreaterThan(Long txn);

    // Commit posts up to a certain transaction ID
    @Modifying
    @Transactional
    @Query("UPDATE PostItem p SET p.committed = true WHERE p.txn <= ?1")
    void commitUpToTxn(Long txn);

    // Get only committed posts for client reads
    @Query("SELECT p FROM PostItem p WHERE p.committed = true")
    Page<PostItem> findAllCommitted(Pageable pageable);

    // Count uncommitted posts
    @Query("SELECT COUNT(p) FROM PostItem p WHERE p.committed = false")
    long countUncommitted();
}