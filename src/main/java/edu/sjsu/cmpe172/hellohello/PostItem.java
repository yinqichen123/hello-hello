package edu.sjsu.cmpe172.hellohello;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class PostItem {
    @Id
    @JsonProperty("id")
    private Long id;  // This will be the transaction ID

    @JsonProperty("author")
    private String author;

    @JsonProperty("message")
    private String message;

    // New fields for replication
    private Long txn;  // Transaction ID
    private boolean committed;  // Whether this post is committed
    private Long timestamp;  // Original timestamp from client

    public PostItem() {
    }

    public PostItem(Long txn, String author, String message, Long timestamp) {
        this.id = txn;
        this.txn = txn;
        this.author = author;
        this.message = message;
        this.timestamp = timestamp;
        this.committed = false;  // Start as uncommitted
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
        this.txn = id;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTxn() {
        return txn;
    }

    public void setTxn(Long txn) {
        this.txn = txn;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}