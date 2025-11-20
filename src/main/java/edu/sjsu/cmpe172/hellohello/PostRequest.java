package edu.sjsu.cmpe172.hellohello;

public class PostRequest {
    private String author;
    private String message;
    private String token;

    public PostRequest() {}

    public PostRequest(String author, String message, String token) {
        this.author = author;
        this.message = message;
        this.token = token;
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

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}