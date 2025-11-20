package edu.sjsu.cmpe172.hellohello;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class GrpcServerService {

    private static final Logger logger = LoggerFactory.getLogger(GrpcServerService.class);

    @Value("${grpc.port:9090}")
    private int grpcPort;

    @Autowired
    private PostReplicaServiceImpl postReplicaService;

    private Server server;

    @PostConstruct
    public void start() throws IOException {
        server = ServerBuilder
                .forPort(grpcPort)
                .addService(postReplicaService)
                .build()
                .start();

        logger.info("gRPC Server started on port {}", grpcPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gRPC server");
            GrpcServerService.this.stop();
        }));
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdown();
            logger.info("gRPC Server stopped");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}