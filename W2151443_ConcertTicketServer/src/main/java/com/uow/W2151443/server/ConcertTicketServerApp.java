package com.uow.W2151443.server;

import com.uow.W2151443.concert.service.ShowInfo;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient; // Import your etcd client
import com.uow.W2151443.server.service.W2151443ReservationServiceImpl;
import com.uow.W2151443.server.service.W2151443ShowManagementServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcertTicketServerApp {
    private static final Logger logger = Logger.getLogger(ConcertTicketServerApp.class.getName());
    private Server grpcServer; // Renamed from server to grpcServer for clarity
    private final int grpcPort; // Renamed from port

    private final Map<String, ShowInfo> W2151443_showsDataStore = new ConcurrentHashMap<>();

    // etcd related fields
    private W2151443EtcdNameServiceClient etcdClient;
    private final String etcdUrl = "http://localhost:2381"; // Make this configurable later
    private final String serviceBasePath = "/services/W2151443ConcertTicketService";
    private String serviceInstanceId;
    private final int serviceRegistryTTL = 30; // seconds, etcd client heartbeats every 5s by default

    public ConcertTicketServerApp(int grpcPort) {
        this.grpcPort = grpcPort;
        // Generate a unique ID for this server instance
        this.serviceInstanceId = "node-W2151443-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String getHostIpAddress() {
        try {
            // Attempt to get a non-loopback address if possible
            // This is often tricky and might need manual configuration in real deployments
            // For local testing, 127.0.0.1 is fine.
            // return InetAddress.getLocalHost().getHostAddress(); // Might return 127.0.0.1
            return "127.0.0.1"; // Hardcode for local testing ease
        } catch (/*UnknownHostException e*/ Exception e) {
            logger.log(Level.WARNING, "Could not determine host IP address, defaulting to 127.0.0.1", e);
            return "127.0.0.1";
        }
    }

    public void start() throws IOException {
        W2151443ShowManagementServiceImpl showManagementService = new W2151443ShowManagementServiceImpl(W2151443_showsDataStore);
        W2151443ReservationServiceImpl reservationService = new W2151443ReservationServiceImpl(W2151443_showsDataStore);

        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(showManagementService)
                .addService(reservationService)
                .build()
                .start();
        logger.info("W2151443 gRPC Server started, listening on port: " + grpcPort);

        // Initialize and register with etcd
        etcdClient = new W2151443EtcdNameServiceClient(etcdUrl);
        String hostIp = getHostIpAddress();
        boolean registered = etcdClient.registerService(serviceBasePath, serviceInstanceId, hostIp, grpcPort, serviceRegistryTTL);
        if (registered) {
            logger.info("Service instance " + serviceInstanceId + " registered successfully with etcd at " + hostIp + ":" + grpcPort);
        } else {
            logger.severe("Failed to register service instance " + serviceInstanceId + " with etcd. The server will run without discovery.");
            // Consider how to handle this failure in a real system (e.g., retry, shutdown)
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** W2151443 Server: Shutting down gRPC server and etcd registration...");
            try {
                ConcertTicketServerApp.this.stopEtcdRegistration(); // Deregister first
                ConcertTicketServerApp.this.stopGrpcServer();     // Then stop gRPC
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** W2151443 Server: Server shut down complete.");
        }));
    }

    public void stopGrpcServer() throws InterruptedException {
        if (grpcServer != null) {
            logger.info("W2151443 Server: Attempting graceful shutdown of gRPC server...");
            grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            logger.info("W2151443 Server: gRPC server shut down.");
        }
    }

    public void stopEtcdRegistration() {
        if (etcdClient != null) {
            logger.info("W2151443 Server: Attempting to deregister " + serviceInstanceId + " from etcd...");
            etcdClient.deregisterService(serviceBasePath, serviceInstanceId);
            etcdClient.shutdownHeartbeat(); // Shutdown the heartbeat scheduler in the etcd client
            logger.info("W2151443 Server: Etcd client resources released.");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int serverPort = 9090;
        if (args.length > 0) {
            try {
                serverPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number provided: " + args[0] + ". Using default port " + serverPort);
            }
        }

        final ConcertTicketServerApp serverApp = new ConcertTicketServerApp(serverPort);
        serverApp.start();
        serverApp.blockUntilShutdown();
    }
}
