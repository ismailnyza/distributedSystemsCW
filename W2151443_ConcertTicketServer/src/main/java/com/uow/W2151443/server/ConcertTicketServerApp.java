package com.uow.W2151443.server;

import com.uow.W2151443.concert.service.ShowInfo;
import com.uow.W2151443.coordination.W2151443LeaderElection; // Import LeaderElection class
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient;
import com.uow.W2151443.server.service.W2151443ReservationServiceImpl;
import com.uow.W2151443.server.service.W2151443ShowManagementServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
// import java.net.InetAddress;
// import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcertTicketServerApp implements W2151443LeaderElection.LeaderChangeListener { // Implement listener
    private static final Logger logger = Logger.getLogger(ConcertTicketServerApp.class.getName());
    private Server grpcServer;
    private final int grpcPort;
    private final String serverHostIp; // IP this server will advertise

    private final Map<String, ShowInfo> W2151443_showsDataStore = new ConcurrentHashMap<>();

    // etcd related fields
    private W2151443EtcdNameServiceClient etcdClient;
    private final String etcdUrl = "http://localhost:2381"; // Your etcd instance URL
    private final String etcdServiceBasePath = "/services/W2151443ConcertTicketService";
    private final String etcdLeaderPath = etcdServiceBasePath + "/_leader"; // Specific key for current leader
    public String etcdServiceInstanceId; // Unique ID for this server's general registration
    private final int serviceRegistryTTL = 30;

    // ZooKeeper related fields
    private W2151443LeaderElection leaderElection;
    private final String zooKeeperUrl = "localhost:2181"; // Your ZooKeeper instance URL
    private final String zkElectionBasePath = "/W2151443_concert_election";
    private volatile boolean isCurrentlyLeader = false; // Current leadership status

    public ConcertTicketServerApp(int grpcPort, String serverIdSuffix) { // Suffix for unique instance IDs if running multiple locally
        this.grpcPort = grpcPort;
        this.serverHostIp = getHostIpAddress(); // Get IP once
        this.etcdServiceInstanceId = "node-W2151443-" + serverIdSuffix + "-" + UUID.randomUUID().toString().substring(0, 4);
    }

    private String getHostIpAddress() {
        // For local testing, "127.0.0.1" is fine.
        // In a real multi-machine setup, this needs to be the actual routable IP.
        return "127.0.0.1";
    }

    public String getServerAddress() {
        return this.serverHostIp + ":" + this.grpcPort;
    }

    public boolean isLeader() {
        return isCurrentlyLeader;
    }

    public void start() throws Exception { // Changed to throws Exception for ZK
        // Initialize etcd client (used for general registration and by leader to publish its address)
        logger.info("Initializing etcd client for URL: " + etcdUrl);
        etcdClient = new W2151443EtcdNameServiceClient(etcdUrl);

        // 1. Start Leader Election Process with ZooKeeper
        logger.info("Initializing Leader Election with ZooKeeper at " + zooKeeperUrl + ", path " + zkElectionBasePath);
        leaderElection = new W2151443LeaderElection(zooKeeperUrl, zkElectionBasePath,
                etcdServiceInstanceId, getServerAddress(), this); // Pass 'this' as listener
        leaderElection.connect();
        leaderElection.volunteerForLeadership(); // This will trigger onElectedLeader or onWorker

        // 2. Start gRPC Server
        // Pass 'this' (ConcertTicketServerApp) to services so they can query 'isLeader()'
        // and potentially get leader address for forwarding (though forwarding logic is in services)
        W2151443ShowManagementServiceImpl showManagementService = new W2151443ShowManagementServiceImpl(W2151443_showsDataStore, this);
        W2151443ReservationServiceImpl reservationService = new W2151443ReservationServiceImpl(W2151443_showsDataStore, this);

        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(showManagementService)
                .addService(reservationService)
                .build()
                .start();
        logger.info("W2151443 gRPC Server started, listening on port: " + grpcPort + " with instance ID: " + etcdServiceInstanceId);


        // 3. General Service Registration with etcd (all nodes do this)
        logger.info("Attempting general service registration with etcd for instance: " + etcdServiceInstanceId);
        boolean generalRegistration = etcdClient.registerService(etcdServiceBasePath, etcdServiceInstanceId, serverHostIp, grpcPort, serviceRegistryTTL);
        if (generalRegistration) {
            logger.info("General registration for " + etcdServiceInstanceId + " successful with etcd.");
        } else {
            logger.severe("CRITICAL: Failed general registration for " + etcdServiceInstanceId + " with etcd.");
        }


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Initiating shutdown sequence...");
            if (leaderElection != null) {
                System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Closing ZooKeeper leader election session...");
                leaderElection.close(); // This will remove its ephemeral znode
            }
            if (etcdClient != null) {
                System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Deregistering from etcd...");
                if (isCurrentlyLeader) { // If it was leader, try to remove leader key
                    etcdClient.deregisterService(etcdLeaderPath, "leader-" + etcdServiceInstanceId); // Or a fixed key like "current"
                }
                etcdClient.deregisterService(etcdServiceBasePath, etcdServiceInstanceId); // Deregister its own instance
                etcdClient.shutdownHeartbeat();
                System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Etcd client resources released.");
            }
            if (grpcServer != null && !grpcServer.isShutdown()) {
                System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Shutting down gRPC server...");
                try {
                    grpcServer.shutdown().awaitTermination(10, TimeUnit.SECONDS); // Shorter timeout
                    System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): gRPC server shut down.");
                } catch (InterruptedException e) {
                    System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): gRPC server shutdown interrupted.");
                    grpcServer.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            System.err.println("*** W2151443 Server (" + etcdServiceInstanceId + "): Shutdown sequence complete.");
        }));
    }

    @Override
    public void onElectedLeader() {
        isCurrentlyLeader = true;
        logger.info("***** " + etcdServiceInstanceId + " (" + getServerAddress() + ") HAS BEEN ELECTED LEADER! *****");
        // Leader registers its specific address in etcd under the _leader key
        if (etcdClient != null) {
            // For simplicity, the leader's unique key could be its own instance ID prefixed
            // Or a fixed key like "/services/W2151443ConcertTicketService/_leader"
            // with the value being its address. Let's use a fixed key for the leader.
            boolean leaderRegistered = etcdClient.registerService(etcdServiceBasePath, "_leader", serverHostIp, grpcPort, serviceRegistryTTL);
            if(leaderRegistered) {
                logger.info("Leader " + etcdServiceInstanceId + " published its address (" + getServerAddress() + ") to etcd path: " + etcdServiceBasePath + "/_leader");
            } else {
                logger.severe("Leader " + etcdServiceInstanceId + " FAILED to publish its address to etcd path: " + etcdServiceBasePath + "/_leader");
            }
        }
    }

    @Override
    public void onWorker() {
        boolean wasLeader = isCurrentlyLeader;
        isCurrentlyLeader = false;
        logger.info("----- " + etcdServiceInstanceId + " (" + getServerAddress() + ") IS A WORKER.-----");
        // If it *was* leader and now it's not (e.g. due to partition then recovery, or session expiry),
        // it should attempt to remove the _leader key *if it was the one that set it*.
        // This is tricky without knowing if it successfully set it or if another node took over.
        // A simpler approach is that the new leader will overwrite the _leader key.
        // For explicit cleanup if it steps down:
        if (wasLeader && etcdClient != null) {
            logger.info("Worker " + etcdServiceInstanceId + " was previously leader, attempting to clean up its _leader entry in etcd.");
            // This deregister should ideally only happen if this node was the one that wrote to _leader.
            // Using a simple deregister for now. The new leader will overwrite anyway.
            etcdClient.deregisterService(etcdServiceBasePath, "_leader");
        }
    }

    public String getLeaderAddressFromZooKeeper() { // For secondaries to find the leader
        if (leaderElection != null) {
            try {
                return leaderElection.getCurrentLeaderData();
            } catch (KeeperException | InterruptedException e) {
                logger.log(Level.WARNING, "Failed to get current leader data from ZooKeeper for " + etcdServiceInstanceId, e);
                return null;
            }
        }
        return null;
    }


    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    // Main method to allow running multiple instances on different ports for testing
    public static void main(String[] args) throws Exception {
        int grpcPort = 9090; // Default for the first instance
        String idSuffix = "p" + grpcPort;

        if (args.length > 0) {
            try {
                grpcPort = Integer.parseInt(args[0]);
                idSuffix = "p" + grpcPort; // Update suffix based on port
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number provided: " + args[0] + ". Using default port " + grpcPort);
            }
        }
        if (args.length > 1) {
            idSuffix = args[1]; // Allow specifying a unique ID suffix directly
        }


        final ConcertTicketServerApp serverApp = new ConcertTicketServerApp(grpcPort, idSuffix);
        serverApp.start();
        serverApp.blockUntilShutdown();
    }
}
