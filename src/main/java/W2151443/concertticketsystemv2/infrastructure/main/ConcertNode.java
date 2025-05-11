package W2151443.concertticketsystemv2.infrastructure.main;

import W2151443.concertticketsystemv2.infrastructure.config.AppConfig;
import W2151443.concertticketsystemv2.infrastructure.coordination.etcd.EtcdClientManager;
import W2151443.concertticketsystemv2.infrastructure.coordination.zk.ZooKeeperClientManager;
import W2151443.concertticketsystemv2.infrastructure.coordination.zk.ZooKeeperConnectionListener;
// import W2151443.concertticketsystemv2.infrastructure.coordination.zk.ZkLeaderElectionService; // Uncomment when implemented
// import com.W2151443.concertticketsystemv2.infrastructure.grpc.GrpcServerManager; // Uncomment when implemented

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class ConcertNode implements ZooKeeperConnectionListener { // Ensure your interface matches this path
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcertNode.class);

    private final String nodeId;
    private final String host;
    private final int grpcPort;
    private final String nodeGrpcAddress;

    private EtcdClientManager etcdClientManager;
    private String etcdNodeRegistrationPath;

    private ZooKeeperClientManager zooKeeperClientManager;
    // private ZkLeaderElectionService leaderElectionService; // Uncomment when ZkLeaderElectionService is ready
    // private ZkDistributedLockManager distributedLockManager; // Uncomment when implemented

    // private GrpcServerManager grpcServerManager; // Uncomment when implemented
    // private InMemoryConcertRepository concertRepository; // Example

    public ConcertNode(String nodeId, String host, int grpcPort) {
        this.nodeId = Objects.requireNonNull(nodeId, "Node ID cannot be null or empty").trim();
        if (this.nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be empty.");
        }
        this.host = Objects.requireNonNull(host, "Host cannot be null or empty").trim();
        if (this.host.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be empty.");
        }

        if (grpcPort <= 0 || grpcPort > 65535) {
            throw new IllegalArgumentException("Invalid gRPC port: " + grpcPort + ". Must be between 1 and 65535.");
        }
        this.grpcPort = grpcPort;
        this.nodeGrpcAddress = this.host + ":" + this.grpcPort;

        LOGGER.info("Initializing ConcertNode: ID='{}', Host='{}', gRPC Port={}, Full gRPC Address='{}'",
                this.nodeId, this.host, this.grpcPort, this.nodeGrpcAddress);
    }

    public void start() throws IOException, InterruptedException {
        LOGGER.info("Starting ConcertNode '{}' on '{}'...", nodeId, nodeGrpcAddress);

        String etcdEndpoints = AppConfig.getString("etcd.endpoints");
        String zkConnectString = AppConfig.getString("zookeeper.connectString");
        int zkSessionTimeout = AppConfig.getInt("zookeeper.sessionTimeout.ms", 5000);

        LOGGER.info("Node '{}' using etcd endpoints: {}", nodeId, etcdEndpoints);
        LOGGER.info("Node '{}' using ZooKeeper connect string: {} with session timeout: {}ms",
                nodeId, zkConnectString, zkSessionTimeout);

        etcdClientManager = new EtcdClientManager(etcdEndpoints);
        try {
            etcdClientManager.connect();
            String nodesBasePath = AppConfig.getString("etcd.service.nodes.path");
            this.etcdNodeRegistrationPath = nodesBasePath + "/" + this.nodeId;
            long etcdTtlSeconds = AppConfig.getLong("etcd.ttl.seconds");

            boolean registeredInEtcd = etcdClientManager.registerWithLease(
                    this.etcdNodeRegistrationPath,
                    this.nodeGrpcAddress,
                    etcdTtlSeconds
            );
            if (!registeredInEtcd) {
                LOGGER.error("Node '{}' FAILED to register with etcd. Service discovery might be impaired.", nodeId);
            } else {
                LOGGER.info("Node '{}' successfully registered in etcd at key '{}' with value '{}' and TTL {}s.",
                        nodeId, this.etcdNodeRegistrationPath, this.nodeGrpcAddress, etcdTtlSeconds);
            }
        } catch (Exception e) {
            LOGGER.error("Node '{}' critical failure during etcd initialization or registration. Exiting.", nodeId, e);
            throw new IOException("Failed to initialize or register with etcd: " + e.getMessage(), e);
        }

        zooKeeperClientManager = new ZooKeeperClientManager(zkConnectString, zkSessionTimeout);
        zooKeeperClientManager.setConnectionListener(this);
        try {
            zooKeeperClientManager.connect();
            LOGGER.info("Node '{}' ZooKeeper Client Manager initialized and connection process started.", nodeId);
            if (!zooKeeperClientManager.isConnected()) {
                LOGGER.error("Node '{}' failed to establish initial ZooKeeper connection. System may not function correctly.", nodeId);
                throw new IOException("Failed to establish initial ZooKeeper connection for node " + nodeId);
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Node '{}' critical failure during ZooKeeper client connection. Exiting.", nodeId, e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            if (etcdClientManager != null) etcdClientManager.close();
            throw e;
        }

        // --- Initialize Leader Election Service (Placeholder - uncomment and implement ZkLeaderElectionService) ---
        /*
        String electionRootPath = AppConfig.getString("zookeeper.election.rootPath");
        leaderElectionService = new ZkLeaderElectionService(
                 zooKeeperClientManager, // Pass the manager
                 electionRootPath,
                 nodeId,
                 nodeGrpcAddress, // Data to store in the leader znode
                 this, // ConcertNode can implement a LeaderStateListener interface
                 etcdClientManager // To register/update primary status in etcd if needed
        );
        try {
            leaderElectionService.start(); // Begins participation
            LOGGER.info("Node '{}' started participation in leader election at path '{}'.", nodeId, electionRootPath);
        } catch (Exception e) {
            LOGGER.error("Node '{}' failed to start leader election service. Exiting.", nodeId, e);
            // Clean up already initialized services
            if (zooKeeperClientManager != null) zooKeeperClientManager.close();
            if (etcdClientManager != null) etcdClientManager.close();
            throw new IOException("Failed to start leader election service: " + e.getMessage(), e);
        }
        */

        // --- Initialize gRPC Server (Placeholder) ---
        // GrpcServerManager grpcServerManager = new GrpcServerManager(grpcPort, /* pass service impls */);
        // grpcServerManager.start();
        // LOGGER.info("Node '{}' gRPC server started on port {}.", nodeId, grpcPort);

        LOGGER.info("ConcertNode '{}' core services initialized and started (ZooKeeper client connected).", nodeId);

        synchronized (this) {
            while (true) {
                this.wait();
            }
        }
    }

    public void stop() throws InterruptedException {
        LOGGER.info("Stopping ConcertNode '{}'...", nodeId);

        // if (grpcServerManager != null) {
        //     grpcServerManager.stop();
        //     LOGGER.info("Node '{}' gRPC server stopped.", nodeId);
        // }

        // if (leaderElectionService != null) {
        //     leaderElectionService.close();
        //     LOGGER.info("Node '{}' stopped leader election participation.", nodeId);
        // }

        if (etcdClientManager != null) {
            etcdClientManager.close();
            LOGGER.info("Node '{}' etcd client manager closed and deregistered.", nodeId);
        }

        if (zooKeeperClientManager != null) {
            zooKeeperClientManager.close();
            LOGGER.info("Node '{}' ZooKeeper client manager closed.", nodeId);
        }

        synchronized (this) {
            this.notifyAll();
        }
        LOGGER.info("ConcertNode '{}' stopped successfully.", nodeId);
    }

    @Override
    public void onZooKeeperConnected() {
        LOGGER.info("Node '{}' [ZK Listener]: Successfully (re)connected to ZooKeeper. Current ZK State: {}",
                nodeId, (zooKeeperClientManager != null && zooKeeperClientManager.getZooKeeperClient() != null) ?
                        zooKeeperClientManager.getZooKeeperClient().getState() : "N/A");
        // If leader election service is implemented, you might need to re-initiate participation
        // if (leaderElectionService != null && zooKeeperClientManager.isConnected()) {
        //    LOGGER.info("Node '{}' re-evaluating leader election status due to ZK (re)connection.", nodeId);
        //    try {
        //        leaderElectionService.start(); // Or a specific method to re-join election
        //    } catch (Exception e) {
        //        LOGGER.error("Node '{}' failed to re-start leader election after ZK connection.", nodeId, e);
        //    }
        // }
        // Any other ZK-dependent services should check their state.
    }

    @Override
    public void onZooKeeperDisconnected() {
        LOGGER.warn("Node '{}' [ZK Listener]: Disconnected from ZooKeeper. Auto-reconnect should be in progress.", nodeId);
        // If this node was the leader, it should be prepared to lose leadership.
        // The ZkLeaderElectionService should handle this by watching its ephemeral znode's existence.
        // if (leaderElectionService != null && leaderElectionService.isLeader()) {
        //     LOGGER.warn("Node '{}' (Leader) is now disconnected from ZK. Leadership might be lost upon session timeout.", nodeId);
        // }
    }

    @Override
    public void onZooKeeperSessionExpired() {
        LOGGER.error("Node '{}' [ZK Listener]: ZooKeeper session EXPIRED. This is critical! All ephemeral nodes are lost.", nodeId);
        // This is a severe event. The node effectively needs to rejoin the cluster.
        // The ZooKeeperClientManager might attempt to create a new session.
        // All services relying on the previous session (like leader election, locks) must be reset and re-initialized.
        // if (leaderElectionService != null) {
        //    LOGGER.info("Node '{}' attempting to fully restart leader election due to ZK session expiry.", nodeId);
        //    leaderElectionService.close(); // Clean up old state based on the expired session
        //    try {
        //        // It's crucial that the ZooKeeperClientManager has established a new session
        //        // before attempting to restart services that depend on it.
        //        // This might require a more coordinated re-initialization sequence.
        //        if (zooKeeperClientManager.isConnected()) { // Check if a new session is up
        //            leaderElectionService.start();
        //        } else {
        //            LOGGER.error("Node '{}' cannot restart leader election, ZK client not reconnected after session expiry.", nodeId);
        //            // Potentially trigger a node shutdown or a more robust recovery mechanism.
        //        }
        //    } catch (Exception e) {
        //        LOGGER.error("Node '{}' failed to restart leader election after ZK session expiry.", nodeId, e);
        //    }
        // }
        // For now, you might need to implement a strategy to re-initialize or shut down the node.
        LOGGER.error("Node '{}': Further action required to handle ZK session expiry (e.g., re-initialize services or shutdown).", nodeId);
    }


    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: ConcertNode <nodeId> <host> <grpcPort>");
            System.err.println("Example: ConcertNode node-1 localhost 50051");
            System.err.println("Ensure config.properties is in the classpath for etcd/zookeeper details.");
            System.exit(1);
        }

        String nodeId = args[0];
        String host = args[1];
        int grpcPort = 0;
        try {
            grpcPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            LOGGER.error("Invalid gRPC port provided: '{}'. Must be an integer.", args[2], e);
            System.err.println("Invalid gRPC port: " + args[2]);
            System.exit(1);
        }

        LOGGER.info("Attempting to start Node: ID='{}', Host='{}', gRPC Port={}", nodeId, host, grpcPort);
        LOGGER.info("Bootstrap Config Loaded. Etcd Endpoints: '{}', ZK Connect String: '{}'",
                AppConfig.getString("etcd.endpoints", "ETCD_ENDPOINT_NOT_CONFIGURED"),
                AppConfig.getString("zookeeper.connectString", "ZK_CONNECT_STRING_NOT_CONFIGURED"));

        final ConcertNode node = new ConcertNode(nodeId, host, grpcPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.warn("*** ConcertNode '{}' JVM Shutting Down. Initiating graceful stop sequence. ***", node.getNodeId());
            try {
                node.stop();
            } catch (InterruptedException e) {
                LOGGER.error("Error during ConcertNode '{}' graceful shutdown:", node.getNodeId(), e);
                Thread.currentThread().interrupt();
            }
            LOGGER.warn("*** ConcertNode '{}' Shutdown Complete. ***", node.getNodeId());
        }, "ConcertNode-ShutdownHook"));

        try {
            node.start();
        } catch (IOException e) {
            LOGGER.error("Node '{}' failed to start due to IOException: {}", nodeId, e.getMessage(), e);
            System.exit(1);
        } catch (InterruptedException e) {
            LOGGER.warn("Node '{}' main thread interrupted during startup or while running. Initiating shutdown...", nodeId);
            Thread.currentThread().interrupt();
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeGrpcAddress() {
        return nodeGrpcAddress;
    }
}
