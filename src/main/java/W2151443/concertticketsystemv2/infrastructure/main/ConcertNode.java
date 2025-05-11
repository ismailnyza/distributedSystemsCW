package W2151443.concertticketsystemv2.infrastructure.main;

import W2151443.concertticketsystemv2.infrastructure.config.AppConfig;
import W2151443.concertticketsystemv2.infrastructure.coordination.etcd.EtcdClientManager; // Import
// import com.W2151443.concertticketsystemv2.infrastructure.grpc.GrpcServerManager;
// import com.W2151443.concertticketsystemv2.infrastructure.coordination.zk.ZooKeeperClientManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects; // Add import for validation
// import java.util.concurrent.TimeUnit;

public class ConcertNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcertNode.class);

    private final String nodeId;
    private final String host;
    private final int grpcPort;
    private final String nodeGrpcAddress;

    // Add EtcdClientManager
    private EtcdClientManager etcdClientManager;
    private String etcdNodeRegistrationPath; // Key for this node in etcd

    // To be initialized later
    // private GrpcServerManager grpcServerManager;
    // private ZooKeeperClientManager zooKeeperClientManager;
    // ... other services and managers ...

    public ConcertNode(String nodeId, String host, int grpcPort) {
        // Validate inputs
        this.nodeId = Objects.requireNonNull(nodeId, "Node ID cannot be null");
        this.host = Objects.requireNonNull(host, "Host cannot be null");
        if (grpcPort <= 0 || grpcPort > 65535) {
            throw new IllegalArgumentException("Invalid gRPC port: " + grpcPort);
        }
        this.grpcPort = grpcPort;
        this.nodeGrpcAddress = host + ":" + grpcPort;

        LOGGER.info("Initializing ConcertNode: ID={}, Address={}", nodeId, nodeGrpcAddress);
    }

    public void start() throws IOException, InterruptedException {
        LOGGER.info("Starting ConcertNode {} on {}...", nodeId, nodeGrpcAddress);

        // 1. Initialize Configuration
        String etcdEndpoints = AppConfig.getString("etcd.endpoints");
        // String zkConnectString = AppConfig.getString("zookeeper.connectString");
        LOGGER.info("Using etcd endpoints: {}", etcdEndpoints);
        // LOGGER.info("Using ZooKeeper connect string: {}", zkConnectString);

        // 2. Initialize EtcdClientManager and Register Node
        etcdClientManager = new EtcdClientManager(etcdEndpoints);
        etcdClientManager.connect(); // Connect to etcd

        // Construct the key for this node's registration
        String nodesBasePath = AppConfig.getString("etcd.service.nodes.path", "/services/concert_system_v2/nodes");
        this.etcdNodeRegistrationPath = nodesBasePath + "/" + this.nodeId;
        long ttlSeconds = AppConfig.getLong("etcd.ttl.seconds", 10L);

        boolean registered = etcdClientManager.registerWithLease(this.etcdNodeRegistrationPath, this.nodeGrpcAddress, ttlSeconds);
        if (!registered) {
            LOGGER.error("Node {} failed to register with etcd. This might affect service discovery.", nodeId);
            // Decide on critical failure or proceed in a degraded state
            // For now, we proceed, but other nodes might not see it.
        } else {
            LOGGER.info("Node {} successfully registered in etcd at {} with value {} and TTL {}s.",
                    nodeId, this.etcdNodeRegistrationPath, this.nodeGrpcAddress, ttlSeconds);
        }


        // 3. Initialize ZooKeeperClientManager (placeholder for next ticket)
        // ...

        // ... other initializations ...

        LOGGER.info("ConcertNode {} started successfully (etcd registration attempted).", nodeId);

        // Keep main thread alive (as before)
        Object keepAlive = new Object();
        synchronized (keepAlive) {
            keepAlive.wait();
        }
    }

    public void stop() throws InterruptedException {
        LOGGER.info("Stopping ConcertNode {}...", nodeId);

        if (etcdClientManager != null) {
            // Deregistration is handled by etcdClientManager.close() which calls its own deregister()
            etcdClientManager.close();
        }

        // ... stop other managers ...
        LOGGER.info("ConcertNode {} stopped.", nodeId);
    }

    public static void main(String[] args) {
        // Validate and parse arguments
        if (args.length < 3) {
            System.err.println("Usage: ConcertNode <nodeId> <host> <grpcPort>");
            System.exit(1);
        }

        String nodeId = args[0];
        String host = args[1];
        int grpcPort;
        try {
            grpcPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid gRPC port: " + args[2]);
            System.exit(1);
            return; // Unreachable, but required for compilation
        }

        final ConcertNode node = new ConcertNode(nodeId, host, grpcPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.warn("*** ConcertNode {} JVM Shutting Down. Initiating stop sequence. ***", nodeId);
            try {
                node.stop(); // This will now call etcdClientManager.close()
            } catch (InterruptedException e) {
                LOGGER.error("Error during node shutdown:", e);
                Thread.currentThread().interrupt();
            }
            LOGGER.warn("*** ConcertNode {} Shutdown Complete. ***", nodeId);
        }));

        try {
            node.start();
        } catch (IOException e) {
            LOGGER.error("Failed to start ConcertNode {}: {}", nodeId, e.getMessage(), e);
            System.exit(1);
        } catch (InterruptedException e) {
            LOGGER.warn("ConcertNode {} main thread interrupted. Shutting down...", nodeId);
            Thread.currentThread().interrupt();
            try {
                node.stop();
            } catch (InterruptedException ex) {
                LOGGER.error("Error during node shutdown after interrupt:", ex);
            }
        }
    }

    // ... getters ...
}
