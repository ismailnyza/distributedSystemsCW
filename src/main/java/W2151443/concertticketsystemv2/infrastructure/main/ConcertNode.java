package W2151443.concertticketsystemv2.infrastructure.main;

import W2151443.concertticketsystemv2.infrastructure.config.AppConfig;
// Import other necessary infrastructure managers and services later
// import com.W2151443.concertticketsystemv2.infrastructure.grpc.GrpcServerManager;
// import com.W2151443.concertticketsystemv2.infrastructure.coordination.etcd.EtcdClientManager;
// import com.W2151443.concertticketsystemv2.infrastructure.coordination.zk.ZooKeeperClientManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
// import java.util.concurrent.TimeUnit;

public class ConcertNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcertNode.class);

    private final String nodeId;
    private final String host;
    private final int grpcPort;
    private final String nodeGrpcAddress;

    // To be initialized later
    // private GrpcServerManager grpcServerManager;
    // private EtcdClientManager etcdClientManager;
    // private ZooKeeperClientManager zooKeeperClientManager;
    // private LeaderElectionService leaderElectionService;
    // private NameServiceProvider nameServiceProvider;
    // ... other services and managers ...

    public ConcertNode(String nodeId, String host, int grpcPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.grpcPort = grpcPort;
        this.nodeGrpcAddress = host + ":" + grpcPort;

        LOGGER.info("Initializing ConcertNode: ID={}, Address={}", nodeId, nodeGrpcAddress);
    }

    public void start() throws IOException, InterruptedException {
        LOGGER.info("Starting ConcertNode {} on {}...", nodeId, nodeGrpcAddress);

        // 1. Initialize Configuration (already done statically by AppConfig, but can fetch dynamic here)
        // String etcdEndpoints = AppConfig.getString("etcd.endpoints");
        // String zkConnectString = AppConfig.getString("zookeeper.connectString");
        // LOGGER.info("Using etcd endpoints: {}", etcdEndpoints);
        // LOGGER.info("Using ZooKeeper connect string: {}", zkConnectString);

        // 2. Initialize EtcdClientManager and NameServiceProvider
        // etcdClientManager = new EtcdClientManager(etcdEndpoints);
        // etcdClientManager.connect();
        // nameServiceProvider = new EtcdNameServiceProvider(etcdClientManager.getClient(), nodeId, nodeGrpcAddress); // Simplified
        // nameServiceProvider.registerNode(); // Start registration and TTL refresh

        // 3. Initialize ZooKeeperClientManager and LeaderElectionService, DistributedLockManager
        // zooKeeperClientManager = new ZooKeeperClientManager(zkConnectString);
        // zooKeeperClientManager.connect();
        // leaderElectionService = new ZkLeaderElectionService(zooKeeperClientManager.getClient(), nodeId, nodeGrpcAddress, nameServiceProvider);
        // leaderElectionService.participateInElection();
        // distributedLockManager = new ZkDistributedLockManager(zooKeeperClientManager.getClient(), nodeId);

        // 4. Initialize Data Store
        // concertRepository = new InMemoryConcertRepository();
        // reservationRepository = new InMemoryReservationRepository();

        // 5. Initialize Use Cases (and inject dependencies)
        // These would be created and then passed to Grpc Controllers

        // 6. Initialize and Start gRPC Server with Service Implementations (Controllers)
        // ConcertAdminGrpcController adminController = new ConcertAdminGrpcController(...useCases...);
        // ConcertCustomerGrpcController customerController = new ConcertCustomerGrpcController(...useCases...);
        // InternalNodeCommGrpcController internalController = new InternalNodeCommGrpcController(...);
        // grpcServerManager = new GrpcServerManager(grpcPort, adminController, customerController, internalController);
        // grpcServerManager.start();

        LOGGER.info("ConcertNode {} started successfully.", nodeId);

        // Keep main thread alive for server, or if GrpcServerManager.start() blocks, this is not needed here.
        // For now, we'll assume gRPC server starts and blocks or we use awaitTermination.
        // If grpcServerManager.start() is non-blocking:
        // Thread.currentThread().join(); // Or a more sophisticated shutdown mechanism
    }

    public void stop() throws InterruptedException {
        LOGGER.info("Stopping ConcertNode {}...", nodeId);
        // if (grpcServerManager != null) {
        //     grpcServerManager.stop();
        // }
        // if (leaderElectionService != null) {
        //     leaderElectionService.stepDown(); // Relinquish leadership if held
        // }
        // if (nameServiceProvider != null) {
        //     nameServiceProvider.deregisterNode(); // Stop TTL refresh and attempt deregister
        // }
        // if (etcdClientManager != null) {
        //     etcdClientManager.close();
        // }
        // if (zooKeeperClientManager != null) {
        //     zooKeeperClientManager.close();
        // }
        LOGGER.info("ConcertNode {} stopped.", nodeId);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            // Command line: nodeId host grpcPort
            // Config file: etcd.endpoints, zookeeper.connectString
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
            LOGGER.error("Invalid gRPC port provided: {}", args[2]);
            System.exit(1);
        }

        // AppConfig is loaded statically, values can be accessed via AppConfig.getString(...)
        LOGGER.info("Attempting to start Node ID: {}, Host: {}, gRPC Port: {}", nodeId, host, grpcPort);
        LOGGER.info("Loading bootstrap config from AppConfig. Etcd: {}, ZK: {}",
                AppConfig.getString("etcd.endpoints", "NOT_FOUND"),
                AppConfig.getString("zookeeper.connectString", "NOT_FOUND"));


        final ConcertNode node = new ConcertNode(nodeId, host, grpcPort);

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.warn("*** ConcertNode {} JVM Shutting Down. Initiating stop sequence. ***", nodeId);
            try {
                node.stop();
            } catch (InterruptedException e) {
                LOGGER.error("Error during node shutdown:", e);
                Thread.currentThread().interrupt();
            }
            LOGGER.warn("*** ConcertNode {} Shutdown Complete. ***", nodeId);
        }));

        try {
            node.start();
            // If GrpcServerManager.start() is blocking and has awaitTermination,
            // then the main thread will block here.
            // Otherwise, we might need a Server.awaitTermination() call here or Thread.join().
            // For now, let's assume start() will block or handle its own blocking.
            // To prevent main from exiting immediately if start() is non-blocking and doesn't join:
            // This is a simple way to keep it alive; a proper server would use awaitTermination on its gRPC server.
            Object keepAlive = new Object();
            synchronized (keepAlive) {
                keepAlive.wait();
            }

        } catch (IOException e) {
            LOGGER.error("Failed to start ConcertNode {}: {}", nodeId, e.getMessage(), e);
            System.exit(1);
        } catch (InterruptedException e) {
            LOGGER.warn("ConcertNode {} main thread interrupted. Shutting down...", nodeId);
            Thread.currentThread().interrupt(); // Preserve interrupt status
            try {
                node.stop(); // Attempt graceful shutdown
            } catch (InterruptedException ex) {
                LOGGER.error("Error during node shutdown after interrupt:", ex);
            }
        }
    }

    // Getters for nodeId, nodeGrpcAddress for other components if needed
    public String getNodeId() {
        return nodeId;
    }

    public String getNodeGrpcAddress() {
        return nodeGrpcAddress;
    }
}
