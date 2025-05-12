// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/server/ConcertTicketServerApp.java
package com.uow.W2151443.server;

import com.uow.W2151443.concert.service.ReserveComboRequest;
import com.uow.W2151443.concert.service.ShowInfo;
import com.uow.W2151443.concert.service.ReservationItem;
import com.uow.W2151443.concert.service.TierDetails;
import com.uow.W2151443.coordination.W2151443LeaderElection;
import com.uow.W2151443.coordination.twopc.DistributedTxListener;
import com.uow.W2151443.coordination.twopc.DistributedTransaction;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient;
import com.uow.W2151443.server.service.W2151443InternalNodeServiceImpl;
import com.uow.W2151443.server.service.W2151443ReservationServiceImpl;
import com.uow.W2151443.server.service.W2151443ShowManagementServiceImpl;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcertTicketServerApp implements W2151443LeaderElection.LeaderChangeListener, DistributedTxListener {
    private static final Logger logger = Logger.getLogger(ConcertTicketServerApp.class.getName());
    private Server grpcServer;
    private final int grpcPort;
    private final String serverHostIp;

    public final Map<String, ShowInfo> W2151443_showsDataStore = new ConcurrentHashMap<>();

    public W2151443EtcdNameServiceClient etcdClient;
    public final String etcdUrl = "http://localhost:2381";
    public final String etcdServiceBasePath = "/services/W2151443ConcertTicketService";
    private final String etcdLeaderKey = "_leader";
    public String etcdServiceInstanceId;
    private final int serviceRegistryTTL = 30;

    private W2151443LeaderElection leaderElection;
    private final String zooKeeperUrl = "localhost:2181";
    private final String zkElectionBasePath = "/W2151443_concert_election";
    private volatile boolean isCurrentlyLeader = false;
    private ZooKeeper zooKeeperClient;

    public final Map<String, ReserveComboRequest> pendingComboTransactions = new ConcurrentHashMap<>();
    private final W2151443LeaderElection.LeaderChangeListener leaderChangeListener = this;
    // Store reference to InternalNodeService to call removeActiveParticipant
    private W2151443InternalNodeServiceImpl internalNodeServiceInstance;


    public ConcertTicketServerApp(int grpcPort, String serverIdSuffix) {
        this.grpcPort = grpcPort;
        this.serverHostIp = getHostIpAddress();
        this.etcdServiceInstanceId = "node-W2151443-" + serverIdSuffix + "-" + UUID.randomUUID().toString().substring(0, 4);
    }

    private String getHostIpAddress() {
        return "127.0.0.1";
    }

    public String getServerAddress() {
        return this.serverHostIp + ":" + this.grpcPort;
    }

    public boolean isLeader() {
        return isCurrentlyLeader;
    }

    public ZooKeeper getZooKeeperClient() {
        if (zooKeeperClient == null || zooKeeperClient.getState() != ZooKeeper.States.CONNECTED) {
            logger.warning(etcdServiceInstanceId + ": Attempted to get ZooKeeper client, but it's null or not connected. State: " + (zooKeeperClient != null ? zooKeeperClient.getState() : "null"));
        }
        return zooKeeperClient;
    }

    private void initializeZooKeeper() throws IOException, InterruptedException {
        logger.info(etcdServiceInstanceId + ": Initializing shared ZooKeeper client for URL: " + zooKeeperUrl);
        final CountDownLatch zkConnectedLatch = new CountDownLatch(1);
        zooKeeperClient = new ZooKeeper(zooKeeperUrl, 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                logger.info(etcdServiceInstanceId + ": Main ZooKeeper Event: " + watchedEvent);
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    if (zkConnectedLatch.getCount() > 0) {
                        zkConnectedLatch.countDown();
                        logger.info(etcdServiceInstanceId + ": Main ZooKeeper client connected state received.");
                    }
                    if (leaderElection != null && !isCurrentlyLeader) {
                        logger.info(etcdServiceInstanceId + ": Main ZooKeeper client reconnected. Re-evaluating leadership status.");
                        try {
                            leaderElection.volunteerForLeadership();
                        } catch (KeeperException | InterruptedException e) {
                            logger.log(Level.SEVERE, etcdServiceInstanceId + ": Error re-volunteering for leadership after ZK reconnection.", e);
                            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                        }
                    }
                } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                    logger.severe(etcdServiceInstanceId + ": Main ZooKeeper session EXPIRED. This is critical.");
                    isCurrentlyLeader = false;
                    leaderChangeListener.onWorker();
                    try {
                        logger.info(etcdServiceInstanceId + ": Attempting to re-initialize ZooKeeper connection and leader election after session expiry.");
                        if (zooKeeperClient != null) try { zooKeeperClient.close(); } catch (InterruptedException ignored) {}
                        initializeZooKeeper();
                        if (leaderElection != null) {
                            leaderElection.close();
                            leaderElection = new W2151443LeaderElection(zooKeeperClient, zkElectionBasePath, etcdServiceInstanceId, getServerAddress(), leaderChangeListener);
                            leaderElection.volunteerForLeadership();
                        }
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, etcdServiceInstanceId + ": Failed to recover from ZooKeeper session expiry.", e);
                        System.exit(1);
                    }
                } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    logger.warning(etcdServiceInstanceId + ": Main ZooKeeper client DISCONNECTED. ZooKeeper client will attempt to auto-reconnect.");
                    if (isCurrentlyLeader) {
                        logger.warning(etcdServiceInstanceId + ": Was leader, but now disconnected from ZK. Acting as worker until reconnected and leadership re-confirmed.");
                        isCurrentlyLeader = false;
                        leaderChangeListener.onWorker();
                    }
                }
            }
        });

        if (!zkConnectedLatch.await(20, TimeUnit.SECONDS)) {
            logger.severe(etcdServiceInstanceId + ": Timed out waiting for main ZooKeeper client to connect to " + zooKeeperUrl);
            if (zooKeeperClient != null) try { zooKeeperClient.close(); } catch (InterruptedException ignored) {}
            throw new IOException("Failed to connect main ZooKeeper client to " + zooKeeperUrl + " within timeout.");
        }
        logger.info(etcdServiceInstanceId + ": Main ZooKeeper client connected successfully with session ID: " + Long.toHexString(zooKeeperClient.getSessionId()));
    }


    public void start() throws Exception {
        logger.info("W2151443 Server Instance " + etcdServiceInstanceId + " starting on port " + grpcPort + "...");

        logger.info(etcdServiceInstanceId + ": Initializing etcd client for URL: " + etcdUrl);
        etcdClient = new W2151443EtcdNameServiceClient(etcdUrl);

        initializeZooKeeper();

        try {
            DistributedTransaction.ensureTransactionRootExists(zooKeeperClient);
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, etcdServiceInstanceId + ": CRITICAL - Failed to ensure/create ZooKeeper transaction root path: " + DistributedTransaction.TRANSACTION_ROOT_ZNODE + ". 2PC will fail.", e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw e;
        }

        logger.info(etcdServiceInstanceId + ": Initializing Leader Election using shared ZK client, path " + zkElectionBasePath);
        leaderElection = new W2151443LeaderElection(zooKeeperClient, zkElectionBasePath,
                etcdServiceInstanceId, getServerAddress(), this);
        leaderElection.volunteerForLeadership();

        W2151443ShowManagementServiceImpl showManagementService = new W2151443ShowManagementServiceImpl(W2151443_showsDataStore, this);
        W2151443ReservationServiceImpl reservationService = new W2151443ReservationServiceImpl(W2151443_showsDataStore, this);
        // Store the instance of W2151443InternalNodeServiceImpl to call its methods
        this.internalNodeServiceInstance = new W2151443InternalNodeServiceImpl(W2151443_showsDataStore, this);


        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(showManagementService)
                .addService(reservationService)
                .addService(this.internalNodeServiceInstance) // Use the stored instance
                .build()
                .start();
        logger.info("W2151443 gRPC Server started, listening on port: " + grpcPort + " with instance ID: " + etcdServiceInstanceId);

        logger.info(etcdServiceInstanceId + ": Attempting general service registration with etcd.");
        boolean generalRegistration = etcdClient.registerService(etcdServiceBasePath, etcdServiceInstanceId, serverHostIp, grpcPort, serviceRegistryTTL);
        if (generalRegistration) {
            logger.info("General registration for " + etcdServiceInstanceId + " (" + getServerAddress() + ") successful with etcd.");
        } else {
            logger.severe("CRITICAL: Failed general registration for " + etcdServiceInstanceId + " with etcd.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("--- W2151443 Server (" + etcdServiceInstanceId + "): Initiating shutdown sequence... ---");
            if (grpcServer != null && !grpcServer.isShutdown()) {
                System.err.println("Shutting down gRPC server for " + etcdServiceInstanceId + "...");
                try {
                    grpcServer.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    grpcServer.shutdownNow(); Thread.currentThread().interrupt();
                } finally {
                    System.err.println("gRPC server for " + etcdServiceInstanceId + (grpcServer.isTerminated() ? " shut down." : " shutdown process completed (may not be fully terminated)."));
                }
            }

            if (isCurrentlyLeader && etcdClient != null) {
                System.err.println("Leader " + etcdServiceInstanceId + " attempting to deregister its _leader key from etcd...");
                etcdClient.deregisterService(etcdServiceBasePath, etcdLeaderKey);
            }
            if (etcdClient != null) {
                System.err.println("Deregistering general instance " + etcdServiceInstanceId + " from etcd...");
                etcdClient.deregisterService(etcdServiceBasePath, etcdServiceInstanceId);
                etcdClient.shutdownHeartbeat();
            }

            if (leaderElection != null) {
                System.err.println("Closing ZooKeeper leader election resources for " + etcdServiceInstanceId + "...");
                leaderElection.close();
            }
            if (zooKeeperClient != null) {
                System.err.println("Closing main ZooKeeper client for " + etcdServiceInstanceId + "...");
                try { zooKeeperClient.close(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
            System.err.println("--- W2151443 Server (" + etcdServiceInstanceId + "): Shutdown sequence complete. ---");
        }));
        logger.info(etcdServiceInstanceId + ": Server startup sequence complete.");
    }


    @Override
    public void onElectedLeader() {
        boolean wasAlreadyLeader = isCurrentlyLeader;
        isCurrentlyLeader = true;
        if (!wasAlreadyLeader) {
            logger.info("***** " + etcdServiceInstanceId + " (" + getServerAddress() + ") HAS BEEN ELECTED LEADER! *****");
            if (etcdClient != null) {
                boolean leaderAdvertised = etcdClient.registerService(etcdServiceBasePath, etcdLeaderKey, serverHostIp, grpcPort, serviceRegistryTTL);
                if (leaderAdvertised) {
                    logger.info("Leader " + etcdServiceInstanceId + " published its address (" + getServerAddress() + ") to etcd path: " + etcdServiceBasePath + "/" + etcdLeaderKey);
                } else {
                    logger.severe("CRITICAL: Leader " + etcdServiceInstanceId + " FAILED to publish its address to etcd leader path: " + etcdServiceBasePath + "/" + etcdLeaderKey);
                }
            }
        } else {
            logger.fine(etcdServiceInstanceId + " (" + getServerAddress() + ") already leader, onElectedLeader callback received (possibly due to ZK re-check).");
        }
    }

    @Override
    public void onWorker() {
        boolean wasLeader = isCurrentlyLeader;
        isCurrentlyLeader = false;
        if (wasLeader) {
            logger.info("----- " + etcdServiceInstanceId + " (" + getServerAddress() + ") IS NO LONGER LEADER, NOW A WORKER. -----");
        } else {
            logger.fine(etcdServiceInstanceId + " (" + getServerAddress() + ") confirmed as WORKER or was already worker.");
        }
    }

    @Override
    public void onGlobalCommit(String transactionIdContext) {
        String coreTransactionId = transactionIdContext.startsWith("tx-combo-") ?
                transactionIdContext.substring(0, transactionIdContext.indexOf("_", "tx-combo-".length())) :
                (transactionIdContext.contains("_") ? transactionIdContext.substring(0, transactionIdContext.indexOf("_")) : transactionIdContext);


        ReserveComboRequest details = pendingComboTransactions.remove(coreTransactionId);

        if (details == null) {
            logger.warning(etcdServiceInstanceId + ": Received onGlobalCommit for unknown or already processed transaction context: " + transactionIdContext + " (core TxID: " + coreTransactionId + ")");
            if(internalNodeServiceInstance != null) internalNodeServiceInstance.removeActiveParticipant(coreTransactionId); // Still try to cleanup participant map
            return;
        }
        logger.info(etcdServiceInstanceId + ": Applying GLOBAL COMMIT for TxID: " + coreTransactionId + " related to original request for Show: " + details.getShowId());
        synchronized (W2151443_showsDataStore) {
            ShowInfo currentShow = W2151443_showsDataStore.get(details.getShowId());
            if (currentShow == null) {
                logger.severe(etcdServiceInstanceId + ": CRITICAL - Show " + details.getShowId() + " not found during onGlobalCommit for TxID: " + coreTransactionId + ". Data inconsistency!");
                if(internalNodeServiceInstance != null) internalNodeServiceInstance.removeActiveParticipant(coreTransactionId);
                return;
            }

            ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
            for (ReservationItem item : details.getConcertItemsList()) {
                TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                if (tier != null) {
                    TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                    mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                } else {
                    logger.severe(etcdServiceInstanceId + ": CRITICAL - Tier " + item.getTierId() + " not found during onGlobalCommit for TxID: " + coreTransactionId);
                }
            }
            if (currentShow.getAfterPartyAvailable() && details.getAfterPartyQuantity() > 0) {
                mutableShowBuilder.setAfterPartyCurrentStock(Math.max(0, mutableShowBuilder.getAfterPartyCurrentStock() - details.getAfterPartyQuantity()));
            }
            ShowInfo updatedShowInfo = mutableShowBuilder.build();
            W2151443_showsDataStore.put(details.getShowId(), updatedShowInfo);
            logger.info(etcdServiceInstanceId + ": Local data committed for TxID: " + coreTransactionId + ". Show: " + details.getShowId() + " updated. New AP Stock: " + updatedShowInfo.getAfterPartyCurrentStock());
        }
        if(internalNodeServiceInstance != null) internalNodeServiceInstance.removeActiveParticipant(coreTransactionId);
    }


    @Override
    public void onGlobalAbort(String transactionIdContext) {
        String coreTransactionId = transactionIdContext.startsWith("tx-combo-") ?
                transactionIdContext.substring(0, transactionIdContext.indexOf("_", "tx-combo-".length())) :
                (transactionIdContext.contains("_") ? transactionIdContext.substring(0, transactionIdContext.indexOf("_")) : transactionIdContext);

        ReserveComboRequest removed = pendingComboTransactions.remove(coreTransactionId);
        if (removed != null) {
            logger.info(etcdServiceInstanceId + ": Applying GLOBAL ABORT for transaction context: " + transactionIdContext + " (core TxID: " + coreTransactionId + "). Request details: " + removed.getRequestId());
        } else {
            logger.warning(etcdServiceInstanceId + ": Received onGlobalAbort for unknown or already processed transaction context: " + transactionIdContext + " (core TxID: "+coreTransactionId+")");
        }
        if(internalNodeServiceInstance != null) internalNodeServiceInstance.removeActiveParticipant(coreTransactionId);
    }


    public String getLeaderAddressFromZooKeeper() {
        if (leaderElection != null) {
            try {
                if (zooKeeperClient == null || zooKeeperClient.getState() != ZooKeeper.States.CONNECTED) {
                    logger.warning(etcdServiceInstanceId + ": ZooKeeper client not connected when trying to get leader data. State: " + (zooKeeperClient != null ? zooKeeperClient.getState() : "null"));
                    return null;
                }
                String leaderData = leaderElection.getCurrentLeaderData();
                if (leaderData != null && !leaderData.isEmpty()) {
                    logger.fine(etcdServiceInstanceId + ": Fetched leader data from ZooKeeper: " + leaderData);
                    return leaderData;
                } else {
                    logger.warning(etcdServiceInstanceId + ": Leader data from ZooKeeper is null or empty.");
                    return null;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.log(Level.WARNING, etcdServiceInstanceId + ": Failed to get current leader data from ZooKeeper", e);
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                logger.log(Level.SEVERE, etcdServiceInstanceId + ": Unexpected error getting leader data from ZooKeeper", e);
                return null;
            }
        } else {
            logger.warning(etcdServiceInstanceId + ": LeaderElection object is null. Cannot get leader address.");
            return null;
        }
    }


    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        int grpcPort = 9090;
        String idSuffix = "Srv" + grpcPort;

        if (args.length > 0) {
            try {
                grpcPort = Integer.parseInt(args[0]);
                idSuffix = "Srv" + grpcPort;
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0] + ". Using default port " + grpcPort);
            }
        }
        if (args.length > 1) {
            idSuffix = args[1];
        }

        final ConcertTicketServerApp serverApp = new ConcertTicketServerApp(grpcPort, idSuffix);
        try {
            serverApp.start();
            serverApp.blockUntilShutdown();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start server " + serverApp.etcdServiceInstanceId, e);
            serverApp.shutdownGracefully();
        }
    }

    private void shutdownGracefully() {
        logger.info("--- W2151443 Server (" + etcdServiceInstanceId + "): Initiating graceful shutdown due to startup failure... ---");
        if (grpcServer != null && !grpcServer.isShutdown()) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException ex) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (leaderElection != null) leaderElection.close();
        if (etcdClient != null) {
            etcdClient.deregisterService(etcdServiceBasePath, etcdServiceInstanceId);
            if (isCurrentlyLeader) etcdClient.deregisterService(etcdServiceBasePath, etcdLeaderKey);
            etcdClient.shutdownHeartbeat();
        }
        if (zooKeeperClient != null) {
            try { zooKeeperClient.close(); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        }
        logger.info("--- W2151443 Server (" + etcdServiceInstanceId + "): Graceful shutdown attempt complete. ---");
    }
}
