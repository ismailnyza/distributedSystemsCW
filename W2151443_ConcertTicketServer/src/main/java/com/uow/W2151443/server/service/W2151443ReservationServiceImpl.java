package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.coordination.twopc.DistributedTransaction;
import com.uow.W2151443.coordination.twopc.DistributedTxCoordinator; // Import
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance;
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException; // For 2PC exceptions

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class W2151443ReservationServiceImpl extends W2151443ReservationServiceGrpc.W2151443ReservationServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ReservationServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData;
    private final Map<String, Object> reservationsLog = new ConcurrentHashMap<>();
    private final ConcertTicketServerApp serverApp;

    public W2151443ReservationServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    // Helper to get secondary nodes (same as in ShowManagementService)
    private List<ServiceInstance> getSecondaryNodes() {
        if (serverApp.etcdClient == null) return new ArrayList<>();
        List<ServiceInstance> allNodes = serverApp.etcdClient.discoverServiceInstances(serverApp.etcdServiceBasePath);
        List<ServiceInstance> secondaryNodes = new ArrayList<>();
        for (ServiceInstance node : allNodes) {
            if (!node.getId().equals(serverApp.etcdServiceInstanceId)) {
                secondaryNodes.add(node);
            }
        }
        return secondaryNodes;
    }

    private void performFullShowReplicationAfterReservation(ShowInfo updatedShowInfoAfterReservation) {
        // ... (Same as before, called by leader after a successful local commit)
        if (!serverApp.isLeader()) return;
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo state after reservation for ID: " + updatedShowInfoAfterReservation.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();
        for (ServiceInstance secondary : secondaries) { /* ... gRPC call to secondary.replicateShowUpdate ... */ }
    }


    // reserveTicket remains the same (leader processes, replicates full ShowInfo)
    @Override
    public void reserveTicket(ReserveTicketRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic as previously defined) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { /* ... error ... */ return; }
            // ... (actual forwarding gRPC call)
            return;
        }
        // --- Leader Node Logic for simple reservation ---
        ShowInfo updatedShowState = null; String reservationId = null; ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();
        synchronized (showsData) { /* ... entire local reservation logic as before ... */ }
        if (responseBuilder.getSuccess() && updatedShowState != null) {
            performFullShowReplicationAfterReservation(updatedShowState);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }


    @Override
    public void reserveCombo(ReserveComboRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // --- Secondary Node: Forward ReserveCombo to Leader ---
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Forwarding ReserveCombo to leader at " + leaderAddressString);
            ManagedChannel leaderChannel = null;
            try {
                if (leaderAddressString == null || leaderAddressString.isEmpty()) throw new IllegalStateException("Leader address not available for ReserveCombo");
                String[] parts = leaderAddressString.split(":");
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub leaderStub = W2151443ReservationServiceGrpc.newBlockingStub(leaderChannel);
                ReservationResponse leaderResponse = leaderStub.reserveCombo(request);
                responseObserver.onNext(leaderResponse);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Secondary): Error forwarding ReserveCombo.", e);
                responseObserver.onError(Status.INTERNAL.withDescription("Error forwarding ReserveCombo to leader: " + e.getMessage()).asRuntimeException());
            } finally {
                if (leaderChannel != null) try { leaderChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            return;
        }

        // --- Leader Node Logic: Initiate and Coordinate 2PC for ReserveCombo ---
        String transactionId = "tx-combo-" + request.getRequestId(); // Use client's request ID for tx ID part
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Initiating 2PC for ReserveCombo. TxID: " + transactionId + ", ShowID: " + request.getShowId());

        // Store details for listener callback
        serverApp.pendingComboTransactions.put(transactionId, request);

        List<ServiceInstance> secondaries = getSecondaryNodes();
        DistributedTxCoordinator coordinator = new DistributedTxCoordinator(serverApp, transactionId, request, secondaries);

        ReservationResponse clientResponse;
        try {
            boolean prepareInitiated = coordinator.prepare(); // Sends prepare to self (local check) and others

            if (!prepareInitiated) { // This means leader's local prepare failed
                clientResponse = ReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Combo reservation failed: Leader unable to prepare locally.")
                        .setErrorCode(ErrorCode.OUT_OF_STOCK) // Or a more generic internal error
                        .build();
                serverApp.pendingComboTransactions.remove(transactionId); // Clean up
            } else {
                // Allow time for voting and then decide global outcome
                boolean globalCommit = coordinator.decideGlobalOutcome();

                if (globalCommit) {
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): 2PC GLOBAL_COMMIT for TxID: " + transactionId);
                    // The actual data change is done by the onGlobalCommit listener in ConcertTicketServerApp
                    // Here, we just construct the success response for the client.
                    // The listener also handles removing from pendingComboTransactions.
                    ShowInfo finalShowState;
                    synchronized(showsData) { // Read the final state after commit
                        finalShowState = showsData.get(request.getShowId());
                    }
                    clientResponse = ReservationResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Combo (Concert + After-Party) reserved successfully via 2PC by leader " + serverApp.etcdServiceInstanceId)
                            .setReservationId("combo-res-" + transactionId) // Use txId as base for reservation ID
                            .setUpdatedShowInfo(finalShowState != null ? finalShowState : ShowInfo.newBuilder().setShowId(request.getShowId()).build()) // Send updated state
                            .build();
                    // After successful 2PC commit and local data update by listener, replicate final state
                    if (finalShowState != null) {
                        performFullShowReplicationAfterReservation(finalShowState);
                    }

                } else {
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): 2PC GLOBAL_ABORT for TxID: " + transactionId);
                    // onGlobalAbort listener in ConcertTicketServerApp handles cleanup of pending transaction.
                    ShowInfo currentShowState;
                    synchronized(showsData) { // Read current state to return to client
                        currentShowState = showsData.get(request.getShowId());
                    }
                    clientResponse = ReservationResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Combo reservation failed: Distributed transaction aborted.")
                            .setErrorCode(ErrorCode.TRANSACTION_ABORTED) // Or more specific if available
                            .setUpdatedShowInfo(currentShowState != null ? currentShowState : ShowInfo.newBuilder().setShowId(request.getShowId()).build())
                            .build();
                }
            }
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Leader): ZooKeeper error during 2PC for TxID: " + transactionId, e);
            clientResponse = ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Combo reservation failed due to coordination error.")
                    .setErrorCode(ErrorCode.INTERNAL_SERVER_ERROR)
                    .build();
            serverApp.pendingComboTransactions.remove(transactionId); // Clean up
            // Attempt to force an abort in ZK if begin() was called
            try {
                DistributedTransaction finalCheckTx = new DistributedTransaction(serverApp.getZooKeeperClient(), transactionId);
                if (finalCheckTx.getGlobalState(false) != DistributedTransaction.TxState.COMMITTED &&
                        finalCheckTx.getGlobalState(false) != DistributedTransaction.TxState.ABORTED) {
                    finalCheckTx.setGlobalState(DistributedTransaction.TxState.ABORTED);
                }
            } catch (Exception zkErr) {
                logger.log(Level.SEVERE, "Failed to ensure ABORT state in ZK for TxID: " + transactionId, zkErr);
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Leader): Unexpected error during 2PC for TxID: " + transactionId, e);
            clientResponse = ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Combo reservation failed due to an unexpected internal error.")
                    .setErrorCode(ErrorCode.INTERNAL_SERVER_ERROR)
                    .build();
            serverApp.pendingComboTransactions.remove(transactionId);
        }

        responseObserver.onNext(clientResponse);
        responseObserver.onCompleted();
    }

    // browseEvents and getShowAvailability remain the same (read-only, served locally)
    // ...
}
