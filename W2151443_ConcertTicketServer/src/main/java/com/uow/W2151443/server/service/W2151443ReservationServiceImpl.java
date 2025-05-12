// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/server/service/W2151443ReservationServiceImpl.java
package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.coordination.twopc.DistributedTxCoordinator;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance;
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class W2151443ReservationServiceImpl extends W2151443ReservationServiceGrpc.W2151443ReservationServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ReservationServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData;
    // private final Map<String, Object> reservationsLog = new ConcurrentHashMap<>(); // Consider if this log is still needed or how it's used
    private final ConcertTicketServerApp serverApp;

    public W2151443ReservationServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    private List<ServiceInstance> getSecondaryNodes() {
        if (serverApp.etcdClient == null) {
            logger.warning(serverApp.etcdServiceInstanceId + ": etcdClient is null in ReservationService. Cannot discover secondaries.");
            return new ArrayList<>();
        }
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
        if (!serverApp.isLeader()) {
            logger.warning(serverApp.etcdServiceInstanceId + ": Non-leader attempting to perform replication after reservation. Aborting.");
            return;
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo state after reservation/2PC commit for ID: " + updatedShowInfoAfterReservation.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();

        if (secondaries.isEmpty()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): No secondary nodes found to replicate ShowInfo for ID: " + updatedShowInfoAfterReservation.getShowId());
            return;
        }

        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo to secondary: " + secondary.getId() + " at " + secondary.getIp() + ":" + secondary.getPort());
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .replicateShowUpdate(updatedShowInfoAfterReservation);
                if (response.getSuccess()) {
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): Successfully replicated ShowInfo post-reservation to " + secondary.getId());
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Failed to replicate ShowInfo post-reservation to " + secondary.getId() + ". Reason: " + response.getMessage());
                }
            }  catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): StatusRuntimeException while replicating ShowInfo post-reservation to secondary " + secondary.getId(), e);
            } catch (Exception e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): General Error replicating ShowInfo post-reservation to secondary " + secondary.getId(), e);
            } finally {
                if (channel != null) try { channel.shutdown().awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt();}
            }
        }
    }


    @Override
    public void reserveTicket(ReserveTicketRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) {
                responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable for ReserveTicket.").asRuntimeException()); return;
            }
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Forwarding ReserveTicket to leader " + leaderAddressString);
            String[] parts = leaderAddressString.split(":");
            if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address for ReserveTicket.").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub lStub = W2151443ReservationServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.reserveTicket(request));
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding ReserveTicket: " + e.getMessage()).asRuntimeException());
            } finally {
                if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }

        // --- Leader Node Logic for simple concert-only reservation ---
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing ReserveTicket for ShowID: " + request.getShowId() + ", Customer: " + request.getCustomerId());
        ShowInfo updatedShowState = null;
        String reservationId = null;
        ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();

        synchronized (showsData) {
            ShowInfo show = showsData.get(request.getShowId());
            if (show == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found.").setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else if (request.getItemsCount() == 0) {
                responseBuilder.setSuccess(false).setMessage("No items in reservation request.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
            } else {
                ShowInfo.Builder showBuilder = show.toBuilder();
                boolean possibleToReserveAll = true;
                for (ReservationItem item : request.getItemsList()) {
                    TierDetails tier = show.getTiersMap().get(item.getTierId());
                    if (tier == null) {
                        possibleToReserveAll = false;
                        responseBuilder.setSuccess(false).setMessage("Tier ID " + item.getTierId() + " not found.").setErrorCode(ErrorCode.TIER_NOT_FOUND);
                        break;
                    }
                    if (tier.getCurrentStock() < item.getQuantity()) {
                        possibleToReserveAll = false;
                        responseBuilder.setSuccess(false).setMessage("Not enough stock for tier " + item.getTierId() + ". Requested: " + item.getQuantity() + ", Available: " + tier.getCurrentStock()).setErrorCode(ErrorCode.OUT_OF_STOCK);
                        break;
                    }
                }

                if (possibleToReserveAll) {
                    for (ReservationItem item : request.getItemsList()) {
                        TierDetails tier = showBuilder.getTiersMap().get(item.getTierId()); // Get from builder
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        showBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    updatedShowState = showBuilder.build();
                    showsData.put(request.getShowId(), updatedShowState); // Commit change locally

                    reservationId = "res-W2151443-" + request.getRequestId(); // Use request ID for idempotency if re-processed
                    // reservationsLog.put(reservationId, request); // Log the reservation (consider structure)

                    responseBuilder.setSuccess(true).setMessage("Tickets reserved successfully by leader " + serverApp.etcdServiceInstanceId)
                            .setReservationId(reservationId)
                            .setUpdatedShowInfo(updatedShowState);
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): Tickets reserved for " + request.getCustomerId() + ". Reservation ID: " + reservationId + ". Initiating replication.");
                }
            }
            if (responseBuilder.getSuccess() && updatedShowState != null) {
                performFullShowReplicationAfterReservation(updatedShowState); // Replicate the new state
            } else if (!responseBuilder.getSuccess()){ // Ensure updated show info is sent even on failure for client context
                if(show != null) responseBuilder.setUpdatedShowInfo(show); // Send current state if reservation failed
            }
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }


    @Override
    public void reserveCombo(ReserveComboRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
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

        String transactionId = "tx-combo-" + request.getRequestId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Initiating 2PC for ReserveCombo. TxID: " + transactionId + ", ShowID: " + request.getShowId() + ", Customer: " + request.getCustomerId());

        serverApp.pendingComboTransactions.put(transactionId, request);

        List<ServiceInstance> secondaries = getSecondaryNodes();
        DistributedTxCoordinator coordinator = new DistributedTxCoordinator(serverApp, transactionId, request, secondaries);

        ReservationResponse clientResponse;
        ShowInfo finalShowStateForClient = null; // To hold the state to send back
        boolean success = false;

        try {
            boolean preparePhaseSucceeded = coordinator.prepare();

            if (!preparePhaseSucceeded) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Prepare phase failed for TxID: " + transactionId + " (likely leader local check). Aborting.");
                coordinator.decideGlobalOutcome(); // This will ensure ABORT is set and (attempted) notification
                success = false;
            } else {
                logger.info(serverApp.etcdServiceInstanceId + " (Leader): Prepare phase initiated for TxID: " + transactionId + ". Proceeding to decide global outcome.");
                success = coordinator.decideGlobalOutcome(); // This triggers commit/abort and ZK updates
            }

            // Construct response based on 2PC outcome
            // The actual data update for COMMIT happens in onGlobalCommit listener in ConcertTicketServerApp
            synchronized(showsData) { // Read the potentially updated state
                finalShowStateForClient = showsData.get(request.getShowId());
            }

            if (success) {
                logger.info(serverApp.etcdServiceInstanceId + " (Leader): 2PC GLOBAL_COMMIT for TxID: " + transactionId + " successful for client " + request.getCustomerId());
                // Data change and removal from pendingComboTransactions is handled by the onGlobalCommit listener
                clientResponse = ReservationResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Combo (Concert + After-Party) reserved successfully via 2PC by leader " + serverApp.etcdServiceInstanceId)
                        .setReservationId("combo-res-" + transactionId)
                        .setUpdatedShowInfo(finalShowStateForClient != null ? finalShowStateForClient : ShowInfo.newBuilder().setShowId(request.getShowId()).build())
                        .build();
                // After successful 2PC commit and local data update by listener, replicate final state
                if (finalShowStateForClient != null) {
                    performFullShowReplicationAfterReservation(finalShowStateForClient);
                }
            } else {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): 2PC GLOBAL_ABORT for TxID: " + transactionId + " for client " + request.getCustomerId());
                // Removal from pendingComboTransactions is handled by onGlobalAbort listener
                clientResponse = ReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Combo reservation failed: Distributed transaction aborted.")
                        .setErrorCode(ErrorCode.TRANSACTION_ABORTED)
                        .setUpdatedShowInfo(finalShowStateForClient != null ? finalShowStateForClient : ShowInfo.newBuilder().setShowId(request.getShowId()).build())
                        .build();
            }
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Leader): ZooKeeper error during 2PC for TxID: " + transactionId, e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            clientResponse = ReservationResponse.newBuilder()
                    .setSuccess(false).setMessage("Combo reservation failed due to coordination error.")
                    .setErrorCode(ErrorCode.INTERNAL_SERVER_ERROR).build();
            serverApp.pendingComboTransactions.remove(transactionId); // Ensure cleanup
        } catch (Exception e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Leader): Unexpected error during 2PC for TxID: " + transactionId, e);
            clientResponse = ReservationResponse.newBuilder()
                    .setSuccess(false).setMessage("Combo reservation failed: " + e.getMessage())
                    .setErrorCode(ErrorCode.INTERNAL_SERVER_ERROR).build();
            serverApp.pendingComboTransactions.remove(transactionId); // Ensure cleanup
        }

        responseObserver.onNext(clientResponse);
        responseObserver.onCompleted();
    }


    @Override
    public void browseEvents(BrowseEventsRequest request, StreamObserver<BrowseEventsResponse> responseObserver) {
        logger.info(serverApp.etcdServiceInstanceId + ": Processing BrowseEvents request. Filter: " + (request.hasFilterByDate() ? request.getFilterByDate() : "ALL"));
        BrowseEventsResponse.Builder responseBuilder = BrowseEventsResponse.newBuilder();
        List<ShowInfo> filteredShows;
        synchronized (showsData) { // Read-only access, but synchronize if iterators are used or for memory visibility
            if (request.hasFilterByDate() && !request.getFilterByDate().equalsIgnoreCase("ALL") && !request.getFilterByDate().isEmpty()) {
                String filterDate = request.getFilterByDate();
                filteredShows = showsData.values().stream()
                        .filter(show -> show.getShowDate().startsWith(filterDate)) // Simple prefix match
                        .collect(Collectors.toList());
            } else {
                filteredShows = new ArrayList<>(showsData.values());
            }
        }
        responseBuilder.addAllShows(filteredShows);
        logger.fine(serverApp.etcdServiceInstanceId + ": Found " + filteredShows.size() + " shows matching filter.");
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getShowAvailability(GetShowAvailabilityRequest request, StreamObserver<ShowInfo> responseObserver) {
        logger.info(serverApp.etcdServiceInstanceId + ": Processing GetShowAvailability request for Show ID: " + request.getShowId());
        ShowInfo showInfo;
        synchronized (showsData) {
            showInfo = showsData.get(request.getShowId());
        }
        if (showInfo != null) {
            responseObserver.onNext(showInfo);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": Show ID " + request.getShowId() + " not found for GetShowAvailability.");
            responseObserver.onError(Status.NOT_FOUND.withDescription("Show ID " + request.getShowId() + " not found.").asRuntimeException());
            return; // Explicit return after onError
        }
        responseObserver.onCompleted();
    }
}
