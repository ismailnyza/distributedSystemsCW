package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.server.ConcertTicketServerApp; // To access leader status & leader info
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class W2151443ShowManagementServiceImpl extends W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ShowManagementServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData; // Shared data store
    private final ConcertTicketServerApp serverApp; // Reference to the main server application

    public W2151443ShowManagementServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    // --- Helper to simulate replication call (will be real in Epic 4) ---
    private void simulateReplication(ShowInfo updatedShowInfo, String operation) {
        // This check is illustrative; actual replication is only done by the leader
        if (serverApp.isLeader()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader) [Simulated Replication]: Replicating " + operation + " for Show ID: " + updatedShowInfo.getShowId());
        }
    }
    private void simulateStockReplication(UpdateStockRequest stockRequest) {
        if (serverApp.isLeader()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader) [Simulated Replication]: Replicating stock update for Show ID: " + stockRequest.getShowId());
        }
    }
    private void simulateCancelReplication(String showId) {
        if (serverApp.isLeader()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader) [Simulated Replication]: Replicating cancellation for Show ID: " + showId);
        }
    }


    // --- Write Operations: Check for Leadership ---

    @Override
    public void addShow(AddShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received AddShow request. Not the leader. Current leader might be at: " + leaderAddress);
            // TODO for SYNC-003 full: Implement forwarding to leader.
            // For now, reject if not leader.
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Please send write requests to the leader node. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        ShowInfo showInfoToAdd = request.getShowInfo();
        String showId = showInfoToAdd.getShowId();
        if (showId == null || showId.trim().isEmpty()) {
            showId = "show-W2151443-" + UUID.randomUUID().toString().substring(0, 8);
            showInfoToAdd = showInfoToAdd.toBuilder().setShowId(showId).build();
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing AddShow for ID: " + showId + ", Name: " + showInfoToAdd.getShowName());

        synchronized (showsData) {
            if (showsData.containsKey(showId)) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Show with ID " + showId + " already exists.");
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("Show with ID " + showId + " already exists.")
                        .asRuntimeException());
                return;
            }
            showsData.put(showId, showInfoToAdd);
            simulateReplication(showInfoToAdd, "AddShow");
        }

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Show added: " + showInfoToAdd.getShowName() + " (ID: " + showId + ")");
        GenericResponse response = GenericResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Show '" + showInfoToAdd.getShowName() + "' added successfully by leader " + serverApp.etcdServiceInstanceId + " with ID: " + showId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateShowDetails(UpdateShowDetailsRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received UpdateShowDetails request. Not the leader. Leader might be: " + leaderAddress);
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Update operations only on leader. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing UpdateShowDetails for ID: " + showId);
        ShowInfo updatedShow;
        synchronized (showsData) {
            ShowInfo existingShow = showsData.get(showId);
            if (existingShow == null) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Show with ID " + showId + " not found for update.");
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Show with ID " + showId + " not found.")
                        .asRuntimeException());
                return;
            }
            ShowInfo.Builder updatedShowBuilder = existingShow.toBuilder();
            if (request.hasShowName()) updatedShowBuilder.setShowName(request.getShowName());
            if (request.hasShowDate()) updatedShowBuilder.setShowDate(request.getShowDate());
            if (request.hasVenue()) updatedShowBuilder.setVenue(request.getVenue());
            if (request.hasDescription()) updatedShowBuilder.setDescription(request.getDescription());
            updatedShow = updatedShowBuilder.build();
            showsData.put(showId, updatedShow);
            simulateReplication(updatedShow, "UpdateShowDetails");
        }
        GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Show details updated by leader " + serverApp.etcdServiceInstanceId).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateStock(UpdateStockRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received UpdateStock request. Not the leader. Leader might be: " + leaderAddress);
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Update operations only on leader. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing UpdateStock for show ID: " + showId + ", Request ID: " + request.getRequestId());
        GenericResponse errorResponse = null;
        ShowInfo finalShowStateForReplication = null;

        synchronized (showsData) {
            ShowInfo show = showsData.get(showId);
            if (show == null) {
                errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND).build();
            } else {
                ShowInfo.Builder showBuilder = show.toBuilder();
                boolean updated = false;
                if (request.hasTierId()) {
                    if (!show.getTiersMap().containsKey(request.getTierId())) {
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("Tier not found: " + request.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND).build();
                    } else {
                        TierDetails tier = show.getTiersMap().get(request.getTierId());
                        TierDetails.Builder tierBuilder = tier.toBuilder();
                        int newStock = tier.getCurrentStock() + request.getChangeInTierStock();
                        if (newStock < 0) newStock = 0;
                        tierBuilder.setCurrentStock(newStock);
                        showBuilder.putTiers(request.getTierId(), tierBuilder.build());
                        updated = true;
                    }
                }
                if (errorResponse == null && request.hasChangeInAfterPartyStock()) {
                    if (!show.getAfterPartyAvailable()) {
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("After-party not available for show " + showId).setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE).build();
                    } else {
                        int newStock = show.getAfterPartyCurrentStock() + request.getChangeInAfterPartyStock();
                        if (newStock < 0) newStock = 0;
                        showBuilder.setAfterPartyCurrentStock(newStock);
                        updated = true;
                    }
                }
                if (errorResponse == null) {
                    if (updated) {
                        finalShowStateForReplication = showBuilder.build();
                        showsData.put(showId, finalShowStateForReplication);
                    } else {
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("No stock update performed. Check params.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA).build();
                    }
                }
            }
        } // end synchronized

        if (errorResponse != null) {
            responseObserver.onNext(errorResponse);
        } else if (finalShowStateForReplication != null) {
            simulateStockReplication(request); // Simulate replication of the operation
            GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Stock updated by leader " + serverApp.etcdServiceInstanceId).build();
            responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void cancelShow(CancelShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received CancelShow request. Not the leader. Leader might be: " + leaderAddress);
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Write operations only on leader. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing CancelShow for ID: " + showId);
        ShowInfo removedShow;
        synchronized (showsData) {
            removedShow = showsData.remove(showId);
        }

        if (removedShow != null) {
            simulateCancelReplication(showId);
            GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Show cancelled by leader " + serverApp.etcdServiceInstanceId).build();
            responseObserver.onNext(response);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Show ID " + showId + " not found for cancellation.");
            responseObserver.onError(Status.NOT_FOUND.withDescription("Show ID " + showId + " not found for cancellation.").asRuntimeException());
        }
        responseObserver.onCompleted();
    }
}
