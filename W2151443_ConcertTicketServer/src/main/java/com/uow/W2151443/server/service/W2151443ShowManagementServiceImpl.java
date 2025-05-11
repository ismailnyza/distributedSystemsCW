package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.concert.service.W2151443ShowManagementServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class W2151443ShowManagementServiceImpl extends W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ShowManagementServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData; // Shared data store

    // Variables to simulate its role (will be dynamic in distributed setup)
    private boolean isPrimary = true; // For PROJECT-003, assume it's always primary
    // private String nodeId = "server1"; // Example node ID

    public W2151443ShowManagementServiceImpl(Map<String, ShowInfo> showsDataStore) {
        this.showsData = showsDataStore;
    }

    // --- Helper to simulate replication call (will be real in Epic 4) ---
    private void simulateReplication(ShowInfo updatedShowInfo) {
        if (isPrimary) {
            logger.info("[Simulated Primary Action] Replicating update for Show ID: " + updatedShowInfo.getShowId() + " to secondaries.");
            // In Epic 4: Iterate through known secondaries and make gRPC call:
            // internalNodeStub.replicateShowUpdate(updatedShowInfo);
        }
    }
    private void simulateStockReplication(UpdateStockRequest stockRequest) {
         if (isPrimary) {
            logger.info("[Simulated Primary Action] Replicating stock update for Show ID: " + stockRequest.getShowId() + " to secondaries.");
            // In Epic 4: internalNodeStub.replicateStockChange(stockRequest);
        }
    }


    @Override
    public void addShow(AddShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        ShowInfo showInfoToAdd = request.getShowInfo();
        String showId = showInfoToAdd.getShowId();

        if (showId == null || showId.trim().isEmpty()) {
            showId = "show-W2151443-" + UUID.randomUUID().toString().substring(0, 8);
            showInfoToAdd = showInfoToAdd.toBuilder().setShowId(showId).build();
        }
        logger.info("Processing AddShow for ID: " + showId + ", Name: " + showInfoToAdd.getShowName());

        // In a distributed system, if this node is not primary, it would forward to primary.
        // For PROJECT-003, we assume it's the primary or acting autonomously.
        if (!isPrimary) {
            logger.info("This node is a secondary. [Simulating Forward to Primary] for AddShow: " + showId);
            // In Epic 3/4: Forward to actual primary. For now, can proceed if testing secondary logic.
            // For PROJECT-003, let's allow it to proceed to test the logic.
        }

        synchronized (showsData) { // Synchronize access to shared data
            if (showsData.containsKey(showId)) {
                logger.warning("Show with ID " + showId + " already exists.");
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("Show with ID " + showId + " already exists.")
                        .asRuntimeException());
                return;
            }
            showsData.put(showId, showInfoToAdd);
            simulateReplication(showInfoToAdd); // Simulate replication
        }

        logger.info("Show added: " + showInfoToAdd.getShowName() + " (ID: " + showId + ")");
        GenericResponse response = GenericResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Show '" + showInfoToAdd.getShowName() + "' added successfully with ID: " + showId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateShowDetails(UpdateShowDetailsRequest request, StreamObserver<GenericResponse> responseObserver) {
        String showId = request.getShowId();
        logger.info("Processing UpdateShowDetails for ID: " + showId);

        synchronized (showsData) {
            ShowInfo existingShow = showsData.get(showId);
            if (existingShow == null) {
                logger.warning("Show with ID " + showId + " not found for update.");
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
            // Note: Tier structure/pricing updates are complex and might need dedicated logic
            // or careful merging if tiers are part of this request. Assuming top-level details for now.

            ShowInfo updatedShow = updatedShowBuilder.build();
            showsData.put(showId, updatedShow);
            simulateReplication(updatedShow); // Simulate replication
        }

        GenericResponse response = GenericResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Show details updated for ID: " + showId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateStock(UpdateStockRequest request, StreamObserver<GenericResponse> responseObserver) {
        String showId = request.getShowId();
        logger.info("Processing UpdateStock for show ID: " + showId + ", Request ID: " + request.getRequestId());

        GenericResponse errorResponse = null;
        ShowInfo updatedShowForReplication = null;

        synchronized (showsData) { // Synchronize for consistent read-modify-write
            ShowInfo show = showsData.get(showId);
            if (show == null) {
                errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND).build();
            } else {
                ShowInfo.Builder showBuilder = show.toBuilder();
                boolean updated = false;

                if (request.hasTierId()) { // Tier stock update
                    if (!show.getTiersMap().containsKey(request.getTierId())) {
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("Tier not found: " + request.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND).build();
                    } else {
                        TierDetails tier = show.getTiersMap().get(request.getTierId());
                        TierDetails.Builder tierBuilder = tier.toBuilder();
                        int newStock = tier.getCurrentStock() + request.getChangeInTierStock();
                        // Add validation: e.g., stock cannot be negative, or exceed total (depending on rules)
                        if (newStock < 0) newStock = 0; // Example: prevent negative stock from admin update
                        tierBuilder.setCurrentStock(newStock);
                        showBuilder.putTiers(request.getTierId(), tierBuilder.build());
                        updated = true;
                    }
                }

                if (errorResponse == null && request.hasChangeInAfterPartyStock()) { // After-party stock update
                    if (!show.getAfterPartyAvailable()) {
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("After-party not available for show " + showId).setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE).build();
                    } else {
                        int newStock = show.getAfterPartyCurrentStock() + request.getChangeInAfterPartyStock();
                        if (newStock < 0) newStock = 0; // Example
                        showBuilder.setAfterPartyCurrentStock(newStock);
                        updated = true;
                    }
                }

                if (errorResponse == null) {
                    if (updated) {
                        updatedShowForReplication = showBuilder.build();
                        showsData.put(showId, updatedShowForReplication);
                    } else {
                        // If no specific update fields were matched but no error occurred prior
                        errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("No stock update performed for show ID: " + showId + ". Check request parameters.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA).build();
                    }
                }
            }
        } // end synchronized block

        if (errorResponse != null) {
            responseObserver.onNext(errorResponse);
        } else if (updatedShowForReplication != null) {
            // simulateReplication(updatedShowForReplication); // More granular: replicate the UpdateStockRequest itself
            simulateStockReplication(request); // Replicate the operation
            GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Stock updated for show ID: " + showId).build();
            responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void cancelShow(CancelShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        String showId = request.getShowId();
        logger.info("Processing CancelShow for ID: " + showId);
        ShowInfo removedShow;
        synchronized (showsData) {
            removedShow = showsData.remove(showId);
        }

        if (removedShow != null) {
            // In a distributed system, replication of cancellation would happen here.
            // For example, by sending a specific "show cancelled" message or replicating a null/marker state.
            if (isPrimary) {
                 logger.info("[Simulated Primary Action] Replicating cancellation for Show ID: " + showId);
                 // This might involve sending the CancelShowRequest itself or a specific "delete" event.
            }
            GenericResponse response = GenericResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Show with ID " + showId + " cancelled.")
                    .build();
            responseObserver.onNext(response);
        } else {
            logger.warning("Show with ID " + showId + " not found for cancellation.");
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Show with ID " + showId + " not found for cancellation.")
                    .asRuntimeException());
        }
        responseObserver.onCompleted();
    }
}
