package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class W2151443ReservationServiceImpl extends W2151443ReservationServiceGrpc.W2151443ReservationServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ReservationServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData; // Shared data store
    private final Map<String, Object> reservationsLog = new ConcurrentHashMap<>(); // Simple log for reservations

    // Variables to simulate its role
    private boolean isPrimary = true; // For PROJECT-003, assume it's always primary
    // private String nodeId = "server1";

    public W2151443ReservationServiceImpl(Map<String, ShowInfo> showsDataStore) {
        this.showsData = showsDataStore;
    }

    // --- Helper to simulate replication call (will be real in Epic 4) ---
    private void simulateReservationReplication(ShowInfo updatedShowInfo, String reservationId, Object originalRequest) {
        if (isPrimary) {
            logger.info("[Simulated Primary Action] Replicating reservation (ID: "+ reservationId +") update for Show ID: " + updatedShowInfo.getShowId() + " to secondaries.");
            // In Epic 4: This would involve replicating the state change (new stock levels)
            // and potentially the reservation record if that's also replicated.
            // The ReplicateShowUpdate might be sufficient if it carries all necessary info.
        }
    }

    @Override
    public void browseEvents(BrowseEventsRequest request, StreamObserver<BrowseEventsResponse> responseObserver) {
        logger.info("Processing BrowseEvents request. Filter: " + (request.hasFilterByDate() ? request.getFilterByDate() : "None"));
        List<ShowInfo> currentShows;
        // In a distributed system, reads can come from local data (eventual consistency)
        // For PROJECT-003, this is fine as it's the only node.
        synchronized (showsData) { // Ensure consistent view if data is being modified elsewhere
            currentShows = new ArrayList<>(showsData.values());
        }

        if (request.hasFilterByDate() && !request.getFilterByDate().equalsIgnoreCase("ALL") && !request.getFilterByDate().isEmpty()) {
            String filterDate = request.getFilterByDate();
            currentShows = currentShows.stream()
                               .filter(show -> show.getShowDate().startsWith(filterDate))
                               .collect(Collectors.toList());
        }

        BrowseEventsResponse response = BrowseEventsResponse.newBuilder()
                .addAllShows(currentShows)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getShowAvailability(GetShowAvailabilityRequest request, StreamObserver<ShowInfo> responseObserver) {
        String showId = request.getShowId();
        logger.info("Processing GetShowAvailability for show ID: " + showId);
        ShowInfo show;
        synchronized (showsData) {
            show = showsData.get(showId);
        }

        if (show != null) {
            responseObserver.onNext(show);
        } else {
            logger.warning("Show not found for GetShowAvailability: " + showId);
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Show with ID " + showId + " not found.")
                    .asRuntimeException());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void reserveTicket(ReserveTicketRequest request, StreamObserver<ReservationResponse> responseObserver) {
        String showId = request.getShowId();
        String requestId = request.getRequestId(); // For idempotency later
        logger.info("Processing ReserveTicket for show ID: " + showId + ", Request ID: " + requestId);

        // For PROJECT-003, assume this node is primary and handles the write.
        // Later, if it's a secondary, it would forward to primary.

        ShowInfo updatedShowState = null;
        String reservationId = null;
        ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();

        synchronized (showsData) { // This entire block is a critical section
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else {
                ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
                boolean possibleToReserve = true;
                List<ReservationItem> itemsToReserve = request.getItemsList();

                if (itemsToReserve.isEmpty()){
                     responseBuilder.setSuccess(false).setMessage("No items requested for reservation.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                     possibleToReserve = false; // Mark as not possible to prevent further processing
                } else {
                    for (ReservationItem item : itemsToReserve) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        if (tier == null) {
                            responseBuilder.setSuccess(false).setMessage("Tier not found: " + item.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND);
                            possibleToReserve = false;
                            break;
                        }
                        if (tier.getCurrentStock() < item.getQuantity()) {
                            responseBuilder.setSuccess(false).setMessage("Not enough stock for tier: " + item.getTierId()).setErrorCode(ErrorCode.OUT_OF_STOCK);
                            possibleToReserve = false;
                            break;
                        }
                    }
                }


                if (possibleToReserve) {
                    for (ReservationItem item : itemsToReserve) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    updatedShowState = mutableShowBuilder.build();
                    showsData.put(showId, updatedShowState); // Commit local change

                    reservationId = "res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request); // Log reservation

                    responseBuilder.setSuccess(true)
                            .setMessage("Tickets reserved successfully for show: " + currentShow.getShowName())
                            .setReservationId(reservationId)
                            .setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                     // If possibleToReserve is false but no specific error code was set yet (e.g. empty items)
                     responseBuilder.setSuccess(false).setMessage("Reservation failed due to unspecified reason or invalid request.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }
                // If error occurred, responseBuilder is already populated with error details
                 if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow); // Send original show info on failure
                }
            }
        } // end synchronized block

        if (updatedShowState != null && reservationId != null) {
            simulateReservationReplication(updatedShowState, reservationId, request); // Simulate replication
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void reserveCombo(ReserveComboRequest request, StreamObserver<ReservationResponse> responseObserver) {
        String showId = request.getShowId();
        String requestId = request.getRequestId(); // For idempotency later
        logger.info("Processing ReserveCombo for show ID: " + showId + ", Request ID: " + requestId);

        // For PROJECT-003, this is a local atomic operation.
        // In Epic 5, this node (if primary) will act as 2PC coordinator.

        ShowInfo updatedShowState = null;
        String reservationId = null;
        ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();

        synchronized (showsData) { // Entire block is a critical section
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else {
                ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
                boolean possibleToReserve = true;

                // Check concert tickets
                 if (request.getConcertItemsList().isEmpty()){
                     responseBuilder.setSuccess(false).setMessage("No concert items requested for combo reservation.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                     possibleToReserve = false;
                } else {
                    for (ReservationItem item : request.getConcertItemsList()) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        if (tier == null) {
                            responseBuilder.setSuccess(false).setMessage("Concert tier not found: " + item.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND);
                            possibleToReserve = false;
                            break;
                        }
                        if (tier.getCurrentStock() < item.getQuantity()) {
                            responseBuilder.setSuccess(false).setMessage("Not enough stock for concert tier: " + item.getTierId()).setErrorCode(ErrorCode.OUT_OF_STOCK);
                            possibleToReserve = false;
                            break;
                        }
                    }
                }


                // Check after-party tickets if reservation is still possible
                if (possibleToReserve) {
                    if (!mutableShowBuilder.getAfterPartyAvailable()) {
                        responseBuilder.setSuccess(false).setMessage("After-party not available for this show.").setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE);
                        possibleToReserve = false;
                    } else if (mutableShowBuilder.getAfterPartyCurrentStock() < request.getAfterPartyQuantity()) {
                        responseBuilder.setSuccess(false).setMessage("Not enough stock for after-party tickets.").setErrorCode(ErrorCode.OUT_OF_STOCK);
                        possibleToReserve = false;
                    }
                }

                if (possibleToReserve) {
                    // All checks passed, perform updates
                    for (ReservationItem item : request.getConcertItemsList()) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    mutableShowBuilder.setAfterPartyCurrentStock(mutableShowBuilder.getAfterPartyCurrentStock() - request.getAfterPartyQuantity());

                    updatedShowState = mutableShowBuilder.build();
                    showsData.put(showId, updatedShowState); // Commit local change

                    reservationId = "combo-res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request); // Log reservation

                    responseBuilder.setSuccess(true)
                            .setMessage("Combo (Concert + After-Party) reserved successfully for show: " + currentShow.getShowName())
                            .setReservationId(reservationId)
                            .setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                     // If possibleToReserve is false but no specific error code was set yet
                     responseBuilder.setSuccess(false).setMessage("Combo reservation failed due to unspecified reason or invalid request.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }

                // If error occurred, responseBuilder is already populated.
                // Ensure original show info is sent on failure if not already set.
                if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow);
                }
            }
        } // end synchronized block

        if (updatedShowState != null && reservationId != null) {
            // In Epic 5, this is where 2PC coordination would happen before this point.
            // For PROJECT-003, the local commit is final for this node.
            // We still simulate the replication of the final state.
            simulateReservationReplication(updatedShowState, reservationId, request);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}