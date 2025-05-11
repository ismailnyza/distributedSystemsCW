package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.server.ConcertTicketServerApp; // To access leader status & leader info
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
    private final Map<String, Object> reservationsLog = new ConcurrentHashMap<>();
    private final ConcertTicketServerApp serverApp; // Reference to the main server application

    public W2151443ReservationServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    private void simulateReservationReplication(ShowInfo updatedShowInfo, String reservationId, String operationType) {
        if (serverApp.isLeader()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader) [Simulated Replication]: Replicating " + operationType +
                    " (ResID: "+ reservationId +") resulting state for Show ID: " + updatedShowInfo.getShowId());
        }
    }

    // --- Read Operations: Can be served by any node ---
    @Override
    public void browseEvents(BrowseEventsRequest request, StreamObserver<BrowseEventsResponse> responseObserver) {
        logger.info(serverApp.etcdServiceInstanceId + ": Processing BrowseEvents request. Filter: " + (request.hasFilterByDate() ? request.getFilterByDate() : "None"));
        List<ShowInfo> currentShows;
        synchronized (showsData) {
            currentShows = new ArrayList<>(showsData.values());
        }
        // ... (filtering logic remains same)
        if (request.hasFilterByDate() && !request.getFilterByDate().equalsIgnoreCase("ALL") && !request.getFilterByDate().isEmpty()) {
            String filterDate = request.getFilterByDate();
            currentShows = currentShows.stream()
                    .filter(show -> show.getShowDate().startsWith(filterDate))
                    .collect(Collectors.toList());
        }
        BrowseEventsResponse response = BrowseEventsResponse.newBuilder().addAllShows(currentShows).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getShowAvailability(GetShowAvailabilityRequest request, StreamObserver<ShowInfo> responseObserver) {
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + ": Processing GetShowAvailability for show ID: " + showId);
        ShowInfo show;
        synchronized (showsData) {
            show = showsData.get(showId);
        }
        if (show != null) {
            responseObserver.onNext(show);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": Show not found for GetShowAvailability: " + showId);
            responseObserver.onError(Status.NOT_FOUND.withDescription("Show with ID " + showId + " not found.").asRuntimeException());
        }
        responseObserver.onCompleted();
    }

    // --- Write Operations: Check for Leadership ---
    @Override
    public void reserveTicket(ReserveTicketRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received ReserveTicket request. Not the leader. Leader might be: " + leaderAddress);
            // TODO for SYNC-003 full: Implement forwarding to leader.
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Reservation operations only on leader. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing ReserveTicket for show ID: " + showId + ", Request ID: " + request.getRequestId());
        ShowInfo updatedShowState = null;
        String reservationId = null;
        ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();

        synchronized (showsData) {
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else {
                ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
                boolean possibleToReserve = true;
                List<ReservationItem> itemsToReserve = request.getItemsList();
                if (itemsToReserve.isEmpty()){
                    responseBuilder.setSuccess(false).setMessage("No items requested for reservation.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                    possibleToReserve = false;
                } else {
                    for (ReservationItem item : itemsToReserve) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        if (tier == null) { /* ... error ... */ possibleToReserve = false; break; }
                        if (tier.getCurrentStock() < item.getQuantity()) { /* ... error ... */ possibleToReserve = false; break; }
                    }
                }
                if (possibleToReserve) {
                    // ... (perform stock decrements as in previous full version)
                    for (ReservationItem item : itemsToReserve) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    updatedShowState = mutableShowBuilder.build();
                    showsData.put(showId, updatedShowState);
                    reservationId = "res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request);
                    responseBuilder.setSuccess(true).setMessage("Tickets reserved by leader " + serverApp.etcdServiceInstanceId).setReservationId(reservationId).setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                    responseBuilder.setSuccess(false).setMessage("Reservation failed on leader " + serverApp.etcdServiceInstanceId).setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }
                if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow);
                }
            }
        } // end synchronized

        if (updatedShowState != null && reservationId != null) {
            simulateReservationReplication(updatedShowState, reservationId, "ReserveTicket");
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void reserveCombo(ReserveComboRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddress = serverApp.getLeaderAddressFromZooKeeper();
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received ReserveCombo request. Not the leader. Leader might be: " + leaderAddress);
            // TODO for SYNC-003 full: Implement forwarding to leader.
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader. Combo reservations only on leader. Leader might be at: " + (leaderAddress != null ? leaderAddress : "Unknown"))
                    .asRuntimeException());
            return;
        }

        // --- Leader Node Logic ---
        // For PROJECT-003/SYNC-002, this is still a local atomic operation on the leader.
        // 2PC will be introduced in Epic 5 for how the leader coordinates this with secondaries.
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing ReserveCombo for show ID: " + showId + ", Request ID: " + request.getRequestId());
        ShowInfo updatedShowState = null;
        String reservationId = null;
        ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();

        synchronized (showsData) {
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else {
                ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
                boolean possibleToReserve = true;
                // Check concert tickets
                if (request.getConcertItemsList().isEmpty()){/* ... error ... */ possibleToReserve = false; }
                else {
                    for (ReservationItem item : request.getConcertItemsList()) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        if (tier == null) { /* ... error ... */ possibleToReserve = false; break; }
                        if (tier.getCurrentStock() < item.getQuantity()) { /* ... error ... */ possibleToReserve = false; break; }
                    }
                }
                // Check after-party tickets
                if (possibleToReserve) {
                    if (!mutableShowBuilder.getAfterPartyAvailable()) { /* ... error ... */ possibleToReserve = false; }
                    else if (mutableShowBuilder.getAfterPartyCurrentStock() < request.getAfterPartyQuantity()) { /* ... error ... */ possibleToReserve = false; }
                }
                if (possibleToReserve) {
                    // ... (perform stock decrements for concert and after-party as in previous full version)
                    for (ReservationItem item : request.getConcertItemsList()) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    mutableShowBuilder.setAfterPartyCurrentStock(mutableShowBuilder.getAfterPartyCurrentStock() - request.getAfterPartyQuantity());
                    updatedShowState = mutableShowBuilder.build();
                    showsData.put(showId, updatedShowState);
                    reservationId = "combo-res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request);
                    responseBuilder.setSuccess(true).setMessage("Combo reserved by leader " + serverApp.etcdServiceInstanceId).setReservationId(reservationId).setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                    responseBuilder.setSuccess(false).setMessage("Combo reservation failed on leader " + serverApp.etcdServiceInstanceId).setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }
                if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow);
                }
            }
        } // end synchronized

        if (updatedShowState != null && reservationId != null) {
            // In Epic 5, this is where 2PC coordination starts.
            // For now, just simulate replication of final state.
            simulateReservationReplication(updatedShowState, reservationId, "ReserveCombo");
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
