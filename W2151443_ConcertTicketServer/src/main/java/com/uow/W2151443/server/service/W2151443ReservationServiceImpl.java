package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance; // Ensure this import
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

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

    // Helper to get other live (secondary) nodes from etcd
    private List<ServiceInstance> getSecondaryNodes() { // Can be refactored to a shared utility
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

    // Replicates the full ShowInfo object to secondaries
    private void performFullShowReplicationAfterReservation(ShowInfo updatedShowInfoAfterReservation) {
        if (!serverApp.isLeader()) return;

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo state after reservation for ID: " + updatedShowInfoAfterReservation.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();

        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);
                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .replicateShowUpdate(updatedShowInfoAfterReservation); // Use the general ShowInfo update
                if (response.getSuccess()) {
                    logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Successfully replicated post-reservation state to " + secondary.getId());
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Failed to replicate post-reservation state to " + secondary.getId() + ". Reason: " + response.getMessage());
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): Error replicating post-reservation state to secondary " + secondary.getId(), e);
            } finally {
                if (channel != null) try { channel.shutdown().awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException e) {Thread.currentThread().interrupt();}
            }
        }
    }


    // --- Read Operations (served by any node) ---
    @Override
    public void browseEvents(BrowseEventsRequest request, StreamObserver<BrowseEventsResponse> responseObserver) {
        // ... (same as before, logging serverApp.etcdServiceInstanceId)
        logger.info(serverApp.etcdServiceInstanceId + ": Processing BrowseEvents request...");
        List<ShowInfo> currentShows; synchronized (showsData) { currentShows = new ArrayList<>(showsData.values());}
        if (request.hasFilterByDate() && !request.getFilterByDate().equalsIgnoreCase("ALL") && !request.getFilterByDate().isEmpty()) { String filterDate = request.getFilterByDate(); currentShows = currentShows.stream().filter(show -> show.getShowDate().startsWith(filterDate)).collect(Collectors.toList());}
        responseObserver.onNext(BrowseEventsResponse.newBuilder().addAllShows(currentShows).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getShowAvailability(GetShowAvailabilityRequest request, StreamObserver<ShowInfo> responseObserver) {
        // ... (same as before, logging serverApp.etcdServiceInstanceId)
        logger.info(serverApp.etcdServiceInstanceId + ": Processing GetShowAvailability for show ID: " + request.getShowId());
        ShowInfo show; synchronized (showsData) {show = showsData.get(request.getShowId());}
        if (show != null) { responseObserver.onNext(show); }
        else { responseObserver.onError(Status.NOT_FOUND.withDescription("Show ID " + request.getShowId() + " not found.").asRuntimeException());}
        responseObserver.onCompleted();
    }


    @Override
    public void reserveTicket(ReserveTicketRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic as previously defined) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub lStub = W2151443ReservationServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.reserveTicket(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding ReserveTicket: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }

        // --- Leader Node Logic ---
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing ReserveTicket for show ID: " + showId);
        ShowInfo updatedShowState = null; String reservationId = null; ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();
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
                        if (tier == null) {
                            responseBuilder.setSuccess(false).setMessage("Tier not found: " + item.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND);
                            possibleToReserve = false; break;
                        }
                        if (tier.getCurrentStock() < item.getQuantity()) {
                            responseBuilder.setSuccess(false).setMessage("Not enough stock for tier: " + item.getTierId()).setErrorCode(ErrorCode.OUT_OF_STOCK);
                            possibleToReserve = false; break;
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
                    showsData.put(showId, updatedShowState);
                    reservationId = "res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request); // Log locally
                    responseBuilder.setSuccess(true).setMessage("Tickets reserved by leader " + serverApp.etcdServiceInstanceId)
                            .setReservationId(reservationId).setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                    responseBuilder.setSuccess(false).setMessage("Reservation failed on leader " + serverApp.etcdServiceInstanceId).setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }
                if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow); // Send original show info on error
                }
            }
        } // end synchronized

        if (responseBuilder.getSuccess() && updatedShowState != null) {
            performFullShowReplicationAfterReservation(updatedShowState); // ACTUAL REPLICATION
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void reserveCombo(ReserveComboRequest request, StreamObserver<ReservationResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic as previously defined) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub lStub = W2151443ReservationServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.reserveCombo(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding ReserveCombo: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }

        // --- Leader Node Logic ---
        // For now, this is still locally atomic. 2PC (Epic 5) will make this distributed atomic.
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing ReserveCombo for show ID: " + showId);
        ShowInfo updatedShowState = null; String reservationId = null; ReservationResponse.Builder responseBuilder = ReservationResponse.newBuilder();
        synchronized (showsData) {
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                responseBuilder.setSuccess(false).setMessage("Show not found: " + showId).setErrorCode(ErrorCode.SHOW_NOT_FOUND);
            } else {
                ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder();
                boolean possibleToReserve = true;
                // ... (Full validation logic for concert and after-party stock as in previous version) ...
                if (request.getConcertItemsList().isEmpty()){ responseBuilder.setSuccess(false).setMessage("No concert items for combo.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA); possibleToReserve = false; }
                else { for (ReservationItem item : request.getConcertItemsList()) { TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId()); if (tier == null) {responseBuilder.setSuccess(false).setMessage("Concert tier not found: " + item.getTierId()).setErrorCode(ErrorCode.TIER_NOT_FOUND); possibleToReserve = false; break;} if (tier.getCurrentStock() < item.getQuantity()) { responseBuilder.setSuccess(false).setMessage("Not enough stock for concert tier: " + item.getTierId()).setErrorCode(ErrorCode.OUT_OF_STOCK); possibleToReserve = false; break;}}}
                if (possibleToReserve) { if (!mutableShowBuilder.getAfterPartyAvailable()) { responseBuilder.setSuccess(false).setMessage("After-party not available.").setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE); possibleToReserve = false;} else if (mutableShowBuilder.getAfterPartyCurrentStock() < request.getAfterPartyQuantity()) { responseBuilder.setSuccess(false).setMessage("Not enough stock for after-party.").setErrorCode(ErrorCode.OUT_OF_STOCK); possibleToReserve = false;}}

                if (possibleToReserve) {
                    for (ReservationItem item : request.getConcertItemsList()) {
                        TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                        TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() - item.getQuantity()).build();
                        mutableShowBuilder.putTiers(item.getTierId(), updatedTier);
                    }
                    mutableShowBuilder.setAfterPartyCurrentStock(mutableShowBuilder.getAfterPartyCurrentStock() - request.getAfterPartyQuantity());
                    updatedShowState = mutableShowBuilder.build();
                    showsData.put(showId, updatedShowState);
                    reservationId = "combo-res-W2151443-" + UUID.randomUUID().toString();
                    reservationsLog.put(reservationId, request); // Log locally
                    responseBuilder.setSuccess(true).setMessage("Combo reserved by leader " + serverApp.etcdServiceInstanceId)
                            .setReservationId(reservationId).setUpdatedShowInfo(updatedShowState);
                } else if (!responseBuilder.hasErrorCode()) {
                    responseBuilder.setSuccess(false).setMessage("Combo reservation failed on leader " + serverApp.etcdServiceInstanceId).setErrorCode(ErrorCode.INVALID_REQUEST_DATA);
                }
                if (!responseBuilder.getSuccess() && !responseBuilder.hasUpdatedShowInfo() && currentShow != null) {
                    responseBuilder.setUpdatedShowInfo(currentShow);
                }
            }
        } // end synchronized

        if (responseBuilder.getSuccess() && updatedShowState != null) {
            // Before 2PC, the leader replicates the state change.
            // After 2PC, replication happens as part of the commit phase on all nodes.
            performFullShowReplicationAfterReservation(updatedShowState); // ACTUAL REPLICATION
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
