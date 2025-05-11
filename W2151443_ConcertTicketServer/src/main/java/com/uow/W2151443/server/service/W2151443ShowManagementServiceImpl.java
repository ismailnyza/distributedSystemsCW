package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance; // Ensure this import is correct
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443ShowManagementServiceImpl extends W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443ShowManagementServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData;
    private final ConcertTicketServerApp serverApp;

    public W2151443ShowManagementServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    // Helper to get other live (secondary) nodes from etcd
    private List<ServiceInstance> getSecondaryNodes() {
        if (serverApp.etcdClient == null) {
            logger.warning(serverApp.etcdServiceInstanceId + " (Leader): etcdClient is null. Cannot discover secondaries for replication.");
            return new ArrayList<>();
        }
        List<ServiceInstance> allNodes = serverApp.etcdClient.discoverServiceInstances(serverApp.etcdServiceBasePath);
        List<ServiceInstance> secondaryNodes = new ArrayList<>();
        for (ServiceInstance node : allNodes) {
            if (!node.getId().equals(serverApp.etcdServiceInstanceId)) { // Exclude self (the leader)
                secondaryNodes.add(node);
            }
        }
        logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Found " + secondaryNodes.size() + " secondary node(s) for replication.");
        return secondaryNodes;
    }

    // Performs replication of the full ShowInfo object
    private void performFullShowReplication(ShowInfo showInfoToReplicate) {
        if (!serverApp.isLeader()) return; // Should only be called by leader

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Starting full replication of ShowInfo for ID: " + showInfoToReplicate.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();

        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo to secondary: " + secondary.getId() + " at " + secondary.getIp() + ":" + secondary.getPort());
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .replicateShowUpdate(showInfoToReplicate);
                if (response.getSuccess()) {
                    logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Successfully replicated ShowInfo to " + secondary.getId());
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Failed to replicate ShowInfo to " + secondary.getId() + ". Reason: " + response.getMessage());
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): Error replicating ShowInfo to secondary " + secondary.getId(), e);
            } finally {
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) { Thread.currentThread().interrupt();}
                }
            }
        }
    }

    // Could be used if we decide to replicate stock changes as deltas, but replicating full ShowInfo is often safer.
    // For now, reservation service will also call performFullShowReplication after it updates stock.
    // This method remains if direct stock updates by admins should use a more granular replication.
    private void performStockChangeReplication(UpdateStockRequest stockUpdateRequestToReplicate) {
        if (!serverApp.isLeader()) return;

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Starting replication of StockUpdate for Show ID: " + stockUpdateRequestToReplicate.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();

        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Replicating StockUpdate to secondary: " + secondary.getId() + " at " + secondary.getIp() + ":" + secondary.getPort());
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .replicateStockChange(stockUpdateRequestToReplicate);
                // Log success/failure
            } catch (Exception e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): Error replicating StockUpdate to secondary " + secondary.getId(), e);
            } finally {
                if (channel != null) try { channel.shutdown().awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt();}
            }
        }
    }


    @Override
    public void addShow(AddShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic as previously defined) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub lStub = W2151443ShowManagementServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.addShow(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding AddShow: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }

        // --- Leader Node Logic ---
        ShowInfo showInfoToAdd = request.getShowInfo();
        String showId = showInfoToAdd.getShowId();
        if (showId == null || showId.trim().isEmpty()) {
            showId = "show-W2151443-" + UUID.randomUUID().toString().substring(0, 8);
            showInfoToAdd = showInfoToAdd.toBuilder().setShowId(showId).build();
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing AddShow for ID: " + showId);
        synchronized (showsData) {
            if (showsData.containsKey(showId)) {
                responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Show ID " + showId + " already exists.").asRuntimeException());
                return;
            }
            showsData.put(showId, showInfoToAdd);
            performFullShowReplication(showInfoToAdd); // ACTUAL REPLICATION
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Show added: " + showInfoToAdd.getShowName());
        GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Show added by leader " + serverApp.etcdServiceInstanceId).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateShowDetails(UpdateShowDetailsRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub lStub = W2151443ShowManagementServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.updateShowDetails(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding UpdateShowDetails: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }
        // --- Leader Node Logic ---
        String showId = request.getShowId();
        ShowInfo updatedShow;
        synchronized (showsData) {
            ShowInfo existingShow = showsData.get(showId);
            if (existingShow == null) { /* ... error NOT_FOUND ... */ responseObserver.onError(Status.NOT_FOUND.withDescription("Show not found").asRuntimeException()); return; }
            ShowInfo.Builder builder = existingShow.toBuilder();
            if(request.hasShowName()) builder.setShowName(request.getShowName());
            // ... other fields ...
            updatedShow = builder.build();
            showsData.put(showId, updatedShow);
            performFullShowReplication(updatedShow); // ACTUAL REPLICATION
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show details updated by leader").build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateStock(UpdateStockRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub lStub = W2151443ShowManagementServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.updateStock(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding UpdateStock: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }
        // --- Leader Node Logic ---
        ShowInfo finalShowState;
        GenericResponse errorResponse = null;
        synchronized (showsData) {
            ShowInfo show = showsData.get(request.getShowId());
            if (show == null) { errorResponse = GenericResponse.newBuilder().setSuccess(false).setErrorCode(ErrorCode.SHOW_NOT_FOUND).build(); }
            else { /* ... actual stock update logic from previous version ... */
                ShowInfo.Builder showBuilder = show.toBuilder(); boolean updated = false;
                if (request.hasTierId()) { /* ... */ if (!show.getTiersMap().containsKey(request.getTierId())) errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("Tier not found").setErrorCode(ErrorCode.TIER_NOT_FOUND).build(); else { TierDetails tier = show.getTiersMap().get(request.getTierId()); TierDetails.Builder tierBuilder = tier.toBuilder(); int newStock = tier.getCurrentStock() + request.getChangeInTierStock(); if (newStock < 0) newStock = 0; tierBuilder.setCurrentStock(newStock); showBuilder.putTiers(request.getTierId(), tierBuilder.build()); updated = true;}}
                if (errorResponse == null && request.hasChangeInAfterPartyStock()) { /* ... */  if (!show.getAfterPartyAvailable()) errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("After-party not available").setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE).build(); else { int newStock = show.getAfterPartyCurrentStock() + request.getChangeInAfterPartyStock(); if (newStock < 0) newStock = 0; showBuilder.setAfterPartyCurrentStock(newStock); updated = true;}}
                if (errorResponse == null) { if (updated) { finalShowState = showBuilder.build(); showsData.put(request.getShowId(), finalShowState); performFullShowReplication(finalShowState); /* ACTUAL REPLICATION */ } else errorResponse = GenericResponse.newBuilder().setSuccess(false).setMessage("No stock update performed.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA).build(); }
            }
        }
        if(errorResponse != null) {responseObserver.onNext(errorResponse); }
        else {responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Stock updated by leader").build());}
        responseObserver.onCompleted();
    }

    @Override
    public void cancelShow(CancelShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            // ... (Forwarding logic) ...
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable").asRuntimeException()); return; }
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address").asRuntimeException()); return; }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub lStub = W2151443ShowManagementServiceGrpc.newBlockingStub(leaderChannel);
                responseObserver.onNext(lStub.cancelShow(request)); responseObserver.onCompleted();
            } catch (Exception e) { responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding CancelShow: " + e.getMessage()).asRuntimeException());
            } finally { if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
            return;
        }
        // --- Leader Node Logic ---
        String showId = request.getShowId();
        ShowInfo removedShow;
        synchronized(showsData) {
            removedShow = showsData.remove(showId);
        }
        if (removedShow != null) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): Cancelled show " + showId + ". Replicating deletion.");
            // Replicate by sending a "DELETED" marker. InternalNodeService needs to handle this.
            ShowInfo deletedMarker = ShowInfo.newBuilder().setShowId(showId).setShowName("DELETED").build(); // Key marker
            performFullShowReplication(deletedMarker); // ACTUAL REPLICATION
            responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show cancelled by leader.").build());
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Show not found for cancellation.").asRuntimeException());
        }
        responseObserver.onCompleted();
    }
}
