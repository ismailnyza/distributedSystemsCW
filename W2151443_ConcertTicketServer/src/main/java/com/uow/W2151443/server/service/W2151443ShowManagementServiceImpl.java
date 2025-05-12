// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/server/service/W2151443ShowManagementServiceImpl.java
package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance;
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

    private List<ServiceInstance> getSecondaryNodes() {
        if (serverApp.etcdClient == null) {
            logger.warning(serverApp.etcdServiceInstanceId + " (Leader): etcdClient is null. Cannot discover secondaries for replication.");
            return new ArrayList<>();
        }
        List<ServiceInstance> allNodes = serverApp.etcdClient.discoverServiceInstances(serverApp.etcdServiceBasePath);
        List<ServiceInstance> secondaryNodes = new ArrayList<>();
        for (ServiceInstance node : allNodes) {
            if (!node.getId().equals(serverApp.etcdServiceInstanceId)) {
                secondaryNodes.add(node);
            }
        }
        logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Found " + secondaryNodes.size() + " secondary node(s) for replication: " + secondaryNodes);
        return secondaryNodes;
    }

    private void performFullShowReplication(ShowInfo showInfoToReplicate) {
        if (!serverApp.isLeader()) {
            logger.warning(serverApp.etcdServiceInstanceId + ": Non-leader attempting to perform replication. Aborting.");
            return;
        }

        String operationType = "DELETED".equals(showInfoToReplicate.getShowName()) ? "deletion" : "update/creation";
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Starting full replication of ShowInfo " + operationType + " for ID: " + showInfoToReplicate.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();

        if (secondaries.isEmpty()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): No secondary nodes found to replicate " + operationType + " of ShowInfo for ID: " + showInfoToReplicate.getShowId());
            return;
        }

        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Replicating ShowInfo ("+operationType+") to secondary: " + secondary.getId() + " at " + secondary.getIp() + ":" + secondary.getPort());
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS) // Timeout for replication call
                        .replicateShowUpdate(showInfoToReplicate);

                if (response.getSuccess()) {
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): Successfully replicated ShowInfo ("+operationType+") to " + secondary.getId());
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Failed to replicate ShowInfo ("+operationType+") to " + secondary.getId() + ". Reason: " + response.getMessage());
                }
            } catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): StatusRuntimeException while replicating ShowInfo ("+operationType+") to secondary " + secondary.getId() + " ("+secondary.getIp()+":"+secondary.getPort()+"). Status: " + e.getStatus(), e);
            } catch (Exception e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): General Exception while replicating ShowInfo ("+operationType+") to secondary " + secondary.getId() + " ("+secondary.getIp()+":"+secondary.getPort()+")", e);
            } finally {
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warning("Interrupted while shutting down channel to " + secondary.getId());
                    }
                }
            }
        }
    }

    // UpdateStock specific replication, might be deprecated if full show replication is always used.
    private void performStockChangeReplication(UpdateStockRequest stockUpdateRequestToReplicate) {
        if (!serverApp.isLeader()) {
            logger.warning(serverApp.etcdServiceInstanceId + ": Non-leader attempting to perform stock change replication. Aborting.");
            return;
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Starting replication of StockUpdate for Show ID: " + stockUpdateRequestToReplicate.getShowId());
        List<ServiceInstance> secondaries = getSecondaryNodes();
        if (secondaries.isEmpty()) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): No secondary nodes found to replicate StockUpdate for Show ID: " + stockUpdateRequestToReplicate.getShowId());
            return;
        }
        for (ServiceInstance secondary : secondaries) {
            ManagedChannel channel = null;
            try {
                logger.fine(serverApp.etcdServiceInstanceId + " (Leader): Replicating StockUpdate to secondary: " + secondary.getId() + " at " + secondary.getIp() + ":" + secondary.getPort());
                channel = ManagedChannelBuilder.forAddress(secondary.getIp(), secondary.getPort()).usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);
                GenericResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .replicateStockChange(stockUpdateRequestToReplicate);
                if (response.getSuccess()) {
                    logger.info(serverApp.etcdServiceInstanceId + " (Leader): Successfully replicated StockUpdate to " + secondary.getId());
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Failed to replicate StockUpdate to " + secondary.getId() + ". Reason: " + response.getMessage());
                }
            } catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, serverApp.etcdServiceInstanceId + " (Leader): StatusRuntimeException while replicating StockUpdate to secondary " + secondary.getId() , e);
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
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Leader address not available. Cannot forward AddShow.");
                responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable to forward AddShow request.").asRuntimeException());
                return;
            }
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Not the leader. Forwarding AddShow request to leader at " + leaderAddressString);
            String[] parts = leaderAddressString.split(":");
            if (parts.length != 2) {
                responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address format from ZooKeeper: " + leaderAddressString).asRuntimeException());
                return;
            }
            ManagedChannel leaderChannel = null;
            try {
                leaderChannel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1])).usePlaintext().build();
                W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub leaderStub = W2151443ShowManagementServiceGrpc.newBlockingStub(leaderChannel);
                GenericResponse leaderResponse = leaderStub.addShow(request);
                responseObserver.onNext(leaderResponse);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Secondary): Error forwarding AddShow to leader.", e);
                responseObserver.onError(Status.INTERNAL.withCause(e).withDescription("Error forwarding AddShow to leader: " + e.getMessage()).asRuntimeException());
            } finally {
                if (leaderChannel != null) try {leaderChannel.shutdown().awaitTermination(1, TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt(); logger.warning("Interrupted while shutting down leader channel for AddShow forwarding.");}
            }
            return;
        }

        // --- Leader Node Logic ---
        ShowInfo showInfoFromRequest = request.getShowInfo();
        String showId = showInfoFromRequest.getShowId();
        if (showId == null || showId.trim().isEmpty()) {
            showId = "show-W2151443-" + UUID.randomUUID().toString().substring(0, 8);
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): ShowId not provided for AddShow. Generated new ID: " + showId);
        }
        // Ensure the ShowInfo object used for storage and replication has the final showId
        ShowInfo showInfoToAdd = showInfoFromRequest.toBuilder().setShowId(showId).build();

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing AddShow for ID: " + showId + ", Name: " + showInfoToAdd.getShowName());
        synchronized (showsData) {
            if (showsData.containsKey(showId)) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): AddShow failed. Show ID " + showId + " already exists.");
                responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Show ID " + showId + " already exists.").asRuntimeException());
                return;
            }
            showsData.put(showId, showInfoToAdd);
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): Show added locally: " + showInfoToAdd.getShowName() + " (ID: " + showId + "). Initiating replication.");
            performFullShowReplication(showInfoToAdd);
        }
        GenericResponse response = GenericResponse.newBuilder().setSuccess(true).setMessage("Show added successfully by leader " + serverApp.etcdServiceInstanceId).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateShowDetails(UpdateShowDetailsRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { /* ... error ... */ responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable for UpdateShowDetails.").asRuntimeException()); return; }
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Forwarding UpdateShowDetails to leader " + leaderAddressString);
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { /* ... error ... */ responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address for UpdateShowDetails").asRuntimeException()); return; }
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
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing UpdateShowDetails for ID: " + showId);
        ShowInfo updatedShow;
        synchronized (showsData) {
            ShowInfo existingShow = showsData.get(showId);
            if (existingShow == null) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateShowDetails failed. Show ID " + showId + " not found.");
                responseObserver.onError(Status.NOT_FOUND.withDescription("Show ID " + showId + " not found.").asRuntimeException());
                return;
            }
            ShowInfo.Builder builder = existingShow.toBuilder();
            if(request.hasShowName()) builder.setShowName(request.getShowName());
            if(request.hasShowDate()) builder.setShowDate(request.getShowDate());
            if(request.hasVenue()) builder.setVenue(request.getVenue());
            if(request.hasDescription()) builder.setDescription(request.getDescription());
            // Note: This RPC doesn't update tiers or after-party details directly. That's via UpdateStock or AddShow.
            updatedShow = builder.build();
            showsData.put(showId, updatedShow);
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): Show details updated locally for ID: " + showId + ". Initiating replication.");
            performFullShowReplication(updatedShow);
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show details updated by leader " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateStock(UpdateStockRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { /* ... error ... */ responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable for UpdateStock.").asRuntimeException()); return; }
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Forwarding UpdateStock to leader " + leaderAddressString);
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { /* ... error ... */ responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address for UpdateStock").asRuntimeException()); return; }
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
        String showId = request.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing UpdateStock for Show ID: " + showId + ", Request ID: " + request.getRequestId());
        ShowInfo finalShowState = null;
        GenericResponse.Builder errorResponseBuilder = GenericResponse.newBuilder().setSuccess(false); // Prepare for potential error

        synchronized (showsData) {
            ShowInfo show = showsData.get(showId);
            if (show == null) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock failed. Show ID " + showId + " not found.");
                responseObserver.onNext(errorResponseBuilder.setMessage("Show ID " + showId + " not found.").setErrorCode(ErrorCode.SHOW_NOT_FOUND).build());
                responseObserver.onCompleted();
                return;
            }

            ShowInfo.Builder showBuilder = show.toBuilder();
            boolean stockChanged = false;

            if (request.hasTierId() && request.hasChangeInTierStock()) {
                TierDetails tier = show.getTiersMap().get(request.getTierId());
                if (tier == null) {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock failed. Tier ID " + request.getTierId() + " not found for show " + showId);
                    responseObserver.onNext(errorResponseBuilder.setMessage("Tier ID " + request.getTierId() + " not found.").setErrorCode(ErrorCode.TIER_NOT_FOUND).build());
                    responseObserver.onCompleted();
                    return;
                }
                TierDetails.Builder tierBuilder = tier.toBuilder();
                int newStock = tier.getCurrentStock() + request.getChangeInTierStock();
                if (newStock < 0) { // Prevent negative stock from admin updates
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock for tier " + request.getTierId() + " would result in negative stock. Setting to 0.");
                    newStock = 0;
                }
                // Admin can also increase total stock if needed, but this RPC implies current_stock change.
                // To change total_stock, a more specific mechanism or updateShowDetails might be better.
                // For now, just update current_stock, ensuring it doesn't exceed total_stock if that's a constraint.
                // If newStock > tier.getTotalStock(), it could be capped or logged as an admin override.
                // For simplicity, we allow current_stock to be set, assuming admin knows what they're doing.
                tierBuilder.setCurrentStock(newStock);
                showBuilder.putTiers(request.getTierId(), tierBuilder.build());
                stockChanged = true;
                logger.info(serverApp.etcdServiceInstanceId + " (Leader): Tier " + request.getTierId() + " stock updated to " + newStock);
            }

            if (request.hasChangeInAfterPartyStock()) {
                if (!show.getAfterPartyAvailable()) {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock failed. After-party not available for show " + showId);
                    responseObserver.onNext(errorResponseBuilder.setMessage("After-party not available for this show.").setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE).build());
                    responseObserver.onCompleted();
                    return;
                }
                int newAPStock = show.getAfterPartyCurrentStock() + request.getChangeInAfterPartyStock();
                if (newAPStock < 0) {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock for after-party would result in negative stock. Setting to 0.");
                    newAPStock = 0;
                }
                // Similar to tier stock, allowing admin to set current_stock, potentially > total if needed temporarily
                showBuilder.setAfterPartyCurrentStock(newAPStock);
                stockChanged = true;
                logger.info(serverApp.etcdServiceInstanceId + " (Leader): After-party stock updated to " + newAPStock);
            }

            if (stockChanged) {
                finalShowState = showBuilder.build();
                showsData.put(showId, finalShowState);
                logger.info(serverApp.etcdServiceInstanceId + " (Leader): Stock updated locally for show ID: " + showId + ". Initiating replication.");
                performFullShowReplication(finalShowState);
                responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Stock updated successfully by leader " + serverApp.etcdServiceInstanceId).build());
            } else {
                logger.warning(serverApp.etcdServiceInstanceId + " (Leader): UpdateStock called but no actual stock change parameters were provided or applicable for show ID: " + showId);
                responseObserver.onNext(errorResponseBuilder.setMessage("No valid stock update parameters provided.").setErrorCode(ErrorCode.INVALID_REQUEST_DATA).build());
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public void cancelShow(CancelShowRequest request, StreamObserver<GenericResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            String leaderAddressString = serverApp.getLeaderAddressFromZooKeeper();
            if (leaderAddressString == null || leaderAddressString.isEmpty()) { /* ... error ... */ responseObserver.onError(Status.UNAVAILABLE.withDescription("Leader unavailable for CancelShow.").asRuntimeException()); return; }
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Forwarding CancelShow to leader " + leaderAddressString);
            String[] parts = leaderAddressString.split(":"); if (parts.length != 2) { /* ... error ... */ responseObserver.onError(Status.INTERNAL.withDescription("Invalid leader address for CancelShow.").asRuntimeException()); return; }
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
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Processing CancelShow for ID: " + showId);
        ShowInfo removedShow;
        synchronized(showsData) {
            removedShow = showsData.remove(showId);
        }

        if (removedShow != null) {
            logger.info(serverApp.etcdServiceInstanceId + " (Leader): Show " + showId + " cancelled locally. Name was: " + removedShow.getShowName() + ". Initiating replication of deletion.");
            // Replicate by sending a "DELETED" marker.
            // Create a minimal ShowInfo object that only contains the showId and the deletion marker.
            ShowInfo deletedMarker = ShowInfo.newBuilder().setShowId(showId).setShowName("DELETED").build();
            performFullShowReplication(deletedMarker);
            responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show cancelled successfully by leader " + serverApp.etcdServiceInstanceId).build());
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + " (Leader): CancelShow failed. Show ID " + showId + " not found.");
            responseObserver.onError(Status.NOT_FOUND.withDescription("Show ID " + showId + " not found for cancellation.").asRuntimeException());
        }
        responseObserver.onCompleted();
    }
}
