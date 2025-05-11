package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443InternalNodeServiceImpl extends W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443InternalNodeServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData; // Shared data store
    private final ConcertTicketServerApp serverApp;

    public W2151443InternalNodeServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    @Override
    public void replicateShowUpdate(ShowInfo replicatedShowInfo, StreamObserver<GenericResponse> responseObserver) {
        if (serverApp.isLeader()) {
            String message = serverApp.etcdServiceInstanceId + " (Leader): Received ReplicateShowUpdate call for show '" +
                    replicatedShowInfo.getShowId() + "', but I am the leader. This is unexpected from another leader.";
            logger.warning(message);
            responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false).setMessage(message).build());
            responseObserver.onCompleted();
            return;
        }

        String showId = replicatedShowInfo.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateShowUpdate for Show ID: " +
                showId + ", Name: " + replicatedShowInfo.getShowName());

        synchronized (showsData) {
            // If the show name is "DELETED" (our marker from leader's cancelShow), then remove it.
            if ("DELETED".equals(replicatedShowInfo.getShowName()) && replicatedShowInfo.getTiersMap().isEmpty()) {
                showsData.remove(showId);
                logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Show ID " + showId + " removed due to replication of deletion.");
            } else {
                showsData.put(showId, replicatedShowInfo); // Add or overwrite
            }
        }

        logger.fine(serverApp.etcdServiceInstanceId + " (Secondary): Successfully applied ReplicateShowUpdate for Show ID: " + showId);
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true)
                .setMessage("Show data for " + showId + " replicated successfully on " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void replicateStockChange(UpdateStockRequest stockChangeRequest, StreamObserver<GenericResponse> responseObserver) {
        // This method might be deprecated if leader always sends full ShowInfo on stock changes.
        // However, if used, it means the leader has sent deltas.
        if (serverApp.isLeader()) {
            logger.warning(serverApp.etcdServiceInstanceId + " (Leader): Received ReplicateStockChange call. Unexpected.");
            responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false).setMessage("Leader node should not receive replicate stock change calls.").build());
            responseObserver.onCompleted();
            return;
        }

        String showId = stockChangeRequest.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateStockChange for show ID: " + showId);

        synchronized (showsData) {
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                logger.severe(serverApp.etcdServiceInstanceId + " (Secondary): Show ID " + showId +
                        " not found locally during ReplicateStockChange. Data might be inconsistent. Requesting full update for this show might be needed.");
                responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false)
                        .setMessage("Show not found locally on secondary " + serverApp.etcdServiceInstanceId)
                        .setErrorCode(ErrorCode.SHOW_NOT_FOUND).build());
                responseObserver.onCompleted();
                return;
            }

            ShowInfo.Builder showBuilder = currentShow.toBuilder();
            boolean updated = false;

            if (stockChangeRequest.hasTierId()) {
                TierDetails tier = currentShow.getTiersMap().get(stockChangeRequest.getTierId());
                if (tier == null) {
                    responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false)
                            .setMessage("Tier not found: " + stockChangeRequest.getTierId() + " on secondary " + serverApp.etcdServiceInstanceId)
                            .setErrorCode(ErrorCode.TIER_NOT_FOUND).build());
                    responseObserver.onCompleted();
                    return;
                }
                TierDetails.Builder tierBuilder = tier.toBuilder();
                // Applying DELTA as per current UpdateStockRequest structure
                int newStock = tier.getCurrentStock() + stockChangeRequest.getChangeInTierStock();
                tierBuilder.setCurrentStock(newStock); // Leader should ensure this is valid
                showBuilder.putTiers(stockChangeRequest.getTierId(), tierBuilder.build());
                updated = true;
            }

            if (stockChangeRequest.hasChangeInAfterPartyStock()) {
                if (!currentShow.getAfterPartyAvailable()) {
                    responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false)
                            .setMessage("After-party not available on secondary " + serverApp.etcdServiceInstanceId)
                            .setErrorCode(ErrorCode.AFTER_PARTY_NOT_AVAILABLE).build());
                    responseObserver.onCompleted();
                    return;
                }
                int newStock = currentShow.getAfterPartyCurrentStock() + stockChangeRequest.getChangeInAfterPartyStock();
                showBuilder.setAfterPartyCurrentStock(newStock); // Leader should ensure this is valid
                updated = true;
            }

            if (updated) {
                showsData.put(showId, showBuilder.build());
                logger.fine(serverApp.etcdServiceInstanceId + " (Secondary): Successfully applied ReplicateStockChange for Show ID: " + showId);
                responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true)
                        .setMessage("Stock change for " + showId + " replicated on " + serverApp.etcdServiceInstanceId).build());
            } else {
                responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false)
                        .setMessage("No effective stock change in replication message for " + showId).build());
            }
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getFullState(GetFullStateRequest request, StreamObserver<FullStateResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received GetFullState request. Not the leader. Cannot serve.");
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This node (" + serverApp.getServerAddress() + ") is not the leader and cannot provide the full state.")
                    .asRuntimeException());
            return;
        }

        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Received GetFullState request from node: " + request.getRequestingNodeId());
        FullStateResponse.Builder responseBuilder = FullStateResponse.newBuilder();
        synchronized (showsData) {
            // Create a defensive copy for thread safety during serialization
            responseBuilder.putAllAllShowsData(new ConcurrentHashMap<>(showsData));
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Sent full state to node: " + request.getRequestingNodeId());
    }
}
