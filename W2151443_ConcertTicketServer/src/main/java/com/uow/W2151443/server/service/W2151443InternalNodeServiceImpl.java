// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/server/service/W2151443InternalNodeServiceImpl.java
package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.coordination.twopc.DistributedTxParticipant;
import com.uow.W2151443.server.ConcertTicketServerApp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443InternalNodeServiceImpl extends W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceImplBase {

    private static final Logger logger = Logger.getLogger(W2151443InternalNodeServiceImpl.class.getName());
    private final Map<String, ShowInfo> showsData;
    private final ConcertTicketServerApp serverApp;
    private final Map<String, DistributedTxParticipant> activeParticipants = new ConcurrentHashMap<>();


    public W2151443InternalNodeServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    @Override
    public void prepareTransaction(TransactionRequest request, StreamObserver<VoteResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        ReserveComboRequest comboDetails = request.getOriginalComboRequest();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Received PREPARE_TRANSACTION request for TxID: " + transactionId + " for show: " + comboDetails.getShowId());

        // The serverApp itself is the DistributedTxListener for actions on its own data
        DistributedTxParticipant participant = new DistributedTxParticipant(serverApp, transactionId, comboDetails, serverApp);
        activeParticipants.put(transactionId, participant);

        try {
            participant.prepareAndVote(); // This will do local checks and write vote to ZooKeeper
            // The VoteResponse from the participant to the coordinator is just an ACK.
            // The actual vote (COMMIT/ABORT) is written to ZooKeeper by the participant.
            // The coordinator reads these votes from ZooKeeper.
            // So, this RPC response doesn't need to convey the actual vote decision.
            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTransactionId(transactionId)
                    .setVoteCommit(true) // Indicating prepare was received and processing initiated.
                    .setNodeId(serverApp.etcdServiceInstanceId)
                    .build());
            responseObserver.onCompleted();
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): PREPARE_TRANSACTION processed for TxID: " + transactionId + ". Voted and watching ZK.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + " (Secondary): Error during prepareAndVote for TxID: " + transactionId, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to process prepare phase: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
            activeParticipants.remove(transactionId);
        }
    }

    @Override
    public void commitTransaction(TransactionDecision request, StreamObserver<GenericResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Received direct COMMIT_TRANSACTION instruction from leader for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId);
        if (participant != null) {
            participant.forceCommit(); // Trigger local commit logic via the listener
            activeParticipants.remove(transactionId); // Clean up
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Local commit processed for TxID: " + transactionId);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): No active participant found for commit instruction on TxID: " + transactionId + ". Might have already processed via ZK watch or timed out.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Commit instruction processed by " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void abortTransaction(TransactionDecision request, StreamObserver<GenericResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Received direct ABORT_TRANSACTION instruction from leader for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId);
        if (participant != null) {
            participant.forceAbort(); // Trigger local abort logic via the listener
            activeParticipants.remove(transactionId); // Clean up
            logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Local abort processed for TxID: " + transactionId);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): No active participant found for abort instruction on TxID: " + transactionId + ". Might have already processed.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Abort instruction processed by " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void replicateShowUpdate(ShowInfo replicatedShowInfo, StreamObserver<GenericResponse> responseObserver) {
        if (serverApp.isLeader()) {
            logger.severe(serverApp.etcdServiceInstanceId + " (Leader): Received replicateShowUpdate RPC. This should only be called on secondaries.");
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Leader node should not receive replicateShowUpdate.").asRuntimeException());
            return;
        }
        String showId = replicatedShowInfo.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateShowUpdate for Show ID: " + showId + ", Name: " + replicatedShowInfo.getShowName());
        synchronized (showsData) {
            if ("DELETED".equals(replicatedShowInfo.getShowName())) {
                // This is a marker for deletion
                ShowInfo removed = showsData.remove(showId);
                if (removed != null) {
                    logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Successfully processed DELETED marker for Show ID: " + showId);
                } else {
                    logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Received DELETED marker for Show ID: " + showId + ", but show was not found locally.");
                }
            } else {
                showsData.put(showId, replicatedShowInfo);
                logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Show data updated/added for Show ID: " + showId);
            }
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show data replicated on " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void replicateStockChange(UpdateStockRequest stockChangeRequest, StreamObserver<GenericResponse> responseObserver) {
        if (serverApp.isLeader()) {
            logger.severe(serverApp.etcdServiceInstanceId + " (Leader): Received replicateStockChange RPC. This should ideally be deprecated or handled carefully.");
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Leader node should not receive replicateStockChange if full ShowInfo replication is used.").asRuntimeException());
            return;
        }
        // This method is likely less used if full ShowInfo is replicated upon every change.
        // If it were to be used, it needs careful delta application.
        String showId = stockChangeRequest.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateStockChange for show ID: " + showId + ". (Note: This method might be deprecated in favor of full ShowInfo replication)");
        synchronized (showsData) {
            ShowInfo currentShow = showsData.get(showId);
            if (currentShow == null) {
                logger.warning(serverApp.etcdServiceInstanceId + " (Secondary): Show " + showId + " not found for ReplicateStockChange.");
                responseObserver.onNext(GenericResponse.newBuilder().setSuccess(false).setMessage("Show not found on secondary for stock change.").setErrorCode(ErrorCode.SHOW_NOT_FOUND).build());
                responseObserver.onCompleted();
                return;
            }
            ShowInfo.Builder showBuilder = currentShow.toBuilder();
            boolean changed = false;
            if (stockChangeRequest.hasTierId() && stockChangeRequest.hasChangeInTierStock()) {
                TierDetails tier = currentShow.getTiersMap().get(stockChangeRequest.getTierId());
                if (tier != null) {
                    TierDetails updatedTier = tier.toBuilder().setCurrentStock(tier.getCurrentStock() + stockChangeRequest.getChangeInTierStock()).build();
                    showBuilder.putTiers(stockChangeRequest.getTierId(), updatedTier);
                    changed = true;
                }
            }
            if (stockChangeRequest.hasChangeInAfterPartyStock()) {
                if(currentShow.getAfterPartyAvailable()){
                    showBuilder.setAfterPartyCurrentStock(currentShow.getAfterPartyCurrentStock() + stockChangeRequest.getChangeInAfterPartyStock());
                    changed = true;
                }
            }
            if(changed){
                showsData.put(showId, showBuilder.build());
            }
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Stock change replicated on " + serverApp.etcdServiceInstanceId + " (if applicable).").build());
        responseObserver.onCompleted();
    }

    @Override
    public void getFullState(GetFullStateRequest request, StreamObserver<FullStateResponse> responseObserver) {
        if (!serverApp.isLeader()) {
            logger.severe(serverApp.etcdServiceInstanceId + " (Secondary): Received GetFullState request. This should only be handled by the leader.");
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Only leader node can provide full state.").asRuntimeException());
            return;
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Received GetFullState request from: " + request.getRequestingNodeId());
        FullStateResponse.Builder responseBuilder = FullStateResponse.newBuilder();
        synchronized (showsData) { // Ensure consistent read of the map
            responseBuilder.putAllAllShowsData(new ConcurrentHashMap<>(showsData)); // Send a copy
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Sending full state with " + responseBuilder.getAllShowsDataCount() + " shows to " + request.getRequestingNodeId());
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
