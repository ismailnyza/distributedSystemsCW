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

    /**
     * Allows the ConcertTicketServerApp (acting as DistributedTxListener) to clean up
     * the participant entry once a transaction is globally committed or aborted.
     * @param transactionId The ID of the transaction whose participant should be removed.
     */
    public void removeActiveParticipant(String transactionId) {
        DistributedTxParticipant removed = activeParticipants.remove(transactionId);
        if (removed != null) {
            logger.info(serverApp.etcdServiceInstanceId + ": Removed active participant for completed TxID: " + transactionId);
        } else {
            logger.fine(serverApp.etcdServiceInstanceId + ": Attempted to remove participant for TxID: " + transactionId + ", but it was not found (possibly already removed or never added).");
        }
    }


    @Override
    public void prepareTransaction(TransactionRequest request, StreamObserver<VoteResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        ReserveComboRequest comboDetails = request.getOriginalComboRequest();
        logger.info(serverApp.etcdServiceInstanceId + ": Received PREPARE_TRANSACTION request for TxID: " + transactionId + " for show: " + comboDetails.getShowId());

        // Check if this transaction is already being handled (e.g., duplicate PREPARE from leader)
        if (activeParticipants.containsKey(transactionId)) {
            logger.warning(serverApp.etcdServiceInstanceId + ": Duplicate PREPARE_TRANSACTION request for TxID: " + transactionId + ". Ignoring.");
            // Send a simple ACK that it's being handled, or a specific error if appropriate.
            // For now, let's just ACK to avoid client timeout on leader if this is a retry.
            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTransactionId(transactionId)
                    .setVoteCommit(true) // Still ACK the receipt.
                    .setNodeId(serverApp.etcdServiceInstanceId)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        DistributedTxParticipant participant = new DistributedTxParticipant(serverApp, transactionId, comboDetails, serverApp);
        activeParticipants.put(transactionId, participant);

        try {
            participant.prepareAndVote();
            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTransactionId(transactionId)
                    .setVoteCommit(true)
                    .setNodeId(serverApp.etcdServiceInstanceId)
                    .build());
            responseObserver.onCompleted();
            logger.info(serverApp.etcdServiceInstanceId + ": PREPARE_TRANSACTION processed for TxID: " + transactionId + ". Voted and watching ZK.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + ": Error during prepareAndVote for TxID: " + transactionId, e);
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
        logger.info(serverApp.etcdServiceInstanceId + ": Received direct COMMIT_TRANSACTION instruction from leader for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId); // Do not remove here yet
        if (participant != null) {
            participant.forceCommit(); // This will trigger the listener in ServerApp, which then calls removeActiveParticipant
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": No active participant found for commit instruction on TxID: " + transactionId + ". Might have already processed via ZK watch or timed out.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Commit instruction acknowledged by " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void abortTransaction(TransactionDecision request, StreamObserver<GenericResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        logger.info(serverApp.etcdServiceInstanceId + ": Received direct ABORT_TRANSACTION instruction from leader for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId); // Do not remove here yet
        if (participant != null) {
            participant.forceAbort(); // This will trigger the listener in ServerApp
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": No active participant found for abort instruction on TxID: " + transactionId + ". Might have already processed.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Abort instruction acknowledged by " + serverApp.etcdServiceInstanceId).build());
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
            logger.severe(serverApp.etcdServiceInstanceId + " (Leader): Received replicateStockChange RPC. This should be deprecated or handled carefully.");
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Leader node should not receive replicateStockChange if full ShowInfo replication is used.").asRuntimeException());
            return;
        }
        String showId = stockChangeRequest.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateStockChange for show ID: " + showId + ". (Note: Prefer full ShowInfo replication)");
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
                    // This logic should be carefully reviewed. Delta updates can be tricky.
                    // The new stock should be what the leader calculated.
                    // For simplicity, if this RPC is used, it's assumed the `changeInTierStock` is the *final* stock,
                    // or it's a delta that the leader determined.
                    // Given current design: leader sends full ShowInfo, so this is less likely used.
                    // If it IS a delta:
                    int newStock = tier.getCurrentStock() + stockChangeRequest.getChangeInTierStock();
                    TierDetails updatedTier = tier.toBuilder().setCurrentStock(newStock).build();
                    showBuilder.putTiers(stockChangeRequest.getTierId(), updatedTier);
                    changed = true;
                }
            }
            if (stockChangeRequest.hasChangeInAfterPartyStock()) {
                if(currentShow.getAfterPartyAvailable()){
                    int newAPStock = currentShow.getAfterPartyCurrentStock() + stockChangeRequest.getChangeInAfterPartyStock();
                    showBuilder.setAfterPartyCurrentStock(newAPStock);
                    changed = true;
                }
            }
            if(changed){
                showsData.put(showId, showBuilder.build());
            }
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Stock change (potentially) replicated on " + serverApp.etcdServiceInstanceId).build());
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
        synchronized (showsData) {
            responseBuilder.putAllAllShowsData(new ConcurrentHashMap<>(showsData));
        }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Sending full state with " + responseBuilder.getAllShowsDataCount() + " shows to " + request.getRequestingNodeId());
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
