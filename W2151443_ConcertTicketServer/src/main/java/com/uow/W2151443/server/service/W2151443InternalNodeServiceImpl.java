package com.uow.W2151443.server.service;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.coordination.twopc.DistributedTxParticipant; // Import
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
    // Store active participants for this node, keyed by transactionId
    private final Map<String, DistributedTxParticipant> activeParticipants = new ConcurrentHashMap<>();


    public W2151443InternalNodeServiceImpl(Map<String, ShowInfo> showsDataStore, ConcertTicketServerApp serverAppInstance) {
        this.showsData = showsDataStore;
        this.serverApp = serverAppInstance;
    }

    @Override
    public void prepareTransaction(TransactionRequest request, StreamObserver<VoteResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        ReserveComboRequest comboDetails = request.getOriginalComboRequest();
        logger.info(serverApp.etcdServiceInstanceId + ": Received PREPARE_TRANSACTION request for TxID: " + transactionId);

        // The serverApp itself is the DistributedTxListener
        DistributedTxParticipant participant = new DistributedTxParticipant(serverApp, transactionId, comboDetails, serverApp);
        activeParticipants.put(transactionId, participant); // Track active participant

        try {
            participant.prepareAndVote(); // This will do local checks and write vote to ZooKeeper
            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTransactionId(transactionId)
                    .setVoteCommit(true) // This is just an ACK that prepare was received and initiated
                    .setNodeId(serverApp.etcdServiceInstanceId)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.log(Level.SEVERE, serverApp.etcdServiceInstanceId + ": Error during prepareAndVote for TxID: " + transactionId, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to process prepare phase: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
            activeParticipants.remove(transactionId); // Clean up on error
        }
    }

    @Override
    public void commitTransaction(TransactionDecision request, StreamObserver<GenericResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        logger.info(serverApp.etcdServiceInstanceId + ": Received direct COMMIT_TRANSACTION instruction for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId);
        if (participant != null) {
            participant.forceCommit(); // Tell the participant logic to process commit
            // The listener in serverApp will do the actual data change
            activeParticipants.remove(transactionId); // Clean up after processing
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": No active participant found for commit instruction on TxID: " + transactionId + ". Might have already processed via ZK watch or timed out.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Commit instruction processed").build());
        responseObserver.onCompleted();
    }

    @Override
    public void abortTransaction(TransactionDecision request, StreamObserver<GenericResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        logger.info(serverApp.etcdServiceInstanceId + ": Received direct ABORT_TRANSACTION instruction for TxID: " + transactionId);
        DistributedTxParticipant participant = activeParticipants.get(transactionId);
        if (participant != null) {
            participant.forceAbort();
            activeParticipants.remove(transactionId);
        } else {
            logger.warning(serverApp.etcdServiceInstanceId + ": No active participant found for abort instruction on TxID: " + transactionId + ". Might have already processed.");
        }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Abort instruction processed").build());
        responseObserver.onCompleted();
    }

    // replicateShowUpdate and replicateStockChange, getFullState remain largely the same as before
    // ... (copy them from the previous "File 1: W2151443InternalNodeServiceImpl.java (Server-Side - For Secondaries to Receive Updates)")
    @Override
    public void replicateShowUpdate(ShowInfo replicatedShowInfo, StreamObserver<GenericResponse> responseObserver) {
        // ... (same as previous version)
        if (serverApp.isLeader()) { /* ... error ... */ return; }
        String showId = replicatedShowInfo.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateShowUpdate for Show ID: " + showId);
        synchronized (showsData) { if ("DELETED".equals(replicatedShowInfo.getShowName()) && replicatedShowInfo.getTiersMap().isEmpty()) { showsData.remove(showId); } else { showsData.put(showId, replicatedShowInfo); }}
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Show data replicated on " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void replicateStockChange(UpdateStockRequest stockChangeRequest, StreamObserver<GenericResponse> responseObserver) {
        // ... (same as previous version - though likely deprecated if full ShowInfo is replicated)
         if (serverApp.isLeader()) { /* ... error ... */ return; }
        String showId = stockChangeRequest.getShowId();
        logger.info(serverApp.etcdServiceInstanceId + " (Secondary): Applying ReplicateStockChange for show ID: " + showId);
        synchronized (showsData) { /* ... logic to apply deltas ... */ }
        responseObserver.onNext(GenericResponse.newBuilder().setSuccess(true).setMessage("Stock change replicated on " + serverApp.etcdServiceInstanceId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getFullState(GetFullStateRequest request, StreamObserver<FullStateResponse> responseObserver) {
        // ... (same as previous version)
        if (!serverApp.isLeader()) { /* ... error ... */ return; }
        logger.info(serverApp.etcdServiceInstanceId + " (Leader): Received GetFullState request from: " + request.getRequestingNodeId());
        FullStateResponse.Builder responseBuilder = FullStateResponse.newBuilder();
        synchronized (showsData) { responseBuilder.putAllAllShowsData(new ConcurrentHashMap<>(showsData)); }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
