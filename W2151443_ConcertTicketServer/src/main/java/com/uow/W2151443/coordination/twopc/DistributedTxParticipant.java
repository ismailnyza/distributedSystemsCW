// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/coordination/twopc/DistributedTxParticipant.java
package com.uow.W2151443.coordination.twopc;

import com.uow.W2151443.concert.service.ReserveComboRequest;
import com.uow.W2151443.concert.service.ReservationItem;
import com.uow.W2151443.concert.service.ShowInfo;
import com.uow.W2151443.concert.service.TierDetails;
import com.uow.W2151443.server.ConcertTicketServerApp;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

// import java.util.Map; // Not directly used
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedTxParticipant implements Watcher {
    private static final Logger logger = Logger.getLogger(DistributedTxParticipant.class.getName());

    private final String transactionId;
    private final DistributedTransaction transaction;
    private final ZooKeeper zk;
    private final ReserveComboRequest operationDetails;
    private final String participantId;
    private final ConcertTicketServerApp serverApp;
    private final DistributedTxListener txListener;

    private boolean votedCommit = false;
    private volatile boolean globallyDecided = false; // To prevent processing multiple times

    public DistributedTxParticipant(ConcertTicketServerApp serverApp,
                                    String transactionId,
                                    ReserveComboRequest operationDetails,
                                    DistributedTxListener listener) {
        this.serverApp = serverApp;
        this.zk = serverApp.getZooKeeperClient();
        this.transactionId = transactionId;
        this.transaction = new DistributedTransaction(zk, transactionId);
        this.operationDetails = operationDetails;
        this.participantId = serverApp.etcdServiceInstanceId;
        this.txListener = listener;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void prepareAndVote() {
        if (globallyDecided) {
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Transaction already globally decided. Skipping prepareAndVote.");
            return;
        }
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received PREPARE request. Performing local checks...");
        boolean canCommitLocally = canCommitOperation();

        try {
            if (canCommitLocally) {
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Local checks PASSED. Voting COMMIT.");
                transaction.submitVote(participantId, true);
                votedCommit = true;
            } else {
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Local checks FAILED. Voting ABORT.");
                transaction.submitVote(participantId, false);
                votedCommit = false;
            }
            watchGlobalTransactionState();
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, "Participant " + participantId + " (TxID: " + transactionId + "): Error submitting vote or setting watch.", e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            // If we intended to commit but failed to vote, we must assume the transaction might abort globally
            // or our vote won't be seen. To maintain local consistency, treat this as a local abort.
            if (canCommitLocally) { // If we *would* have voted COMMIT
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Failed to submit COMMIT vote due to ZK error. Triggering local abort for safety.");
                //This will also trigger cleanup via the listener in ConcertTicketServerApp
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_vote_failure");
                globallyDecided = true; // Mark as decided to prevent further processing
            } else { // Already voted abort, or would have
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_vote_failure_already_aborting");
                globallyDecided = true;
            }
        }
    }

    private boolean canCommitOperation() {
        synchronized (serverApp.W2151443_showsDataStore) {
            ShowInfo currentShow = serverApp.W2151443_showsDataStore.get(operationDetails.getShowId());
            if (currentShow == null) {
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Show " + operationDetails.getShowId() + " not found locally during prepare.");
                return false;
            }
            if (operationDetails.getConcertItemsList().isEmpty()){
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): No concert items in request for show " + operationDetails.getShowId());
                return false;
            }

            for (ReservationItem item : operationDetails.getConcertItemsList()) {
                TierDetails tier = currentShow.getTiersMap().get(item.getTierId());
                if (tier == null || tier.getCurrentStock() < item.getQuantity()) {
                    logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Insufficient stock for concert tier " + item.getTierId() + ". Requested: " + item.getQuantity() + ", Available: " + (tier != null ? tier.getCurrentStock() : "N/A"));
                    return false;
                }
            }
            if (operationDetails.getAfterPartyQuantity() > 0) {
                if (!currentShow.getAfterPartyAvailable() || currentShow.getAfterPartyCurrentStock() < operationDetails.getAfterPartyQuantity()) {
                    logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): After-party not available or insufficient stock. Requested: " + operationDetails.getAfterPartyQuantity() + ", Available: " + currentShow.getAfterPartyCurrentStock());
                    return false;
                }
            }
            return true;
        }
    }

    private void watchGlobalTransactionState() throws KeeperException, InterruptedException {
        if (globallyDecided) return;
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Watching global transaction state at " + transaction.getTransactionPath());
        Stat stat = zk.exists(transaction.getTransactionPath(), this);
        if (stat == null) {
            logger.severe("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction znode " + transaction.getTransactionPath() + " does not exist! Cannot watch. Assuming ABORT.");
            if (!globallyDecided) {
                globallyDecided = true;
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_missing_tx_node");
            }
        } else {
            // Check current state in case we missed a quick change or if it's already decided
            processGlobalStateChange(transaction.getGlobalState(true)); // Read and re-set watch
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (globallyDecided && event.getType() != Event.EventType.None) { // Allow session events through
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Transaction already globally decided. Ignoring event: " + event);
            return;
        }
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): ZooKeeper Watcher event: " + event);
        if (event.getType() == Event.EventType.NodeDataChanged && event.getPath() != null && event.getPath().equals(transaction.getTransactionPath())) {
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction state changed. Re-fetching...");
            try {
                if (!globallyDecided) { // Re-check before processing
                    DistributedTransaction.TxState globalState = transaction.getGlobalState(true);
                    processGlobalStateChange(globalState);
                }
            } catch (KeeperException | InterruptedException e) {
                logger.log(Level.SEVERE, "Participant " + participantId + " (TxID: " + transactionId + "): Error getting global transaction state after watch.", e);
                if (!globallyDecided) {
                    globallyDecided = true;
                    txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_zk_error_on_watch");
                }
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            }
        } else if (event.getState() == Event.KeeperState.Expired) {
            logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): ZooKeeper session expired for watcher. Transaction outcome uncertain. Assuming ABORT.");
            if (!globallyDecided) {
                globallyDecided = true;
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_session_expiry_watcher");
            }
        }
    }

    private void processGlobalStateChange(DistributedTransaction.TxState globalState) {
        if (globallyDecided) {
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Already processed global state " + globalState + ". Ignoring.");
            return;
        }
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Processing global state: " + globalState);
        switch (globalState) {
            case COMMITTED:
                globallyDecided = true;
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received GLOBAL_COMMIT.");
                if (votedCommit) {
                    txListener.onGlobalCommit(transactionId + "_" + participantId + "_local_op");
                } else {
                    logger.warning("Participant " + participantId + " (TxID: " + transactionId +
                            "): Received GLOBAL_COMMIT but had voted ABORT. Honoring local ABORT (no action).");
                    // If we voted abort, we should not commit locally, even if global says commit.
                    // The listener for abort might have already cleaned up any pending state.
                }
                break;
            case ABORTED:
                globallyDecided = true;
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received GLOBAL_ABORT.");
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op");
                break;
            case INIT:
            case PREPARING:
            case VOTING:
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Transaction still in progress (" + globalState + "). Continuing to watch.");
                // Re-watch if necessary, though getGlobalState(true) should handle it
                try {
                    if (zk.exists(transaction.getTransactionPath(), this) == null && !globallyDecided) { // Re-set watch if it was removed
                        logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Transaction node disappeared while in VOTING/PREPARING state. Assuming ABORT.");
                        globallyDecided = true;
                        txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_node_vanished");
                    }
                } catch (KeeperException | InterruptedException e) {
                    logger.log(Level.SEVERE, "Participant " + participantId + " (TxID: " + transactionId + "): Error re-checking/watching transaction node.", e);
                    if(!globallyDecided) {
                        globallyDecided = true;
                        txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_zk_error_rewatch");
                    }
                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                }
                break;
            case UNKNOWN:
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction state is UNKNOWN. Assuming ABORT.");
                if (!globallyDecided) {
                    globallyDecided = true;
                    txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_unknown_state");
                }
                break;
        }
    }

    public void forceCommit() {
        if (globallyDecided) {
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): forceCommit called, but already globally decided.");
            return;
        }
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received direct COMMIT instruction (forceCommit).");
        globallyDecided = true; // Mark that a global decision has been enforced externally
        if (votedCommit) {
            txListener.onGlobalCommit(transactionId + "_" + participantId + "_local_op_forced");
        } else {
            logger.warning("Participant " + participantId + " (TxID: " + transactionId +
                    "): forceCommit called, but this participant had voted ABORT. Honoring local ABORT.");
        }
    }

    public void forceAbort() {
        if (globallyDecided) {
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): forceAbort called, but already globally decided.");
            return;
        }
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received direct ABORT instruction (forceAbort).");
        globallyDecided = true; // Mark that a global decision has been enforced externally
        txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_forced");
    }
}
