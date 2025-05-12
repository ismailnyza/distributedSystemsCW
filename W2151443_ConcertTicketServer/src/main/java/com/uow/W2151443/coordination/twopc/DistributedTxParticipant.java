package com.uow.W2151443.coordination.twopc;

import com.uow.W2151443.concert.service.ReserveComboRequest;
import com.uow.W2151443.concert.service.ReservationItem;
import com.uow.W2151443.concert.service.ShowInfo;
import com.uow.W2151443.concert.service.TierDetails;
import com.uow.W2151443.server.ConcertTicketServerApp; // To access local data store and its own ID

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedTxParticipant implements Watcher {
    private static final Logger logger = Logger.getLogger(DistributedTxParticipant.class.getName());

    private final String transactionId;
    private final DistributedTransaction transaction;
    private final ZooKeeper zk;
    private final ReserveComboRequest operationDetails;
    private final String participantId; // This node's unique ID (e.g., serverApp.etcdServiceInstanceId)
    private final ConcertTicketServerApp serverApp; // To access local data and listener
    private final DistributedTxListener txListener; // The listener to call on commit/abort

    private boolean votedCommit = false;

    public DistributedTxParticipant(ConcertTicketServerApp serverApp,
                                    String transactionId,
                                    ReserveComboRequest operationDetails,
                                    DistributedTxListener listener) {
        this.serverApp = serverApp;
        this.zk = serverApp.getZooKeeperClient(); // Assuming ConcertTicketServerApp provides this
        this.transactionId = transactionId;
        this.transaction = new DistributedTransaction(zk, transactionId);
        this.operationDetails = operationDetails;
        this.participantId = serverApp.etcdServiceInstanceId; // This server's unique ID
        this.txListener = listener;
    }

    /**
     * Called when a "prepare" (vote request) is received.
     * Performs local checks and votes COMMIT or ABORT.
     */
    public void prepareAndVote() {
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
                votedCommit = false; // Explicitly
            }
            // After voting, start watching the global transaction znode for the outcome
            watchGlobalTransactionState();
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, "Participant " + participantId + " (TxID: " + transactionId + "): Error submitting vote or watching global state.", e);
            // If vote submission fails, it's effectively an ABORT or timeout from coordinator's perspective
            // Consider how to handle this; might need to try to clean up its vote znode or signal error
        }
    }

    /**
     * Checks if the local node can commit the operation described in operationDetails.
     * @return true if can commit, false otherwise.
     */
    private boolean canCommitOperation() {
        // Access this node's local data store (showsData) via serverApp
        synchronized (serverApp.W2151443_showsDataStore) {
            ShowInfo currentShow = serverApp.W2151443_showsDataStore.get(operationDetails.getShowId());
            if (currentShow == null) {
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Show " + operationDetails.getShowId() + " not found locally.");
                return false; // Cannot commit if show doesn't exist
            }

            // Check concert ticket stock
            for (ReservationItem item : operationDetails.getConcertItemsList()) {
                TierDetails tier = currentShow.getTiersMap().get(item.getTierId());
                if (tier == null || tier.getCurrentStock() < item.getQuantity()) {
                    logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Insufficient stock for concert tier " + item.getTierId());
                    return false; // Cannot commit
                }
            }

            // Check after-party ticket stock
            if (!currentShow.getAfterPartyAvailable() || currentShow.getAfterPartyCurrentStock() < operationDetails.getAfterPartyQuantity()) {
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): After-party not available or insufficient stock.");
                return false; // Cannot commit
            }
            return true; // All checks passed
        }
    }

    private void watchGlobalTransactionState() throws KeeperException, InterruptedException {
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Watching global transaction state at " + transaction.getTransactionPath());
        // Set a watch on the main transaction znode. The process() method will be called when it changes.
        Stat stat = zk.exists(transaction.getTransactionPath(), this); // 'this' is the Watcher
        if (stat == null) {
            // This is problematic - the transaction znode should have been created by the coordinator.
            logger.severe("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction znode " + transaction.getTransactionPath() + " does not exist! Cannot watch.");
            // This might imply an ABORT or a serious issue.
            txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_missing_tx_node");
        } else {
            // Check current state in case we missed a quick change or if it's already decided
            processGlobalStateChange(transaction.getGlobalState(true)); // Read and re-set watch
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): ZooKeeper Watcher event: " + event);
        if (event.getType() == Event.EventType.NodeDataChanged && event.getPath() != null && event.getPath().equals(transaction.getTransactionPath())) {
            // Global transaction znode data changed! This is the coordinator's decision.
            logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction state changed. Re-fetching...");
            try {
                DistributedTransaction.TxState globalState = transaction.getGlobalState(true); // Re-fetch and re-watch
                processGlobalStateChange(globalState);
            } catch (KeeperException | InterruptedException e) {
                logger.log(Level.SEVERE, "Participant " + participantId + " (TxID: " + transactionId + "): Error getting global transaction state after watch.", e);
                // Fallback: assume ABORT on error to be safe
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_zk_error");
            }
        } else if (event.getState() == Event.KeeperState.Expired) {
            logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): ZooKeeper session expired. Transaction outcome uncertain from this participant's perspective. Assuming ABORT.");
            txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_session_expiry");
            // Further recovery/reconnect logic for ZK would be needed for the app in general.
        }
        // Other events (disconnected, etc.) should be handled by the main ZooKeeper connection management in ConcertTicketServerApp
    }

    private void processGlobalStateChange(DistributedTransaction.TxState globalState) {
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Processing global state: " + globalState);
        switch (globalState) {
            case COMMITTED:
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received GLOBAL_COMMIT.");
                if (votedCommit) { // Only commit locally if this participant had voted to commit
                    txListener.onGlobalCommit(transactionId + "_" + participantId + "_local_op");
                } else {
                    // This scenario (voted ABORT but global is COMMIT) should ideally not happen in strict 2PC
                    // unless the coordinator made a mistake or there's a logic flaw.
                    // If it occurs, the participant should still honor its ABORT vote locally (i.e., do nothing).
                    logger.warning("Participant " + participantId + " (TxID: " + transactionId +
                            "): Received GLOBAL_COMMIT but had voted ABORT. Honoring local ABORT vote (doing nothing).");
                    // Or, if the protocol demands strict adherence to global commit regardless of local vote (less common for 2PC after voting phase):
                    // txListener.onGlobalCommit(transactionId + "_" + participantId + "_local_op_forced");
                }
                break;
            case ABORTED:
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received GLOBAL_ABORT.");
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op");
                break;
            case INIT:
            case PREPARING:
            case VOTING:
                logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Transaction still in progress (" + globalState + "). Continuing to watch.");
                // Watch should already be set, or will be re-set by getGlobalState(true)
                break;
            case UNKNOWN:
                logger.warning("Participant " + participantId + " (TxID: " + transactionId + "): Global transaction state is UNKNOWN (e.g., znode deleted prematurely by coordinator?). Assuming ABORT for safety.");
                txListener.onGlobalAbort(transactionId + "_" + participantId + "_local_op_due_to_unknown_state");
                break;
        }
    }

    /**
     * Called by the W2151443InternalNodeServiceImpl when it receives a direct CommitTransaction RPC.
     * This is an alternative/belt-and-suspenders way to learn the outcome.
     */
    public void forceCommit() {
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received direct COMMIT instruction.");
        processGlobalStateChange(DistributedTransaction.TxState.COMMITTED);
    }

    /**
     * Called by the W2151443InternalNodeServiceImpl when it receives a direct AbortTransaction RPC.
     */
    public void forceAbort() {
        logger.info("Participant " + participantId + " (TxID: " + transactionId + "): Received direct ABORT instruction.");
        processGlobalStateChange(DistributedTransaction.TxState.ABORTED);
    }
}
