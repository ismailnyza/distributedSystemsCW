// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/coordination/twopc/DistributedTransaction.java
package com.uow.W2151443.coordination.twopc;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList; // Added this import
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedTransaction {
    private static final Logger logger = Logger.getLogger(DistributedTransaction.class.getName());
    public static final String TRANSACTION_ROOT_ZNODE = "/W2151443_transactions";
    private final ZooKeeper zk;
    private final String transactionId;
    private final String transactionPath; // e.g., /W2151443_transactions/tx_12345

    public enum TxState {
        INIT, PREPARING, VOTING, COMMITTED, ABORTED, UNKNOWN
    }

    public DistributedTransaction(ZooKeeper zk, String transactionId) {
        this.zk = zk;
        this.transactionId = transactionId;
        this.transactionPath = TRANSACTION_ROOT_ZNODE + "/" + transactionId;
    }

    public String getTransactionPath() {
        return transactionPath;
    }

    public String getParticipantVotePath(String participantId) {
        return transactionPath + "/" + participantId + "_vote";
    }

    // Ensures the root transaction path exists
    public static void ensureTransactionRootExists(ZooKeeper zk) throws KeeperException, InterruptedException { // Corrected signature
        Stat stat = zk.exists(TRANSACTION_ROOT_ZNODE, false);
        if (stat == null) {
            zk.create(TRANSACTION_ROOT_ZNODE, "Transaction Root".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Created transaction root znode: " + TRANSACTION_ROOT_ZNODE);
        }
    }

    // Coordinator: Initializes the transaction znode
    public boolean begin() throws KeeperException, InterruptedException {
        ensureTransactionRootExists(zk); // Ensure root exists before creating transaction node
        if (zk.exists(transactionPath, false) == null) {
            zk.create(transactionPath, TxState.INIT.name().getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Transaction " + transactionId + " began. Path: " + transactionPath);
            return true;
        }
        logger.warning("Transaction " + transactionId + " path already exists: " + transactionPath);
        return false;
    }

    // Coordinator: Sets the global state (COMMIT or ABORT)
    public void setGlobalState(TxState state) throws KeeperException, InterruptedException {
        if (zk.exists(transactionPath, false) != null) {
            zk.setData(transactionPath, state.name().getBytes(StandardCharsets.UTF_8), -1);
            logger.info("Transaction " + transactionId + " global state set to: " + state);
        } else {
            logger.warning("Cannot set global state. Transaction path " + transactionPath + " does not exist for " + transactionId);
        }
    }

    // Participant or Coordinator: Reads the global state
    public TxState getGlobalState(boolean watch) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(transactionPath, watch);
        if (stat != null) {
            byte[] data = zk.getData(transactionPath, watch, stat);
            return TxState.valueOf(new String(data, StandardCharsets.UTF_8));
        }
        return TxState.UNKNOWN;
    }

    // Participant: Submits its vote
    public void submitVote(String participantId, boolean voteCommit) throws KeeperException, InterruptedException {
        String votePath = getParticipantVotePath(participantId);
        String voteData = voteCommit ? "COMMIT" : "ABORT";
        if (zk.exists(votePath, false) == null) {
            // Ensure the parent transaction path exists before creating a child vote node
            if (zk.exists(transactionPath, false) == null) {
                logger.warning("Transaction path " + transactionPath + " does not exist. Cannot create vote node for participant " + participantId + " of transaction " + transactionId);
                // This situation implies the coordinator might not have begun the transaction properly.
                // Or there's a race condition / ZK issue.
                // The participant should likely consider this an ABORT scenario.
                return; // Or throw exception
            }
            zk.create(votePath, voteData.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // Votes should be persistent until explicitly cleaned up
        } else {
            zk.setData(votePath, voteData.getBytes(StandardCharsets.UTF_8), -1);
        }
        logger.info("Participant " + participantId + " voted " + voteData + " for transaction " + transactionId);
    }

    // Coordinator: Collects all votes
    public List<String> getVotes() throws KeeperException, InterruptedException {
        List<String> votesData = new ArrayList<>();
        if (zk.exists(transactionPath, false) != null) {
            List<String> children = zk.getChildren(transactionPath, false); // No watch needed here, just reading current votes
            for (String child : children) {
                if (child.endsWith("_vote")) { // Filter for vote nodes
                    byte[] data = zk.getData(transactionPath + "/" + child, false, null);
                    votesData.add(new String(data, StandardCharsets.UTF_8));
                }
            }
        } else {
            logger.warning("Cannot get votes. Transaction path " + transactionPath + " does not exist for " + transactionId);
        }
        return votesData;
    }

    public void cleanup() {
        try {
            if (zk.exists(transactionPath, false) != null) {
                List<String> children = zk.getChildren(transactionPath, false);
                for (String child : children) {
                    zk.delete(transactionPath + "/" + child, -1);
                }
                zk.delete(transactionPath, -1);
                logger.info("Cleaned up transaction: " + transactionId + " at path " + transactionPath);
            } else {
                logger.info("Cleanup skipped for transaction " + transactionId + ": path " + transactionPath + " does not exist.");
            }
        } catch (KeeperException.NoNodeException e) {
            logger.info("Cleanup for transaction " + transactionId + ": Node or child already deleted (NoNodeException). Path: " + transactionPath);
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.WARNING, "Failed to fully cleanup transaction " + transactionId, e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }
}
