// File: distributedSyStemsCW/W2151443_ConcertTicketServer/src/main/java/com/uow/W2151443/coordination/twopc/DistributedTransaction.java
package com.uow.W2151443.coordination.twopc;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedTransaction {
    private static final Logger logger = Logger.getLogger(DistributedTransaction.class.getName());
    public static final String TRANSACTION_ROOT_ZNODE = "/W2151443_transactions";
    private final ZooKeeper zk;
    private final String transactionId;
    private final String transactionPath;

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

    public static void ensureTransactionRootExists(ZooKeeper zk) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(TRANSACTION_ROOT_ZNODE, false);
        if (stat == null) {
            try {
                zk.create(TRANSACTION_ROOT_ZNODE, "Transaction Root".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created transaction root znode: " + TRANSACTION_ROOT_ZNODE);
            } catch (KeeperException.NodeExistsException e) {
                logger.info("Transaction root znode " + TRANSACTION_ROOT_ZNODE + " already exists (created concurrently).");
            }
        }
    }

    public boolean begin() throws KeeperException, InterruptedException {
        ensureTransactionRootExists(zk);
        if (zk.exists(transactionPath, false) == null) {
            try {
                zk.create(transactionPath, TxState.INIT.name().getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Transaction " + transactionId + " began. Path: " + transactionPath);
                return true;
            } catch (KeeperException.NodeExistsException e) {
                logger.warning("Transaction " + transactionId + " path "+ transactionPath +" already exists (created concurrently during begin).");
                return false; // Or re-read state
            }
        }
        logger.warning("Transaction " + transactionId + " path already exists before attempting create: " + transactionPath);
        // If it already exists, check its state. Maybe it's a retried transaction.
        // For simplicity, if it exists, we consider 'begin' to have failed for this attempt to start fresh.
        return false;
    }

    public void setGlobalState(TxState state) throws KeeperException, InterruptedException {
        if (zk.exists(transactionPath, false) != null) {
            zk.setData(transactionPath, state.name().getBytes(StandardCharsets.UTF_8), -1);
            logger.info("Transaction " + transactionId + " global state set to: " + state);
        } else {
            logger.warning("Cannot set global state for TxID " + transactionId + ". Transaction path " + transactionPath + " does not exist.");
        }
    }

    public TxState getGlobalState(boolean watch) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(transactionPath, watch);
        if (stat != null) {
            byte[] data = zk.getData(transactionPath, watch, stat);
            try {
                return TxState.valueOf(new String(data, StandardCharsets.UTF_8));
            } catch (IllegalArgumentException e) {
                logger.log(Level.WARNING, "Unknown state data in ZK for TxID " + transactionId + ": " + new String(data, StandardCharsets.UTF_8), e);
                return TxState.UNKNOWN;
            }
        }
        return TxState.UNKNOWN;
    }

    public void submitVote(String participantId, boolean voteCommit) throws KeeperException, InterruptedException {
        String votePath = getParticipantVotePath(participantId);
        String voteData = voteCommit ? "COMMIT" : "ABORT";
        if (zk.exists(transactionPath, false) == null) {
            logger.warning("Transaction path " + transactionPath + " does not exist. Cannot create vote node for participant " + participantId + " of transaction " + transactionId);
            return;
        }
        if (zk.exists(votePath, false) == null) {
            zk.create(votePath, voteData.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(votePath, voteData.getBytes(StandardCharsets.UTF_8), -1);
        }
        logger.info("Participant " + participantId + " voted " + voteData + " for transaction " + transactionId + " at path " + votePath);
    }

    /**
     * Gets the vote string ("COMMIT" or "ABORT") for a specific participant.
     * @param participantId The ID of the participant.
     * @return The vote string, or null if the vote node doesn't exist or has no data.
     */
    public String getParticipantVote(String participantId) throws KeeperException, InterruptedException {
        String votePath = getParticipantVotePath(participantId);
        Stat stat = zk.exists(votePath, false); // No watch needed here, just reading vote
        if (stat != null) {
            byte[] data = zk.getData(votePath, false, stat);
            if (data != null && data.length > 0) {
                return new String(data, StandardCharsets.UTF_8);
            }
        }
        return null; // Vote not found or empty
    }


    public List<String> getVotes() throws KeeperException, InterruptedException {
        List<String> votesData = new ArrayList<>();
        if (zk.exists(transactionPath, false) != null) {
            List<String> children = zk.getChildren(transactionPath, false);
            for (String child : children) {
                if (child.endsWith("_vote")) {
                    byte[] data = zk.getData(transactionPath + "/" + child, false, null);
                    if (data != null && data.length > 0) {
                        votesData.add(new String(data, StandardCharsets.UTF_8));
                    }
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
                    try {
                        zk.delete(transactionPath + "/" + child, -1);
                    } catch (KeeperException.NoNodeException e) {
                        logger.fine("Child node " + transactionPath + "/" + child + " already deleted during cleanup for TxID " + transactionId);
                    }
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
