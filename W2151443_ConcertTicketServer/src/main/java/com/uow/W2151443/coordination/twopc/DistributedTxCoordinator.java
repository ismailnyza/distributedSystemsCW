package com.uow.W2151443.coordination.twopc;

import com.uow.W2151443.concert.service.GenericResponse;
import com.uow.W2151443.concert.service.ReserveComboRequest; // For the operation details
import com.uow.W2151443.concert.service.TransactionRequest;
import com.uow.W2151443.concert.service.VoteResponse;
import com.uow.W2151443.concert.service.W2151443InternalNodeServiceGrpc;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance; // To know participant addresses
import com.uow.W2151443.server.ConcertTicketServerApp; // To get its own details and etcd client

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DistributedTxCoordinator {
    private static final Logger logger = Logger.getLogger(DistributedTxCoordinator.class.getName());

    private final String transactionId;
    private final DistributedTransaction transaction;
    private final ZooKeeper zk;
    private final ReserveComboRequest operationDetails; // The actual combo reservation to perform
    private final List<ServiceInstance> participants; // List of all participants (secondaries)
    private final ConcertTicketServerApp leaderServerApp; // To make gRPC calls

    private int expectedVotes;
    private final CountDownLatch voteLatch;

    public DistributedTxCoordinator(ConcertTicketServerApp leaderServerApp,
                                    String transactionId,
                                    ReserveComboRequest operationDetails,
                                    List<ServiceInstance> participantNodes) { // Secondaries
        this.leaderServerApp = leaderServerApp;
        this.zk = leaderServerApp.getZooKeeperClient(); // Assume ConcertTicketServerApp provides this
        this.transactionId = transactionId;
        this.transaction = new DistributedTransaction(zk, transactionId);
        this.operationDetails = operationDetails;
        this.participants = participantNodes; // These are the secondaries
        // Expected votes = number of secondaries + 1 (for the leader itself as a participant)
        this.expectedVotes = participantNodes.size() + 1;
        this.voteLatch = new CountDownLatch(expectedVotes); // Initialize correctly based on actual participant count
    }

    /**
     * Phase 1: Send Prepare (Vote Request) to all participants (including self conceptually)
     * and wait for votes.
     * @return true if all participants (including leader's local check) voted COMMIT, false otherwise.
     */
    public boolean prepare() throws KeeperException, InterruptedException {
        logger.info("Coordinator (TxID: " + transactionId + "): Starting PREPARE phase for " + expectedVotes + " participants.");
        transaction.begin(); // Create /tx_id znode, set to INIT or PREPARING

        // Leader acts as a participant for its own data first (local check)
        boolean leaderLocalVoteCommit = performLeaderLocalPrepare();
        if (!leaderLocalVoteCommit) {
            logger.warning("Coordinator (TxID: " + transactionId + "): Leader local prepare failed (cannot commit). Voting ABORT locally.");
            // Leader's own vote is ABORT, so the transaction will abort.
            // No need to ask others if leader itself cannot commit.
            transaction.submitVote(leaderServerApp.etcdServiceInstanceId, false); // Leader votes ABORT
            return false; // Overall transaction aborts
        } else {
            logger.info("Coordinator (TxID: " + transactionId + "): Leader local prepare successful. Voting COMMIT locally.");
            transaction.submitVote(leaderServerApp.etcdServiceInstanceId, true); // Leader votes COMMIT
        }
        // voteLatch.countDown(); // Leader's vote counted (or handled by expectedVotes initial value)

        // Send PrepareTransaction RPC to all secondary participants
        TransactionRequest prepareRpcRequest = TransactionRequest.newBuilder()
                .setTransactionId(transactionId)
                .setOriginalComboRequest(operationDetails) // Send the details needed for participant to check
                .build();

        for (ServiceInstance participant : participants) { // these are secondaries
            sendPrepareToParticipant(participant, prepareRpcRequest);
        }

        // Wait for all votes (or timeout) - ZK watches on vote znodes would be more robust
        // For simplicity, this example might just query after a delay or rely on participants voting quickly.
        // A more robust way is to use ZooKeeper watches on the individual vote paths or the transaction path children.
        // Here, we'll assume votes are submitted directly to ZK by participants.
        // The leader will then collect these votes.

        // This simple latch assumes RPC calls to prepare are synchronous enough or we wait.
        // In reality, vote collection is more complex.
        // For this example, let's assume participants directly call transaction.submitVote().
        // The coordinator then calls transaction.getVotes().

        logger.info("Coordinator (TxID: " + transactionId + "): All PREPARE requests sent. Waiting for votes from secondaries...");
        // Actual vote collection and decision will be in a separate method or after this.
        // This 'prepare' method's job is just to initiate.
        return true; // Indicates prepare phase initiated, not the final outcome.
    }

    /**
     * Leader performs its own local "prepare" check.
     * @return true if the leader can commit locally, false otherwise.
     */
    private boolean performLeaderLocalPrepare() {
        logger.info("Coordinator (TxID: " + transactionId + "): Leader (" + leaderServerApp.etcdServiceInstanceId + ") performing local prepare check...");
        // Access leader's local data store (showsData) and check if the combo can be reserved
        // This logic is similar to what W2151443ReservationServiceImpl.reserveCombo does for local commit
        synchronized (leaderServerApp.W2151443_showsDataStore) { // Accessing shared data store from app
            ShowInfo currentShow = leaderServerApp.W2151443_showsDataStore.get(operationDetails.getShowId());
            if (currentShow == null) return false; // Show not found

            ShowInfo.Builder mutableShowBuilder = currentShow.toBuilder(); // Use a temporary builder for checks
            boolean possibleToReserve = true;

            if (operationDetails.getConcertItemsList().isEmpty()){ possibleToReserve = false; }
            else {
                for (ReservationItem item : operationDetails.getConcertItemsList()) {
                    TierDetails tier = mutableShowBuilder.getTiersMap().get(item.getTierId());
                    if (tier == null || tier.getCurrentStock() < item.getQuantity()) {
                        possibleToReserve = false; break;
                    }
                }
            }
            if (possibleToReserve) {
                if (!mutableShowBuilder.getAfterPartyAvailable() ||
                        mutableShowBuilder.getAfterPartyCurrentStock() < operationDetails.getAfterPartyQuantity()) {
                    possibleToReserve = false;
                }
            }
            logger.info("Coordinator (TxID: " + transactionId + "): Leader local prepare check result: " + possibleToReserve);
            return possibleToReserve;
        }
    }


    private void sendPrepareToParticipant(ServiceInstance participant, TransactionRequest prepareRequest) {
        logger.fine("Coordinator (TxID: " + transactionId + "): Sending PREPARE to participant: " + participant.getId() + " at " + participant.getIp() + ":" + participant.getPort());
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(participant.getIp(), participant.getPort())
                    .usePlaintext().build();
            W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                    W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

            // This RPC tells the participant to start its 2PC logic and vote in ZK.
            // The VoteResponse here is just an ACK that it received the prepare, not the actual vote.
            VoteResponse ack = stub.withDeadlineAfter(10, TimeUnit.SECONDS).prepareTransaction(prepareRequest);
            // The actual vote from participant 'participant.getId()' will be written to ZooKeeper:
            // transaction.getParticipantVotePath(participant.getId())
            // The coordinator will later collect these votes from ZK.
            logger.info("Coordinator (TxID: " + transactionId + "): PREPARE request acknowledged by participant " + participant.getId() + ". They will now vote in ZooKeeper.");

        } catch (Exception e) {
            logger.log(Level.WARNING, "Coordinator (TxID: " + transactionId + "): Failed to send PREPARE or get ACK from participant " + participant.getId(), e);
            // If PREPARE fails to send, this participant is effectively an ABORT vote.
            // Need robust handling: assume ABORT or retry. For now, we assume it will lead to an ABORT if critical.
            // The coordinator, when collecting votes, will see this participant hasn't voted COMMIT.
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {Thread.currentThread().interrupt();}
            }
        }
    }

    /**
     * Phase 2: Collect votes and decide global outcome.
     * This should be called after allowing participants time to vote.
     * @return true if GLOBAL_COMMIT, false if GLOBAL_ABORT.
     */
    public boolean decideGlobalOutcome() throws KeeperException, InterruptedException {
        logger.info("Coordinator (TxID: " + transactionId + "): Collecting votes...");

        // Give some time for votes to appear. In a real system, watches are better.
        Thread.sleep(2000); // Simple delay for votes to be cast via ZK by participants

        List<String> votes = transaction.getVotes();
        logger.info("Coordinator (TxID: " + transactionId + "): Received " + votes.size() + " votes out of " + expectedVotes + " expected.");

        // Check if all expected participants have voted.
        // The leader already voted. Secondaries are in 'participants' list.
        // Total expected votes = 1 (leader) + participants.size().
        // This simple check might not be robust if a participant crashes before voting.
        // A more robust check would iterate through expected participant IDs and check their specific vote znodes.

        boolean allCommit = true;
        if (votes.size() < expectedVotes) {
            logger.warning("Coordinator (TxID: " + transactionId + "): Not all participants voted in time. Assuming ABORT for missing votes.");
            allCommit = false; // Timeout or missing votes typically mean abort
        } else {
            for (String vote : votes) {
                if (!"COMMIT".equalsIgnoreCase(vote)) {
                    allCommit = false;
                    break;
                }
            }
        }

        if (allCommit) {
            logger.info("Coordinator (TxID: " + transactionId + "): All participants voted COMMIT. Deciding GLOBAL_COMMIT.");
            transaction.setGlobalState(DistributedTransaction.TxState.COMMITTED);
            notifyParticipants(true); // Notify of global commit
            return true;
        } else {
            logger.info("Coordinator (TxID: " + transactionId + "): One or more participants voted ABORT or timed out. Deciding GLOBAL_ABORT.");
            transaction.setGlobalState(DistributedTransaction.TxState.ABORTED);
            notifyParticipants(false); // Notify of global abort
            return false;
        }
    }

    private void notifyParticipants(boolean globalCommit) {
        logger.info("Coordinator (TxID: " + transactionId + "): Notifying participants of GLOBAL_" + (globalCommit ? "COMMIT" : "ABORT"));
        // Leader first processes its own local commit/abort via listener
        if (leaderServerApp instanceof DistributedTxListener) { // Assuming ConcertTicketServerApp implements it
            if (globalCommit) {
                ((DistributedTxListener) leaderServerApp).onGlobalCommit(transactionId + "_leader_local_op"); // Suffix for clarity
            } else {
                ((DistributedTxListener) leaderServerApp).onGlobalAbort(transactionId + "_leader_local_op");
            }
        }


        for (ServiceInstance participant : participants) { // These are the secondaries
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder.forAddress(participant.getIp(), participant.getPort())
                        .usePlaintext().build();
                W2151443InternalNodeServiceGrpc.W2151443InternalNodeServiceBlockingStub stub =
                        W2151443InternalNodeServiceGrpc.newBlockingStub(channel);

                if (globalCommit) {
                    stub.commitTransaction(com.uow.W2151443.concert.service.TransactionDecision.newBuilder().setTransactionId(transactionId).build());
                } else {
                    stub.abortTransaction(com.uow.W2151443.concert.service.TransactionDecision.newBuilder().setTransactionId(transactionId).build());
                }
                logger.info("Coordinator (TxID: " + transactionId + "): Notified participant " + participant.getId() + " of GLOBAL_" + (globalCommit ? "COMMIT" : "ABORT"));
            } catch (Exception e) {
                logger.log(Level.WARNING, "Coordinator (TxID: " + transactionId + "): Failed to notify participant " + participant.getId() + " of global decision.", e);
                // Participant will eventually find out by watching ZK or on timeout.
            } finally {
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {Thread.currentThread().interrupt();}
                }
            }
        }
        // Optionally, cleanup ZK transaction znodes after a while
        // transaction.cleanup(); // Be careful with timing of cleanup
    }

    public String getTransactionId() {
        return transactionId;
    }
}
