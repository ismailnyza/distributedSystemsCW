package com.uow.W2151443.coordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException; // Will be removed if connect() is removed
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
// import java.util.concurrent.CountDownLatch; // No longer needed here if connect() is removed
// import java.util.concurrent.TimeUnit; // No longer needed here
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443LeaderElection implements Watcher {
    private static final Logger logger = Logger.getLogger(W2151443LeaderElection.class.getName());

    private final ZooKeeper zooKeeper; // Use this client passed from ConcertTicketServerApp
    private final String electionBasePath;
    private String currentNodePath;
    private volatile boolean isLeader = false;
    private final String serverId;
    private final String serverData; // Data for this server (e.g., "ip:port")

    // private final CountDownLatch connectedSignal = new CountDownLatch(1); // Not needed here if ZK client is managed by App
    private final LeaderChangeListener listener;

    public interface LeaderChangeListener {
        void onElectedLeader();
        void onWorker();
    }

    public W2151443LeaderElection(ZooKeeper zkClient, String electionBasePath, String serverId, String serverData, LeaderChangeListener listener) {
        if (zkClient == null) {
            throw new IllegalArgumentException("ZooKeeper client cannot be null.");
        }
        this.zooKeeper = zkClient; // Use the passed-in, already connected or connecting client
        this.electionBasePath = electionBasePath;
        this.serverId = serverId;
        this.serverData = serverData;
        this.listener = listener;
        logger.info("W2151443LeaderElection initialized for server: " + serverId + " with ZK client session ID: " + (zkClient.getSessionId() == 0L ? "Not Connected Yet" : Long.toHexString(zkClient.getSessionId())));

        // Ensure base path exists upon initialization.
        // This might be better done once in the main app, but doing it here is also okay.
        try {
            ensureBasePathExists();
        } catch (KeeperException | InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to ensure election base path " + electionBasePath + " exists during W2151443LeaderElection initialization.", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Depending on the error, you might want to throw a runtime exception
            // or handle it in a way that prevents the server from starting incorrectly.
            // For now, just logging. The volunteerForLeadership will likely fail if ZK is unusable.
        }
    }

    // Removed connect() method as the ZooKeeper client is now managed by ConcertTicketServerApp

    private void ensureBasePathExists() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(electionBasePath, false) == null) {
            logger.info("Election base path " + electionBasePath + " does not exist for server " + serverId + ". Creating it.");
            try {
                zooKeeper.create(electionBasePath, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                logger.info("Election base path " + electionBasePath + " was created by another instance concurrently (server " + serverId + ").");
            }
        } else {
            logger.fine("Election base path " + electionBasePath + " already exists (checked by server " + serverId + ").");
        }
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        if (zooKeeper == null || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            logger.severe(serverId + ": Not connected to ZooKeeper. Cannot volunteer for leadership.");
            // Potentially trigger onWorker() if we were previously leader, or signal error
            if (listener != null) listener.onWorker();
            // Re-attempt connection or signal failure to the main app to handle.
            // For now, this method will likely fail if ZK is not connected.
            // The main app should ensure ZK is connected before calling this.
            throw new KeeperException.ConnectionLossException();
        }
        // Ensure base path one more time in case it was missed or ZK session expired and reconnected
        ensureBasePathExists();

        String znodePrefix = electionBasePath + "/node-W2151443-";
        currentNodePath = zooKeeper.create(znodePrefix,
                serverData.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info(serverId + ": Volunteered for leadership. Created znode: " + currentNodePath + " with data: " + serverData);
        attemptToBecomeLeader();
    }

    private void attemptToBecomeLeader() throws KeeperException, InterruptedException {
        if (currentNodePath == null) {
            logger.warning(serverId + ": Current node path is null. Cannot attempt to become leader. Re-volunteering might be needed.");
            if (listener != null) listener.onWorker();
            // This state indicates a problem, perhaps the znode creation failed silently or was deleted.
            // Consider re-trying volunteerForLeadership or signaling error.
            return;
        }
        if (zooKeeper == null || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            logger.warning(serverId + ": ZooKeeper disconnected while attempting to become leader for " + currentNodePath);
            if(listener != null) listener.onWorker();
            isLeader = false;
            // The main ZK watcher in ConcertTicketServerApp should handle reconnection.
            // When it reconnects, this node might need to re-evaluate its leadership status.
            return;
        }


        List<String> children = zooKeeper.getChildren(electionBasePath, false); // No watch here, just get current children
        if (children.isEmpty()) {
            logger.warning(serverId + ": No children found in election path " + electionBasePath + " including my own ("+currentNodePath+"). This is unexpected after creating a node. Re-attempting leadership.");
            isLeader = false;
            if (listener != null) listener.onWorker();
            // Possibly the node got deleted immediately. Try to re-volunteer.
            // volunteerForLeadership(); // Be careful with recursion, add a limit or delay
            return;
        }
        Collections.sort(children);

        String smallestChildNodeName = children.get(0);
        String smallestChildPath = electionBasePath + "/" + smallestChildNodeName;

        if (currentNodePath.equals(smallestChildPath)) {
            if (!isLeader) { // Check if status is changing
                isLeader = true;
                logger.info("***** " + serverId + " (" + currentNodePath + ") IS NOW LEADER! *****");
                if (listener != null) listener.onElectedLeader();
            } else {
                logger.fine(serverId + " (" + currentNodePath + ") re-confirmed as LEADER.");
            }
        } else {
            if (isLeader) { // Check if status is changing
                logger.info("----- " + serverId + " (" + currentNodePath + ") WAS LEADER, but no longer is. Now a WORKER. New leader: " + smallestChildPath + " -----");
            } else {
                logger.info(serverId + " (" + currentNodePath + ") is a WORKER. Current leader appears to be " + smallestChildPath);
            }
            isLeader = false;
            if (listener != null) listener.onWorker();

            int currentIndex = children.indexOf(currentNodePath.substring(electionBasePath.length() + 1));
            if (currentIndex == -1) { // Current node not in the list of children
                logger.warning(serverId + ": My node " + currentNodePath + " is not in the list of children: " + children + ". Ephemeral node might have been deleted. Attempting to re-volunteer.");
                currentNodePath = null; // Reset path
                volunteerForLeadership(); // Re-create the node
                return;
            }

            if (currentIndex > 0) {
                String nodeToWatchPath = electionBasePath + "/" + children.get(currentIndex - 1);
                watchPreviousNode(nodeToWatchPath);
            } else {
                // This implies currentIndex is 0, but we already checked if we are the smallest.
                // This case should not be reached if logic is correct.
                logger.severe(serverId + ": Inconsistent state. Smallest node is " + smallestChildPath + ", my node is " + currentNodePath + ", but my index is " + currentIndex + ". Re-evaluating.");
                attemptToBecomeLeader(); // Re-evaluate.
            }
        }
    }

    private void watchPreviousNode(String path) throws KeeperException, InterruptedException {
        logger.info(serverId + " ("+currentNodePath+"): Watching predecessor node: " + path);
        // Set a watch. The 'this' instance will handle the event in its process() method.
        Stat stat = zooKeeper.exists(path, this);
        if (stat == null) {
            logger.info(serverId + ": Predecessor node " + path + " disappeared before watch could be set. Re-attempting leadership check.");
            attemptToBecomeLeader(); // Predecessor gone, check if we are now leader
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // This process method is primarily for the Watcher set by zooKeeper.exists() on the predecessor.
        logger.info(serverId + " ("+ (currentNodePath != null ? currentNodePath : "N/A") +"): ZooKeeper Watcher Event: " + event);

        if (event.getType() == Event.EventType.NodeDeleted) {
            // A watched node was deleted (likely the one before us, or the election base path if that was watched elsewhere)
            if (event.getPath() != null && !event.getPath().equals(currentNodePath)) {
                logger.info(serverId + ": Watched node " + event.getPath() + " deleted. Re-evaluating leadership.");
                try {
                    attemptToBecomeLeader();
                } catch (KeeperException | InterruptedException e) {
                    logger.log(Level.SEVERE, serverId + ": Error re-evaluating leadership after node deletion.", e);
                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                    // Critical error, might need to signal main app or attempt to close/reconnect ZK.
                    // For now, the app's main ZK client watcher handles session issues like Expired/Disconnected.
                }
            }
        }
        // The main ZK session events (SyncConnected, Expired, Disconnected) are handled by the Watcher
        // in ConcertTicketServerApp for the shared zooKeeperClient.
        // This LeaderElection's watcher is specific to znode watches it sets (e.g., on predecessor).
        // However, if the ZK session used by this election instance expires, it also needs to react.
        // The current design passes the shared ZK client, so ConcertTicketServerApp's main watcher
        // for that client handles session events. W2151443LeaderElection's onWorker/onElectedLeader
        // will be triggered by the main app in those cases.
    }


    public boolean isLeader() {
        return isLeader;
    }

    public String getCurrentLeaderData() throws KeeperException, InterruptedException {
        if (zooKeeper == null || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            logger.warning(serverId + ": Not connected to ZooKeeper. Cannot get leader data.");
            return null;
        }
        List<String> children = zooKeeper.getChildren(electionBasePath, false);
        if (children.isEmpty()) {
            logger.info(serverId + ": No leader elected yet (no children in election path " + electionBasePath + ").");
            return null;
        }
        Collections.sort(children);
        String leaderZnodePath = electionBasePath + "/" + children.get(0);
        byte[] data = zooKeeper.getData(leaderZnodePath, false, null);
        if (data != null) {
            return new String(data, StandardCharsets.UTF_8);
        }
        logger.warning(serverId + ": Leader znode " + leaderZnodePath + " has null data.");
        return null;
    }

    public void close() {
        // This method is called when the server instance is shutting down.
        // The ephemeral znode (currentNodePath) will be automatically deleted by ZooKeeper
        // when this client's session is closed or expires.
        // The shared ZooKeeper client is closed by ConcertTicketServerApp.
        logger.info(serverId + ": W2151443LeaderElection close() called. Ephemeral node " + currentNodePath + " will be removed by ZK upon session closure.");
        isLeader = false; // No longer leader if we are closing.
        // The listener.onWorker() would typically be called by the main app's shutdown or ZK session expiry logic.
    }

    // Main method for quick testing (can be removed or commented out in final version)
    public static void main(String[] args) throws Exception {
        String zkUrl = "localhost:2181";
        String electionPath = "/W2151443_LeaderElectionTestMain"; // Use a different path for this test

        // Setup a ZK client for Server1
        CountDownLatch s1Latch = new CountDownLatch(1);
        ZooKeeper zk1 = new ZooKeeper(zkUrl, 3000, event -> { if(event.getState() == Event.KeeperState.SyncConnected) s1Latch.countDown(); });
        s1Latch.await(5, TimeUnit.SECONDS);

        W2151443LeaderElection server1 = new W2151443LeaderElection(zk1, electionPath, "Server1-MainTest", "127.0.0.1:9090",
                new LeaderChangeListener() {
                    @Override public void onElectedLeader() { System.out.println("Server1-MainTest: I am the LEADER!"); }
                    @Override public void onWorker() { System.out.println("Server1-MainTest: I am a WORKER."); }
                });
        server1.volunteerForLeadership();
        Thread.sleep(1000);

        // Setup a ZK client for Server2
        CountDownLatch s2Latch = new CountDownLatch(1);
        ZooKeeper zk2 = new ZooKeeper(zkUrl, 3000, event -> { if(event.getState() == Event.KeeperState.SyncConnected) s2Latch.countDown(); });
        s2Latch.await(5, TimeUnit.SECONDS);

        W2151443LeaderElection server2 = new W2151443LeaderElection(zk2, electionPath, "Server2-MainTest", "127.0.0.1:9091",
                new LeaderChangeListener() {
                    @Override public void onElectedLeader() { System.out.println("Server2-MainTest: I am the LEADER!"); }
                    @Override public void onWorker() { System.out.println("Server2-MainTest: I am a WORKER."); }
                });
        server2.volunteerForLeadership();
        Thread.sleep(2000); // Give more time for election and watches

        System.out.println("Server1-MainTest is leader: " + server1.isLeader());
        System.out.println("Server2-MainTest is leader: " + server2.isLeader());
        System.out.println("Current leader data from Server1's perspective: " + server1.getCurrentLeaderData());

        System.out.println("\n--- Simulating Server1 (leader) crash by closing its ZK session ---");
        zk1.close(); // This will delete its ephemeral node
        Thread.sleep(10000); // Give Server2 ample time to detect and elect itself

        System.out.println("After Server1-MainTest ZK session close:");
        System.out.println("Server2-MainTest is leader: " + server2.isLeader());
        System.out.println("Current leader data from Server2's perspective: " + server2.getCurrentLeaderData());

        zk2.close();
        System.out.println("Test finished.");
    }
}
