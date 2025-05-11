package com.uow.W2151443.coordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443LeaderElection implements Watcher {
    private static final Logger logger = Logger.getLogger(W2151443LeaderElection.class.getName());

    private ZooKeeper zooKeeper;
    private final String zooKeeperUrl;
    private final String electionBasePath; // e.g., "/W2151443_concert_election"
    private String currentNodePath; // Path of the znode created by this instance
    private volatile boolean isLeader = false;
    private final String serverId; // Unique ID for this server instance
    private final String serverData; // Data for this server (e.g., "ip:port")

    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private final LeaderChangeListener listener;

    public interface LeaderChangeListener {
        void onElectedLeader();
        void onWorker(); // When it's confirmed not to be leader or loses leadership
    }

    public W2151443LeaderElection(String zooKeeperUrl, String electionBasePath, String serverId, String serverData, LeaderChangeListener listener) {
        this.zooKeeperUrl = zooKeeperUrl;
        this.electionBasePath = electionBasePath;
        this.serverId = serverId;
        this.serverData = serverData;
        this.listener = listener;
    }

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(zooKeeperUrl, 3000, this); // 3s session timeout
        connectedSignal.await(10, TimeUnit.SECONDS); // Wait for connection
        if (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            throw new IOException("Failed to connect to ZooKeeper at " + zooKeeperUrl);
        }
        logger.info("Connected to ZooKeeper: " + zooKeeperUrl);
        try {
            ensureBasePathExists();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureBasePathExists() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(electionBasePath, false);
        if (stat == null) {
            logger.info("Election base path " + electionBasePath + " does not exist. Creating it.");
            try {
                zooKeeper.create(electionBasePath, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Another instance might have created it concurrently, which is fine.
                logger.info("Election base path " + electionBasePath + " was created by another instance concurrently.");
            }
        } else {
            logger.info("Election base path " + electionBasePath + " already exists.");
        }
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        if (zooKeeper == null || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            logger.severe("Not connected to ZooKeeper. Cannot volunteer for leadership.");
            return;
        }
        String znodePrefix = electionBasePath + "/node-W2151443-";
        // Create an ephemeral sequential znode
        currentNodePath = zooKeeper.create(znodePrefix,
                serverData.getBytes(StandardCharsets.UTF_8), // Store server's data (IP:Port)
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("Volunteered for leadership. Created znode: " + currentNodePath + " with data: " + serverData);
        attemptToBecomeLeader();
    }

    private void attemptToBecomeLeader() throws KeeperException, InterruptedException {
        if (currentNodePath == null) {
            logger.warning("Current node path is null. Cannot attempt to become leader.");
            if (listener != null) listener.onWorker();
            return;
        }

        List<String> children = zooKeeper.getChildren(electionBasePath, false);
        Collections.sort(children);

        String smallestChildPath = electionBasePath + "/" + children.get(0);

        if (currentNodePath.equals(smallestChildPath)) {
            isLeader = true;
            logger.info("Elected as LEADER: " + serverId + " (" + currentNodePath + ")");
            if (listener != null) listener.onElectedLeader();
        } else {
            isLeader = false;
            logger.info(serverId + " is a WORKER (" + currentNodePath + "). Current leader seems to be " + smallestChildPath);
            if (listener != null) listener.onWorker();
            // Watch the node just before this one
            int currentIndex = children.indexOf(currentNodePath.substring(electionBasePath.length() + 1));
            if (currentIndex > 0) { // Should always be > 0 if not leader
                String nodeToWatch = electionBasePath + "/" + children.get(currentIndex - 1);
                watchPreviousNode(nodeToWatch);
            } else {
                // This case should ideally not happen if not leader and list is not empty.
                // Could happen if the current node was deleted for some reason. Re-volunteer might be an option.
                logger.warning("Could not find predecessor to watch for " + currentNodePath + ". Children: " + children);
            }
        }
    }

    private void watchPreviousNode(String path) throws KeeperException, InterruptedException {
        logger.info(serverId + " is watching node: " + path);
        Stat stat = zooKeeper.exists(path, true); // Set a watch
        if (stat == null) {
            // Predecessor disappeared between getChildren and exists, re-check leadership
            logger.info("Node " + path + " disappeared before watch could be set. Re-attempting leadership check.");
            attemptToBecomeLeader();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("ZooKeeper Watcher: Received event: " + event);
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown(); // Signal that connection is up
            logger.info("ZooKeeper Watcher: Successfully connected to ZooKeeper.");
        } else if (event.getState() == Event.KeeperState.Expired) {
            logger.warning("ZooKeeper Watcher: Session expired. Attempting to reconnect and re-volunteer.");
            isLeader = false;
            if (listener != null) listener.onWorker();
            try {
                close(); // Close old session
                connect();
                volunteerForLeadership();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "ZooKeeper Watcher: Error re-establishing session after expiration.", e);
            }
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            logger.warning("ZooKeeper Watcher: Disconnected from ZooKeeper. Will attempt to reconnect automatically.");
            isLeader = false; // Assume not leader if disconnected
            if (listener != null) listener.onWorker();
        }

        if (event.getType() == Event.EventType.NodeDeleted) {
            // A watched node was deleted (likely the one before us)
            if (event.getPath() != null && !event.getPath().equals(currentNodePath)) { // Not our own node deletion
                logger.info("ZooKeeper Watcher: Watched node " + event.getPath() + " deleted. Re-evaluating leadership for " + serverId);
                try {
                    attemptToBecomeLeader();
                } catch (KeeperException | InterruptedException e) {
                    logger.log(Level.SEVERE, "ZooKeeper Watcher: Error re-evaluating leadership for " + serverId, e);
                }
            }
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public String getCurrentLeaderData() throws KeeperException, InterruptedException {
        if (zooKeeper == null || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            logger.warning("Not connected to ZooKeeper. Cannot get leader data.");
            return null;
        }
        List<String> children = zooKeeper.getChildren(electionBasePath, false);
        if (children.isEmpty()) {
            logger.info("No leader elected yet (no children in election path).");
            return null;
        }
        Collections.sort(children);
        String leaderZnodePath = electionBasePath + "/" + children.get(0);
        byte[] data = zooKeeper.getData(leaderZnodePath, false, null);
        if (data != null) {
            return new String(data, StandardCharsets.UTF_8);
        }
        return null;
    }


    public void close() {
        isLeader = false;
        if (listener != null) listener.onWorker();
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                logger.info("ZooKeeper client session closed for " + serverId);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Error closing ZooKeeper client session for " + serverId, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    // For quick testing
    public static void main(String[] args) throws Exception {
        String zkUrl = "localhost:2181";
        String electionPath = "/W2151443_LeaderElectionTest";

        // Simulate Server 1
        W2151443LeaderElection server1 = new W2151443LeaderElection(zkUrl, electionPath, "Server1", "127.0.0.1:9090",
                new LeaderChangeListener() {
                    @Override
                    public void onElectedLeader() { System.out.println("Server1: I am the LEADER!"); }
                    @Override
                    public void onWorker() { System.out.println("Server1: I am a WORKER."); }
                });
        server1.connect();
        server1.volunteerForLeadership();

        Thread.sleep(1000); // Give it time to elect

        // Simulate Server 2
        W2151443LeaderElection server2 = new W2151443LeaderElection(zkUrl, electionPath, "Server2", "127.0.0.1:9091",
                new LeaderChangeListener() {
                    @Override
                    public void onElectedLeader() { System.out.println("Server2: I am the LEADER!"); }
                    @Override
                    public void onWorker() { System.out.println("Server2: I am a WORKER."); }
                });
        server2.connect();
        server2.volunteerForLeadership();

        Thread.sleep(1000);

        System.out.println("Server1 is leader: " + server1.isLeader());
        System.out.println("Server2 is leader: " + server2.isLeader());
        System.out.println("Current leader data from Server1's perspective: " + server1.getCurrentLeaderData());
        System.out.println("Current leader data from Server2's perspective: " + server2.getCurrentLeaderData());


        System.out.println("\n--- Simulating Server1 (leader) crash ---");
        server1.close(); // This will delete its ephemeral node
        Thread.sleep(5000); // Give Server2 time to detect and elect itself

        System.out.println("After Server1 crash:");
        System.out.println("Server1 is leader: " + server1.isLeader()); // Should be false
        System.out.println("Server2 is leader: " + server2.isLeader()); // Should be true
        System.out.println("Current leader data from Server2's perspective: " + server2.getCurrentLeaderData());

        server2.close();
        System.out.println("Test finished.");
    }
}
