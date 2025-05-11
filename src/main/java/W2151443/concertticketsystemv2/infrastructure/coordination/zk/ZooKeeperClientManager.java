package W2151443.concertticketsystemv2.infrastructure.coordination.zk;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooKeeperClientManager implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClientManager.class);
    private final String connectString;
    private final int sessionTimeoutMs;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectionLatch = new CountDownLatch(1);
    private ZooKeeperConnectionListener connectionListener;

    public ZooKeeperClientManager(String connectString, int sessionTimeoutMs) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void setConnectionListener(ZooKeeperConnectionListener listener) {
        this.connectionListener = listener;
    }

    public void connect() throws IOException, InterruptedException {
        if (zooKeeper != null && zooKeeper.getState().isAlive()) {
            LOGGER.info("ZooKeeper client is already connected.");
            connectionLatch.countDown(); // Ensure latch is open if already connected
            return;
        }
        LOGGER.info("Attempting to connect to ZooKeeper at {} with session timeout {}ms", connectString, sessionTimeoutMs);
        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, this);

        // Wait for connection to establish, with a timeout
        boolean connected = connectionLatch.await(sessionTimeoutMs + 5000, TimeUnit.MILLISECONDS); // Wait a bit longer than session timeout
        if (!connected) {
            LOGGER.error("Timed out waiting for ZooKeeper connection to establish.");
            throw new IOException("Timeout connecting to ZooKeeper: " + connectString);
        }
        LOGGER.info("ZooKeeper client connection established (or timed out waiting). Current state: {}", zooKeeper.getState());
    }

    public boolean isConnected() {
        return zooKeeper != null && zooKeeper.getState().isConnected();
    }

    public ZooKeeper getZooKeeperClient() {
        if (zooKeeper == null || !zooKeeper.getState().isAlive()) {
            LOGGER.warn("ZooKeeper client accessed while not alive or null. Attempting to reconnect...");
            try {
                // Reset latch for new connection attempt
                connectionLatch = new CountDownLatch(1);
                connect();
            } catch (IOException | InterruptedException e) {
                LOGGER.error("Failed to reconnect ZooKeeper client.", e);
                Thread.currentThread().interrupt(); // Preserve interrupt status
                return null;
            }
        }
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("Received ZooKeeper event: {}", event);
        if (event.getType() == Event.EventType.None) {
            KeeperState state = event.getState();
            switch (state) {
                case SyncConnected:
                    LOGGER.info("ZooKeeper client SyncConnected.");
                    connectionLatch.countDown();
                    if (connectionListener != null) {
                        connectionListener.onZooKeeperConnected();
                    }
                    break;
                case Disconnected:
                    LOGGER.warn("ZooKeeper client Disconnected. Attempting to reconnect is handled by the ZK client library.");
                    connectionLatch = new CountDownLatch(1); // Reset latch for next connection attempt
                    if (connectionListener != null) {
                        connectionListener.onZooKeeperDisconnected();
                    }
                    break;
                case Expired:
                    LOGGER.error("ZooKeeper session Expired. This is critical. All ephemeral nodes are lost.");
                    connectionLatch = new CountDownLatch(1); // Reset latch
                    if (connectionListener != null) {
                        connectionListener.onZooKeeperSessionExpired();
                    }
                    // Re-instantiate ZooKeeper object to get a new session
                    // This could be part of a more robust reconnection strategy
                    try {
                        LOGGER.info("Attempting to create a new ZooKeeper session after expiry...");
                        close(); // Close the old expired client
                        connect(); // Attempt to establish a new session
                    } catch (IOException | InterruptedException e) {
                        LOGGER.error("Failed to re-establish ZooKeeper connection after session expiry.", e);
                        Thread.currentThread().interrupt();
                    }
                    break;
                case AuthFailed:
                    LOGGER.error("ZooKeeper authentication failed. Check credentials/ACLs.");
                    connectionLatch.countDown(); // Unblock if anyone is waiting, though connection failed
                    // Potentially notify listener of a critical failure
                    break;
                case SaslAuthenticated:
                    LOGGER.info("ZooKeeper client SASL Authenticated.");
                    // No specific action needed for connectionLatch unless it's the first successful connect signal
                    break;
                // Other states can be handled if necessary
                default:
                    LOGGER.info("ZooKeeper client received unhandled state: {}", state);
                    break;
            }
        }
        // You would also handle other event types here, e.g., NodeChildrenChanged, NodeDataChanged, if this manager
        // sets specific watchers. For a general client manager, only connection state is primary here.
    }

    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                LOGGER.info("ZooKeeper client closed.");
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while closing ZooKeeper client.", e);
                Thread.currentThread().interrupt();
            } finally {
                zooKeeper = null;
            }
        }
    }
}
