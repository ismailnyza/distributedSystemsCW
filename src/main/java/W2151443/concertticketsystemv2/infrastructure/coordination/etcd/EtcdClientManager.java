package W2151443.concertticketsystemv2.infrastructure.coordination.etcd;

import W2151443.concertticketsystemv2.infrastructure.config.AppConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.options.PutOption;
//import io.etcd.jetcd.PutOption;
import io.etcd.jetcd.kv.PutResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class EtcdClientManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdClientManager.class);
    private Client client;
    private final String[] endpoints;
    private long leaseId = -1; // Lease ID for ephemeral-like registrations
    private ScheduledExecutorService leaseKeepAliveExecutor;

    public EtcdClientManager(String etcdEndpoints) {
        this.endpoints = etcdEndpoints.split(",");
        // Sanitize endpoints
        for (int i = 0; i < this.endpoints.length; i++) {
            this.endpoints[i] = this.endpoints[i].trim();
        }
    }

    public void connect() {
        if (this.client == null) {
            LOGGER.info("Connecting to etcd endpoints: {}", String.join(",", endpoints));
            try {
                this.client = Client.builder().endpoints(endpoints).build();
                LOGGER.info("Successfully connected to etcd.");
            } catch (Exception e) {
                LOGGER.error("Failed to connect to etcd: {}", e.getMessage(), e);
                // Depending on policy, could throw a runtime exception or allow retries
                throw new RuntimeException("Failed to initialize etcd client", e);
            }
        }
    }

    public Client getClient() {
        if (client == null) {
            // Or throw an IllegalStateException if connect() must be called first
            LOGGER.warn("Etcd client accessed before connect() was called or after close(). Attempting to connect.");
            connect(); // Attempt to connect if not already
        }
        return client;
    }

    /**
     * Registers a key with a value and associates it with a TTL (lease).
     * Starts a keep-alive for the lease.
     *
     * @param key          The key to register.
     * @param value        The value for the key.
     * @param ttlSeconds   Time-to-live in seconds for the lease.
     * @return true if registration and lease keep-alive started successfully, false otherwise.
     */
    public boolean registerWithLease(String key, String value, long ttlSeconds) {
        if (getClient() == null) {
            LOGGER.error("Cannot register key '{}': etcd client is not connected.", key);
            return false;
        }

        try {
            // 1. Grant a lease
            Lease leaseClient = getClient().getLeaseClient();
            this.leaseId = leaseClient.grant(ttlSeconds).get().getID(); // Blocks until lease is granted
            LOGGER.info("Granted lease with ID: {} for TTL: {} seconds, for key: {}", leaseId, ttlSeconds, key);

            // 2. Put the key with the lease
            ByteSequence bsKey = ByteSequence.from(key, StandardCharsets.UTF_8);
            ByteSequence bsValue = ByteSequence.from(value, StandardCharsets.UTF_8);
            PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
            CompletableFuture<PutResponse> putFuture = getClient().getKVClient().put(bsKey, bsValue, putOption);
            putFuture.get(); // Blocks until put is complete
            LOGGER.info("Successfully registered key '{}' with value '{}' under lease ID {}.", key, value, leaseId);

            // 3. Start keep-alive for the lease
            // Note: jetcd's LeaseClient has a keepAlive method that handles this,
            // but it requires careful management of the observer and potential reconnects.
            // For simplicity, a manual periodic refresh can be implemented,
            // or use the client's built-in keepAlive if comfortable with its API.
            // A simpler approach is to re-grant and re-put if keepAlive is complex to manage across reconnects.
            // However, the proper way is to use leaseClient.keepAlive(leaseId, observer).
            // For robustness in this example, we will use a scheduled task to periodically call keepAliveOnce.
            // This is less efficient than the streaming keepAlive but simpler to show here.

            if (leaseKeepAliveExecutor != null && !leaseKeepAliveExecutor.isShutdown()) {
                leaseKeepAliveExecutor.shutdownNow();
            }
            leaseKeepAliveExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "etcd-lease-keep-alive-" + Long.toHexString(leaseId));
                t.setDaemon(true);
                return t;
            });

            // Keep alive slightly more frequently than half the TTL to be safe
            long keepAlivePeriodSeconds = Math.max(1, ttlSeconds / 2 -1); // Ensure at least 1 second
            if (ttlSeconds <=2) keepAlivePeriodSeconds =1;


            leaseKeepAliveExecutor.scheduleAtFixedRate(() -> {
                try {
                    if (this.client != null && this.leaseId != -1) {
                        //LOGGER.debug("Sending keep-alive for lease ID: {}", this.leaseId);
                        leaseClient.keepAliveOnce(this.leaseId).get(); // Blocks
                        //LOGGER.debug("Keep-alive successful for lease ID: {}", this.leaseId);
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Lease keep-alive thread interrupted for lease ID: {}", this.leaseId);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOGGER.error("Failed to send keep-alive for lease ID: {}. Underlying cause: {}", this.leaseId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    // This could mean the lease expired or etcd connection issue.
                    // The registration might become invalid. A robust system would try to re-register.
                    // For now, we log. The node might appear offline if this fails consistently.
                }
            }, 0, keepAlivePeriodSeconds, TimeUnit.SECONDS); // Initial delay 0, then periodic

            LOGGER.info("Started keep-alive for lease ID {} every {} seconds.", this.leaseId, keepAlivePeriodSeconds);
            return true;

        } catch (InterruptedException e) {
            LOGGER.error("Registration for key '{}' interrupted: {}", key, e.getMessage());
            Thread.currentThread().interrupt();
            revokeCurrentLease(); // Clean up lease if registration process was interrupted
        } catch (ExecutionException e) {
            LOGGER.error("Failed to register key '{}' or grant lease: {}", key, e.getMessage(), e.getCause());
            revokeCurrentLease();
        }
        return false;
    }

    /**
     * Deregisters a key by revoking its associated lease.
     * Also stops the keep-alive scheduler.
     */
    public void deregister() {
        if (leaseKeepAliveExecutor != null) {
            leaseKeepAliveExecutor.shutdown();
            try {
                if (!leaseKeepAliveExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    leaseKeepAliveExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                leaseKeepAliveExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOGGER.info("Stopped lease keep-alive scheduler for lease ID: {}", leaseId);
        }
        revokeCurrentLease();
    }

    private void revokeCurrentLease() {
        if (this.client != null && this.leaseId != -1) {
            try {
                getClient().getLeaseClient().revoke(this.leaseId).get();
                LOGGER.info("Successfully revoked lease ID: {}", this.leaseId);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while revoking lease ID: {}", this.leaseId, e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.warn("Failed to revoke lease ID: {}. It might have already expired.", this.leaseId, e.getCause());
            } finally {
                this.leaseId = -1; // Invalidate the lease ID
            }
        }
    }

    public void close() {
        LOGGER.info("Closing etcd client connection...");
        deregister(); // Ensure lease is revoked and keep-alive stopped
        if (client != null) {
            try {
                client.close(); // This closes all sub-clients like KV, Lease, Watch
                LOGGER.info("Etcd client closed.");
            } catch (Exception e) {
                LOGGER.error("Error closing etcd client: {}", e.getMessage(), e);
            } finally {
                client = null;
            }
        }
    }
}
