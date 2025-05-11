package com.uow.W2151443.discovery;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class W2151443EtcdNameServiceClient {

    private static final Logger logger = Logger.getLogger(W2151443EtcdNameServiceClient.class.getName());
    private final String etcdUrl; // e.g., "http://localhost:2381"
    private final ScheduledExecutorService heartbeatScheduler;
    // Key: fullEtcdPath (e.g. http://localhost:2381/v2/keys/services/Svc/node1), Value: original TTL for re-registration if needed
    private final Map<String, Integer> registeredServicesWithTtl = new HashMap<>();

    public W2151443EtcdNameServiceClient(String etcdUrl) {
        if (etcdUrl.endsWith("/")) {
            this.etcdUrl = etcdUrl.substring(0, etcdUrl.length() - 1);
        } else {
            this.etcdUrl = etcdUrl;
        }
        // Using daemon threads so they don't prevent JVM shutdown
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        startHeartbeatTask();
    }

    private String buildV2KeyPath(String key) {
        if (!key.startsWith("/")) {
            key = "/" + key;
        }
        return this.etcdUrl + "/v2/keys" + key;
    }

    public boolean registerService(String serviceBasePath, String serviceInstanceId, String ip, int port, int ttlSeconds) {
        String key = serviceBasePath + "/" + serviceInstanceId;
        JSONObject valueJson = new JSONObject();
        valueJson.put("ip", ip);
        valueJson.put("port", port);
        valueJson.put("id", serviceInstanceId);
        // valueJson.put("timestamp", System.currentTimeMillis()); // Optional metadata

        String fullEtcdPath = buildV2KeyPath(key);
        logger.info("Registering service instance '" + serviceInstanceId + "' at etcd path: " + fullEtcdPath + " with TTL: " + ttlSeconds + "s");

        try {
            URL url = new URI(fullEtcdPath).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            String data = "value=" + URLEncoder.encode(valueJson.toString(), StandardCharsets.UTF_8.name()) +
                    "&ttl=" + ttlSeconds;

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = data.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
                logger.info("Service instance '" + serviceInstanceId + "' registered successfully. Response: " + responseCode);
                synchronized (registeredServicesWithTtl) {
                    registeredServicesWithTtl.put(fullEtcdPath, ttlSeconds); // Store the path and original TTL
                }
                return true;
            } else {
                logger.severe("Failed to register service instance '" + serviceInstanceId + "'. HTTP error code: " + responseCode + " - " + conn.getResponseMessage());
                logErrorStream(conn);
                return false;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception during service registration for '" + serviceInstanceId + "'", e);
            return false;
        }
    }

    private boolean refreshServiceRegistration(String fullEtcdKeyUrl, int ttlSeconds) {
        // For etcd v2, refreshing TTL for a key (not a directory) is done by a PUT request
        // with `prevExist=true` and the `ttl` parameter. No `value` is needed if only refreshing.
        logger.fine("Refreshing service registration (heartbeat) for etcd path: " + fullEtcdKeyUrl + " with new TTL: " + ttlSeconds + "s");
        try {
            URL url = new URI(fullEtcdKeyUrl).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // To refresh TTL for an existing key (not directory):
//            String data = "ttl=" + ttlSeconds + "&prevExist=true";
            String data = "ttl=" + ttlSeconds + "&prevExist=true&refresh=true"; // Added refresh=true
            // Note: some etcd versions might also need `refresh=true` but `prevExist=true` should suffice for a key.
            // If we were updating the value as well, we would include it.
            // To be absolutely safe against the key potentially being a dir if not created with value:
            // String data = "value=" + URLEncoder.encode("{\"heartbeat\":\"" + System.currentTimeMillis() + "\"}", StandardCharsets.UTF_8.name()) +
            //               "&ttl=" + ttlSeconds + "&prevExist=true";
            // But if it's a key, just updating TTL should be enough.

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = data.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // logger.fine("Service registration TTL refreshed for path: " + fullEtcdKeyUrl);
                return true;
            } else {
                logger.warning("Failed to refresh service registration TTL for '" + fullEtcdKeyUrl + "'. HTTP error code: " + responseCode + " - " + conn.getResponseMessage());
                logErrorStream(conn);
                return false; // Indicates heartbeat failed, server might need to re-register fully
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception during service registration TTL refresh for '" + fullEtcdKeyUrl + "'", e);
            return false;
        }
    }

    private void startHeartbeatTask() {
        // Heartbeat interval should be < TTL / 2, e.g., TTL/3.
        // If TTL is 15s, heartbeat every 5s. If TTL is 30s, heartbeat every 10s.
        long heartbeatIntervalSeconds = 5; // Default

        heartbeatScheduler.scheduleAtFixedRate(() -> {
            synchronized (registeredServicesWithTtl) {
                if (registeredServicesWithTtl.isEmpty()) {
                    return;
                }
                // Create a copy to avoid ConcurrentModificationException if deregister happens during iteration
                new HashMap<>(registeredServicesWithTtl).forEach((fullEtcdPath, originalTtl) -> {
                    boolean success = refreshServiceRegistration(fullEtcdPath, originalTtl);
                    if (!success) {
                        logger.warning("Heartbeat failed for " + fullEtcdPath + ". Service might have expired or etcd is unreachable. Server should attempt re-registration.");
                        // Potentially remove from local map if consistently failing, forcing server to re-register.
                        // registeredServicesWithTtl.remove(fullEtcdPath); // Be careful with this, could cause loop if server re-registers immediately
                    }
                });
            }
        }, heartbeatIntervalSeconds, heartbeatIntervalSeconds, TimeUnit.SECONDS);
        logger.info("Etcd heartbeat task scheduled to run every " + heartbeatIntervalSeconds + " seconds.");
    }

    public List<ServiceInstance> discoverServiceInstances(String serviceBasePath) {
        List<ServiceInstance> instances = new ArrayList<>();
        String discoveryPath = buildV2KeyPath(serviceBasePath);
        // For directory listing, etcd v2 API expects no trailing slash for the dir itself,
        // or if it has one, it might behave differently. ?recursive=true is key.
        String fullEtcdPathForDiscovery = discoveryPath + "?recursive=true";

        logger.info("Discovering service instances under etcd path: " + fullEtcdPathForDiscovery);

        try {
            URL url = new URI(fullEtcdPathForDiscovery).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                StringBuilder response = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                }
                JSONObject jsonResponse = new JSONObject(response.toString());
                if (jsonResponse.has("node")) {
                    JSONObject baseNode = jsonResponse.getJSONObject("node");
                    if (baseNode.optBoolean("dir", false) && baseNode.has("nodes")) {
                        JSONArray serviceNodes = baseNode.getJSONArray("nodes");
                        for (int i = 0; i < serviceNodes.length(); i++) {
                            JSONObject serviceNode = serviceNodes.getJSONObject(i);
                            if (serviceNode.has("value") && !serviceNode.optBoolean("dir", false)) { // Ensure it's a key, not a sub-directory
                                try {
                                    JSONObject serviceValue = new JSONObject(serviceNode.getString("value"));
                                    String id = serviceValue.optString("id", serviceNode.getString("key").substring(serviceNode.getString("key").lastIndexOf('/') + 1));
                                    instances.add(new ServiceInstance(
                                            id,
                                            serviceValue.getString("ip"),
                                            serviceValue.getInt("port")
                                    ));
                                } catch (JSONException e) {
                                    logger.warning("Failed to parse service instance value: " + serviceNode.optString("value") + " for key " + serviceNode.getString("key") + " - Error: " + e.getMessage());
                                }
                            }
                        }
                    } else if (!baseNode.optBoolean("dir", false) && baseNode.has("value")) {
                        // Case where serviceBasePath points directly to a single service key (not a directory)
                        try {
                            JSONObject serviceValue = new JSONObject(baseNode.getString("value"));
                            String id = serviceValue.optString("id", baseNode.getString("key").substring(baseNode.getString("key").lastIndexOf('/') + 1));
                            instances.add(new ServiceInstance(
                                    id,
                                    serviceValue.getString("ip"),
                                    serviceValue.getInt("port")
                            ));
                        } catch (JSONException e) {
                            logger.warning("Failed to parse single service instance value: " + baseNode.optString("value") + " for key " + baseNode.getString("key") + " - Error: " + e.getMessage());
                        }
                    }
                }
                logger.info("Found " + instances.size() + " instances for service base path: " + serviceBasePath);
            } else if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                JSONObject errorJson = null;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) { errorResponse.append(responseLine.trim()); }
                    errorJson = new JSONObject(errorResponse.toString());
                } catch (Exception e) { /* ignore, just log main error */ }

                if (errorJson != null && errorJson.optInt("errorCode", 0) == 100) { // Key not found
                    logger.info("No service instances found under base path (etcd errorCode 100 - Key not found): " + serviceBasePath);
                } else {
                    logger.warning("Failed to discover services (HTTP " + responseCode + ") for path '" + serviceBasePath + "'. Response: " + (errorJson != null ? errorJson.toString() : conn.getResponseMessage()));
                }
            } else {
                logger.severe("Failed to discover services under '" + serviceBasePath + "'. HTTP error code: " + responseCode + " - " + conn.getResponseMessage());
                logErrorStream(conn);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception during service discovery for '" + serviceBasePath + "'", e);
        }
        return instances;
    }

    public boolean deregisterService(String serviceBasePath, String serviceInstanceId) {
        String key = serviceBasePath + "/" + serviceInstanceId;
        String fullEtcdPath = buildV2KeyPath(key);
        logger.info("Deregistering service instance '" + serviceInstanceId + "' from etcd path: " + fullEtcdPath);

        synchronized (registeredServicesWithTtl) {
            registeredServicesWithTtl.remove(fullEtcdPath); // Stop trying to heartbeat it
        }

        try {
            URL url = new URI(fullEtcdPath).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                logger.info("Service instance '" + serviceInstanceId + "' deregistered successfully.");
                return true;
            } else {
                logger.severe("Failed to deregister service instance '" + serviceInstanceId + "'. HTTP error code: " + responseCode + " - " + conn.getResponseMessage());
                logErrorStream(conn);
                return false;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception during service deregistration for '" + serviceInstanceId + "'", e);
            return false;
        }
    }

    private void logErrorStream(HttpURLConnection conn) {
        if (conn == null) return;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
            StringBuilder errorResponse = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                errorResponse.append(responseLine.trim());
            }
            if (errorResponse.length() > 0) {
                logger.severe("Error details from etcd: " + errorResponse.toString());
            }
        } catch (Exception e) {
            // This might happen if there's no error stream (e.g., connection already closed or other network issue)
            logger.log(Level.WARNING, "Could not read error stream from etcd connection: " + e.getMessage(), e);
        }
    }

    public void shutdownHeartbeat() {
        logger.info("Shutting down etcd client heartbeat scheduler...");
        heartbeatScheduler.shutdown();
        try {
            if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatScheduler.shutdownNow();
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.severe("Heartbeat scheduler did not terminate.");
                }
            }
        } catch (InterruptedException e) {
            heartbeatScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Etcd client heartbeat scheduler shut down.");
    }

    // Simple class to hold discovered service instance details
    public static class ServiceInstance {
        private final String id;
        private final String ip;
        private final int port;

        public ServiceInstance(String id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }
        public String getId() { return id; }
        public String getIp() { return ip; }
        public int getPort() { return port; }
        @Override
        public String toString() {
            return "ServiceInstance{" + "id='" + id + '\'' + ", ip='" + ip + '\'' + ", port=" + port + '}';
        }
    }

    // Main method for quick testing
    public static void main(String[] args) throws InterruptedException {
        // Ensure etcd v3.3.27 is running and HOST_CLIENT_PORT in run_etcd.sh is, for example, 2381
        W2151443EtcdNameServiceClient client = new W2151443EtcdNameServiceClient("http://localhost:2381");
        String basePath = "/services/W2151443TestServiceV2";
        String instanceId1 = "node_test_v2_1";
        String instanceId2 = "node_test_v2_2";
        int testTTL = 15; // seconds

        System.out.println("--- Test: Registering node_test_v2_1 ---");
        boolean reg1 = client.registerService(basePath, instanceId1, "127.0.0.1", 9091, testTTL);
        System.out.println("Registration 1 success: " + reg1);

        System.out.println("\n--- Test: Registering node_test_v2_2 ---");
        boolean reg2 = client.registerService(basePath, instanceId2, "127.0.0.1", 9092, testTTL);
        System.out.println("Registration 2 success: " + reg2);

        System.out.println("\n--- Test: Discovering after 2s (should find 2) ---");
        Thread.sleep(2000);
        List<ServiceInstance> instances = client.discoverServiceInstances(basePath);
        System.out.println("Discovered instances count: " + instances.size());
        instances.forEach(System.out::println);

        System.out.println("\n--- Test: Waiting for " + (testTTL / 3) + "s (heartbeat should run) ---");
        Thread.sleep( (long)testTTL * 1000 / 3);
        instances = client.discoverServiceInstances(basePath);
        System.out.println("Discovered instances count after first heartbeat interval: " + instances.size());
        instances.forEach(System.out::println);


        System.out.println("\n--- Test: Waiting for " + (testTTL + 5) + "s (services should expire if heartbeat failed, or persist if it worked) ---");
        Thread.sleep((long)(testTTL + 5) * 1000);
        List<ServiceInstance> instancesAfterWait = client.discoverServiceInstances(basePath);
        System.out.println("Discovered instances count after TTL+5s: " + instancesAfterWait.size());
        instancesAfterWait.forEach(System.out::println);

        System.out.println("\n--- Test: Deregistering node_test_v2_1 ---");
        client.deregisterService(basePath, instanceId1);
        instances = client.discoverServiceInstances(basePath);
        System.out.println("Discovered instances count after deregistering node1: " + instances.size());
        instances.forEach(System.out::println);


        System.out.println("\n--- Test: Shutting down heartbeat ---");
        client.shutdownHeartbeat();
        System.out.println("--- Test: End ---");
    }
}
