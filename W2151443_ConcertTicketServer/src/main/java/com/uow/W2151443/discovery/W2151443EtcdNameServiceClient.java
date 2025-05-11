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
    private final Map<String, Integer> registeredServicesWithTtl = new HashMap<>(); // Key: fullEtcdPath, Value: ttl

    public W2151443EtcdNameServiceClient(String etcdUrl) {
        if (etcdUrl.endsWith("/")) {
            this.etcdUrl = etcdUrl.substring(0, etcdUrl.length() - 1);
        } else {
            this.etcdUrl = etcdUrl;
        }
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
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
        // Add any other metadata if needed
        // valueJson.put("lastHeartbeat", System.currentTimeMillis());

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
                    registeredServicesWithTtl.put(fullEtcdPath, ttlSeconds);
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

    public boolean refreshServiceRegistration(String fullEtcdPath, int ttlSeconds) {
        // For etcd v2, refreshing TTL is done by updating the key with a new TTL,
        // but without providing the 'value' parameter and setting 'prevExist=true'.
        // Or, simply re-PUT with the value and new TTL. We'll do a re-PUT.
        // To be more precise, one could fetch the current value first.
        // For simplicity here, we will assume the value doesn't change during heartbeat.
        // A more robust way would be to fetch current value then update with new TTL.
        // Or, a PUT with "prevExist=true" and only the ttl parameter.
        // Let's try with prevExist=true and only ttl.

        logger.fine("Refreshing service registration (heartbeat) for etcd path: " + fullEtcdPath + " with TTL: " + ttlSeconds + "s");
        try {
            URL url = new URI(fullEtcdPath).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // To refresh TTL, we must ensure the key exists and only update TTL
            // We also need to provide a value, otherwise etcd v2 interprets it as directory creation.
            // A common way is to re-PUT the existing value with a new TTL.
            // Or, use `prevExist=true` and `refresh=true` (if supported and correctly used)
            // Let's re-PUT the value, as it's more reliable for basic TTL refresh.
            // This requires fetching the current value first if we don't want to assume it.
            // For now, let's try a simpler PUT focusing on updating TTL, by providing a value field.
            // This means the value needs to be known or fetched.
            // A simpler heartbeat for etcd v2 with TTL is to just re-register with the same value and new TTL.
            // The 'registerService' method is complex to call here without all original params.

            // Correct way to refresh TTL for a key, ensuring it's not a directory operation
            String data = "ttl=" + ttlSeconds + "&refresh=true&prevExist=true"; // this tells etcd to refresh the ttl of an existing key

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = data.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // logger.fine("Service registration refreshed for path: " + fullEtcdPath);
                return true;
            } else {
                logger.warning("Failed to refresh service registration for '" + fullEtcdPath + "'. HTTP error code: " + responseCode + " - " + conn.getResponseMessage());
                logErrorStream(conn);
                // If refresh fails (e.g. key expired), try to re-register fully. This is complex, handle outside.
                return false;
            }

        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception during service registration refresh for '" + fullEtcdPath + "'", e);
            return false;
        }
    }


    private void startHeartbeatTask() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            synchronized (registeredServicesWithTtl) {
                if (registeredServicesWithTtl.isEmpty()) {
                    return;
                }
                logger.fine("Running heartbeat task for " + registeredServicesWithTtl.size() + " registered services...");
                new HashMap<>(registeredServicesWithTtl).forEach((path, ttl) -> {
                    // Refresh with a slightly shorter interval than registration to be safe
                    boolean success = refreshServiceRegistration(path, ttl);
                    if (!success) {
                        logger.warning("Heartbeat failed for " + path + ". Service might need re-registration if it expired.");
                        // Optionally, could attempt a full re-register if it's known the key should exist
                        // For now, just log. The service itself should handle re-registration logic on prolonged failure.
                    }
                });
            }
        }, 5, 5, TimeUnit.SECONDS); // Initial delay 5s, repeat every 5s. TTL should be > 10-15s.
    }


    public List<ServiceInstance> discoverServiceInstances(String serviceBasePath) {
        List<ServiceInstance> instances = new ArrayList<>();
        String fullEtcdPath = buildV2KeyPath(serviceBasePath) + "?recursive=true"; // Get all keys under the base path
        logger.info("Discovering service instances under etcd path: " + fullEtcdPath);

        try {
            URL url = new URI(fullEtcdPath).toURL();
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
                if (jsonResponse.has("node") && jsonResponse.getJSONObject("node").has("nodes")) {
                    JSONArray nodes = jsonResponse.getJSONObject("node").getJSONArray("nodes");
                    for (int i = 0; i < nodes.length(); i++) {
                        JSONObject serviceNode = nodes.getJSONObject(i);
                        if (serviceNode.has("value")) {
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
                }
                logger.info("Found " + instances.size() + " instances for service base path: " + serviceBasePath);
            } else if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                logger.info("No service instances found under base path (404): " + serviceBasePath);
            }
            else {
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

        try {
            URL url = new URI(fullEtcdPath).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                logger.info("Service instance '" + serviceInstanceId + "' deregistered successfully.");
                synchronized (registeredServicesWithTtl) {
                    registeredServicesWithTtl.remove(fullEtcdPath);
                }
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
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
            StringBuilder errorResponse = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                errorResponse.append(responseLine.trim());
            }
            logger.severe("Error details from etcd: " + errorResponse.toString());
        } catch (Exception e) {
            logger.warning("Could not read error stream from etcd connection: " + e.getMessage());
        }
    }


    public void shutdownHeartbeat() {
        logger.info("Shutting down etcd client heartbeat scheduler...");
        heartbeatScheduler.shutdown();
        try {
            if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatScheduler.shutdownNow();
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

    public static void main(String[] args) throws InterruptedException {
    W2151443EtcdNameServiceClient client = new W2151443EtcdNameServiceClient("http://localhost:2381");
    String basePath = "/services/W2151443TestService";
    String instanceId1 = "node_test_1";
    String instanceId2 = "node_test_2";

    boolean reg1 = client.registerService(basePath, instanceId1, "127.0.0.1", 9091, 15); // 15s TTL
    System.out.println("Registration 1 success: " + reg1);
    boolean reg2 = client.registerService(basePath, instanceId2, "127.0.0.1", 9092, 15);
    System.out.println("Registration 2 success: " + reg2);

    Thread.sleep(5000); // Wait for heartbeats to kick in or for discovery

    List<ServiceInstance> instances = client.discoverServiceInstances(basePath);
    System.out.println("Discovered instances: " + instances.size());
    instances.forEach(System.out::println);

    Thread.sleep(20000); // Wait longer than TTL to see if it expires without heartbeat (if you stop heartbeat)
                         // Or just to see heartbeat working

    List<ServiceInstance> instancesAfterWait = client.discoverServiceInstances(basePath);
    System.out.println("Discovered instances after wait: " + instancesAfterWait.size());
    instancesAfterWait.forEach(System.out::println);


    client.deregisterService(basePath, instanceId1);
    client.deregisterService(basePath, instanceId2);
    client.shutdownHeartbeat();
}
}
