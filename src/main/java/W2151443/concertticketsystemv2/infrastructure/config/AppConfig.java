package W2151443.concertticketsystemv2.infrastructure.config;

import W2151443.concertticketsystemv2.adapter.gateway.PropertiesConfigurationProvider;
import W2151443.concertticketsystemv2.domain.port.ConfigurationPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// For now, a simple static access or singleton pattern.
// In a DI framework, this would be an injectable bean.
public class AppConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppConfig.class);
    private static final String DEFAULT_PROPERTIES_FILE = "config.properties";
    private static ConfigurationPort configurationPort;

    // Static initializer block to load configuration when the class is loaded
    static {
        try {
            configurationPort = new PropertiesConfigurationProvider(DEFAULT_PROPERTIES_FILE);
            LOGGER.info("AppConfig initialized using {}.", DEFAULT_PROPERTIES_FILE);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize AppConfig with {}. System might be unstable or use hardcoded defaults.", DEFAULT_PROPERTIES_FILE, e);
            // Fallback or rethrow critical error if bootstrap config is absolutely necessary
            // For now, if this fails, getString() etc., will fail or return defaults from provider.
            // It's better for PropertiesConfigurationProvider to throw if the file is critical and not found.
        }
    }

    // Private constructor to prevent instantiation if using static access
    private AppConfig() {}

    public static String getString(String key) {
        if (configurationPort == null) throw new IllegalStateException("Configuration not initialized.");
        return configurationPort.getString(key);
    }

    public static String getString(String key, String defaultValue) {
        if (configurationPort == null) return defaultValue; // Or handle error
        return configurationPort.getString(key, defaultValue);
    }

    public static int getInt(String key) {
        if (configurationPort == null) throw new IllegalStateException("Configuration not initialized.");
        return configurationPort.getInt(key);
    }

    public static int getInt(String key, int defaultValue) {
        if (configurationPort == null) return defaultValue;
        return configurationPort.getInt(key, defaultValue);
    }

    public static long getLong(String key) {
        if (configurationPort == null) throw new IllegalStateException("Configuration not initialized.");
        return configurationPort.getLong(key);
    }

    public static long getLong(String key, long defaultValue) {
        if (configurationPort == null) return defaultValue;
        return configurationPort.getLong(key, defaultValue);
    }

    public static boolean getBoolean(String key) {
        if (configurationPort == null) throw new IllegalStateException("Configuration not initialized.");
        return configurationPort.getBoolean(key);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        if (configurationPort == null) return defaultValue;
        return configurationPort.getBoolean(key, defaultValue);
    }

    // Later, this AppConfig could be enhanced to:
    // - Take an etcd client to fetch dynamic configurations.
    // - Overlay dynamic configs over file-based configs.
    // - Provide methods to register listeners for config changes from etcd.
}
