package W2151443.concertticketsystemv2.adapter.gateway;

import W2151443.concertticketsystemv2.domain.port.ConfigurationPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesConfigurationProvider implements ConfigurationPort {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfigurationProvider.class);
    private final Properties properties;
    private final String propertiesFileName;

    public PropertiesConfigurationProvider(String fileName) {
        this.propertiesFileName = fileName;
        this.properties = new Properties();
        loadProperties();
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(propertiesFileName)) {
            if (input == null) {
                LOGGER.error("Sorry, unable to find " + propertiesFileName);
                // Depending on how critical this is, you might throw a runtime exception
                // or proceed with an empty properties object (and rely on defaults).
                // For bootstrap config, it's usually critical.
                throw new RuntimeException("Configuration file '" + propertiesFileName + "' not found in classpath.");
            }
            properties.load(input);
            LOGGER.info("Successfully loaded configuration from {}", propertiesFileName);
        } catch (IOException ex) {
            LOGGER.error("Error loading configuration file " + propertiesFileName, ex);
            // Again, decide on critical failure or proceed with empty/defaults.
            throw new RuntimeException("Error loading configuration file: " + propertiesFileName, ex);
        }
    }

    @Override
    public String getString(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            LOGGER.warn("Configuration key '{}' not found in {}.", key, propertiesFileName);
            // Or throw new ConfigurationMissingException(key);
        }
        return value;
    }

    @Override
    public String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    @Override
    public int getInt(String key) {
        String value = getString(key);
        if (value == null) {
            // Or throw
            throw new NumberFormatException("Configuration key '" + key + "' not found for int conversion.");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.error("Invalid integer format for key '{}', value: '{}' in {}.", key, value, propertiesFileName);
            throw e;
        }
    }

    @Override
    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid integer format for key '{}', value: '{}'. Using default value: {}.", key, value, defaultValue);
            return defaultValue;
        }
    }

    @Override
    public long getLong(String key) {
        String value = getString(key);
        if (value == null) {
            throw new NumberFormatException("Configuration key '" + key + "' not found for long conversion.");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOGGER.error("Invalid long format for key '{}', value: '{}' in {}.", key, value, propertiesFileName);
            throw e;
        }
    }

    @Override
    public long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid long format for key '{}', value: '{}'. Using default value: {}.", key, value, defaultValue);
            return defaultValue;
        }
    }

    @Override
    public boolean getBoolean(String key) {
        String value = getString(key);
        if (value == null) {
            // Or throw
            throw new IllegalArgumentException("Configuration key '" + key + "' not found for boolean conversion.");
        }
        return Boolean.parseBoolean(value); // "true" (case-insensitive) is true, everything else is false
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        // Boolean.parseBoolean is quite lenient, "true" (any case) is true, otherwise false.
        return Boolean.parseBoolean(value);
    }
}
