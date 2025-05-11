package W2151443.concertticketsystemv2.domain.port;

public interface ConfigurationPort {
    String getString(String key);
    String getString(String key, String defaultValue);

    int getInt(String key);
    int getInt(String key, int defaultValue);

    long getLong(String key);
    long getLong(String key, long defaultValue);

    boolean getBoolean(String key);
    boolean getBoolean(String key, boolean defaultValue);

    // Potentially methods to reload or get dynamic properties later
}
