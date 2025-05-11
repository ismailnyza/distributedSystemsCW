package W2151443.concertticketsystemv2.infrastructure.coordination.zk;

public interface ZooKeeperConnectionListener {
    void onZooKeeperConnected();
    void onZooKeeperDisconnected();
    void onZooKeeperSessionExpired();
}
