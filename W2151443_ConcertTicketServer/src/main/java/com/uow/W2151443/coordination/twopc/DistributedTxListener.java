package com.uow.W2151443.coordination.twopc;

public interface DistributedTxListener {
    void onGlobalCommit(String transactionId);
    void onGlobalAbort(String transactionId);
}
