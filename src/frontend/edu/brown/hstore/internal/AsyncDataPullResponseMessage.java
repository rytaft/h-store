package edu.brown.hstore.internal;

import org.voltdb.VoltTable;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.Hstoreservice.AsyncPullResponse;
import edu.brown.hstore.Hstoreservice.ReconfigurationControlRequest;

public class AsyncDataPullResponseMessage extends InternalMessage {
    private AsyncPullResponse asyncPullResponse;
    private ReconfigurationControlRequest acknowledgingCallback;
    public long createTime;
    
    
    public AsyncDataPullResponseMessage(AsyncPullResponse asyncPullResponse, ReconfigurationControlRequest acknowledgingCallback) {
        super();
        this.asyncPullResponse = asyncPullResponse;
        this.acknowledgingCallback = acknowledgingCallback;
        this.createTime = System.currentTimeMillis();
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }

    public AsyncPullResponse getAsyncPullResponse() {
        return asyncPullResponse;
    }

    public ReconfigurationControlRequest getAcknowledgingCallback() {
        return acknowledgingCallback;
    }
    
    
}
