package edu.brown.hstore.internal;

import org.voltdb.VoltTable;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.Hstoreservice.AsyncPullResponse;

public class AsyncDataPullResponseMessage extends InternalMessage {
    private AsyncPullResponse asyncPullResponse;
    public long createTime;
    
    
    public AsyncDataPullResponseMessage(AsyncPullResponse asyncPullResponse) {
        super();
        this.asyncPullResponse = asyncPullResponse;
        this.createTime = System.currentTimeMillis();
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }

    public AsyncPullResponse getAsyncPullResponse() {
        return asyncPullResponse;
    }
}
