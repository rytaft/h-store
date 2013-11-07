package edu.brown.hstore.internal;

import org.voltdb.VoltTable;

import com.google.protobuf.RpcCallback;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.Hstoreservice.AsyncPullRequest;
import edu.brown.hstore.Hstoreservice.AsyncPullResponse;

public class AsyncDataPullRequestMessage extends InternalMessage {
    private RpcCallback<AsyncPullResponse> asyncPullRequestCallback;
    private AsyncPullRequest asyncPullRequest;
    private int chunk;
    public long createTime;
    
    
    public AsyncDataPullRequestMessage(AsyncPullRequest asyncPullRequest, RpcCallback<AsyncPullResponse> asyncPullRequestCallback) {
        super();
        this.asyncPullRequest = asyncPullRequest;
        this.asyncPullRequestCallback = asyncPullRequestCallback;
        this.createTime = System.currentTimeMillis();
        this.chunk = 0;
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }

    public RpcCallback<AsyncPullResponse> getAsyncPullRequestCallback() {
        return asyncPullRequestCallback;
    }

    public AsyncPullRequest getAsyncPullRequest() {
        return asyncPullRequest;
    }
    
    public int getAndIncrementChunk(){
        int t = chunk;
        chunk++;
        return t;
    }
}
