package edu.brown.hstore.internal;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.Hstoreservice.LivePullResponse;

public class LivePullRequestMessage extends InternalMessage {

    LivePullRequest livePullRequest;
    RpcCallback<LivePullResponse> livePullResponseCallback;
    private long startTime;

    public LivePullRequestMessage(LivePullRequest livePullRequest, RpcCallback<LivePullResponse> livePullResponseCallback) {
        super();
        this.startTime = System.currentTimeMillis();
        this.livePullRequest = livePullRequest;
        this.livePullResponseCallback = livePullResponseCallback;
    }


    
    public LivePullRequest getLivePullRequest() {
        return this.livePullRequest;
    }

    public RpcCallback<LivePullResponse> getLivePullResponseCallback() {
        return (this.livePullResponseCallback);
    }

    public long getStartTime() {
        return startTime;
    }

}
