package edu.brown.hstore.internal;

import org.apache.commons.lang.NotImplementedException;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.Hstoreservice.LivePullResponse;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.profilers.ProfileMeasurement;

public class LivePullRequestMessage extends InternalTxnMessage {

    LivePullRequest livePullRequest;
    RpcCallback<LivePullResponse> livePullResponseCallback;
    private long startTime;
    private boolean recordTime;

    public LivePullRequestMessage(AbstractTransaction ts, LivePullRequest livePullRequest, RpcCallback<LivePullResponse> livePullResponseCallback, boolean recordTime) {
        // TODO : Check whether null can be passed
        super(ts);
        this.recordTime = recordTime;
        if (recordTime)
            startTime = ProfileMeasurement.getTime();

        this.livePullRequest = livePullRequest;
        this.livePullResponseCallback = livePullResponseCallback;
    }


    
    public LivePullRequest getLivePullRequest() {
        return this.livePullRequest;
    }

    public RpcCallback<LivePullResponse> getLivePullResponseCallback() {
        return (this.livePullResponseCallback);
    }



    public synchronized long getStartTime() {
        if(recordTime)
            return startTime;
        else
            throw new NotImplementedException("No implementation for recorded without time");
    }

}
