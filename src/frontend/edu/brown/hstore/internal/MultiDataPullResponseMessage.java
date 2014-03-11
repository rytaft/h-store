package edu.brown.hstore.internal;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.MultiPullReplyRequest;
import edu.brown.hstore.Hstoreservice.MultiPullReplyResponse;
import edu.brown.profilers.ProfileMeasurement;

public class MultiDataPullResponseMessage extends InternalMessage{
	private MultiPullReplyRequest multiPullReplyRequest;
    private  RpcCallback<MultiPullReplyResponse> multiPullReplyCallback;
    public long createTime;
    
    
    public MultiDataPullResponseMessage(MultiPullReplyRequest multiPullReplyRequest, 
    		RpcCallback<MultiPullReplyResponse> multiPullReplyCallback) {
        super();
        this.multiPullReplyRequest = multiPullReplyRequest;
        this.multiPullReplyCallback = multiPullReplyCallback;
        this.createTime = ProfileMeasurement.getTime();
    }

    public long getQueueTime(){
        return ProfileMeasurement.getTime() - this.createTime;
    }

    public MultiPullReplyRequest getMultiPullReplyRequest() {
        return multiPullReplyRequest;
    }

    public RpcCallback<MultiPullReplyResponse> getMultiPullReplyCallback(){
        return multiPullReplyCallback;
    }
}
