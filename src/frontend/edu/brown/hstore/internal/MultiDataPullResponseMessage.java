package edu.brown.hstore.internal;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.MultiPullReplyRequest;
import edu.brown.hstore.Hstoreservice.MultiPullReplyResponse;

public class MultiDataPullResponseMessage extends InternalMessage{
	private MultiPullReplyRequest multiPullReplyRequest;
    private  RpcCallback<MultiPullReplyResponse> multiPullReplyCallback;
    public long createTime;
    
    
    public MultiDataPullResponseMessage(MultiPullReplyRequest multiPullReplyRequest, 
    		RpcCallback<MultiPullReplyResponse> multiPullReplyCallback) {
        super();
        this.multiPullReplyRequest = multiPullReplyRequest;
        this.multiPullReplyCallback = multiPullReplyCallback;
        this.createTime = System.currentTimeMillis();
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }

    public MultiPullReplyRequest getMultiPullReplyRequest() {
        return multiPullReplyRequest;
    }

    public RpcCallback<MultiPullReplyResponse> getMultiPullReplyCallback(){
        return multiPullReplyCallback;
    }
}
