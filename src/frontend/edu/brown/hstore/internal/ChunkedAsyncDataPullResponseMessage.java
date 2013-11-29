package edu.brown.hstore.internal;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.ChunkedAsyncPullReplyRequest;
import edu.brown.hstore.Hstoreservice.ChunkedAsyncPullReplyResponse;

public class ChunkedAsyncDataPullResponseMessage extends InternalMessage{
	private ChunkedAsyncPullReplyRequest chunkedAsyncPullReplyRequest;
    private  RpcCallback<ChunkedAsyncPullReplyResponse> chunkedAsyncPullReplyCallback;
    public long createTime;
    
    
    public ChunkedAsyncDataPullResponseMessage(ChunkedAsyncPullReplyRequest chunkedAsyncPullReplyRequest, RpcCallback<ChunkedAsyncPullReplyResponse> chunkedAsyncPullReplyCallback) {
        super();
        this.chunkedAsyncPullReplyRequest = chunkedAsyncPullReplyRequest;
        this.chunkedAsyncPullReplyCallback = chunkedAsyncPullReplyCallback;
        this.createTime = System.currentTimeMillis();
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }

    public ChunkedAsyncPullReplyRequest getChunkedAsyncPullReplyRequest() {
        return chunkedAsyncPullReplyRequest;
    }

    public RpcCallback<ChunkedAsyncPullReplyResponse> getChunkedAsyncPullReplyCallback(){
        return chunkedAsyncPullReplyCallback;
    }
}
