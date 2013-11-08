package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

public class AsyncNonChunkPullRequestMessage extends InternalMessage {
    private ReconfigurationRange<? extends Comparable<?>> pullRange;
    public long createTime;
    
    public AsyncNonChunkPullRequestMessage(ReconfigurationRange<? extends Comparable<?>> pullRange) {
        super();
        this.pullRange = pullRange;
        this.createTime = System.currentTimeMillis();
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
    }
    
    public ReconfigurationRange<? extends Comparable<?>> getPullRange() {
        return pullRange;
    }
    
    
}
