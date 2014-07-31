package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.profilers.ProfileMeasurement;

public class AsyncNonChunkPullRequestMessage extends InternalMessage {
    private ReconfigurationRange pullRange;
    public long createTime;
    
    public AsyncNonChunkPullRequestMessage(ReconfigurationRange pullRange) {
        super();
        this.pullRange = pullRange;
        this.createTime = ProfileMeasurement.getTime();
    }

    public long getQueueTime(){
        return ProfileMeasurement.getTime() - this.createTime;
    }
    
    public ReconfigurationRange getPullRange() {
        return pullRange;
    }
    
    
}
