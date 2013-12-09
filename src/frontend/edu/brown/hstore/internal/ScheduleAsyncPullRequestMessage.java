package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.profilers.ProfileMeasurement;

public class ScheduleAsyncPullRequestMessage extends InternalMessage {
    private ReconfigurationRange<? extends Comparable<?>> pullRange;
    public long createTime;
    public String protocol;
    
    public ScheduleAsyncPullRequestMessage(ReconfigurationRange<? extends Comparable<?>> pullRange) {
        super();
        this.pullRange = pullRange;
        this.createTime = ProfileMeasurement.getTime();
        this.protocol = "livePull";
    }

    public long getQueueTime(){
        return ProfileMeasurement.getTime() - this.createTime;
    }
    
    public ReconfigurationRange<? extends Comparable<?>> getPullRange() {
        return pullRange;
    }
    
    public void setProtocol(String protocol){
    	this.protocol = protocol;
    }
    
    public String getProtocol(){
    	return this.protocol;
    }
    
    
}
