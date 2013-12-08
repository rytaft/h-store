package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

public class ScheduleAsyncPullRequestMessage extends InternalMessage {
    private ReconfigurationRange<? extends Comparable<?>> pullRange;
    public long createTime;
    public String protocol;
    
    public ScheduleAsyncPullRequestMessage(ReconfigurationRange<? extends Comparable<?>> pullRange) {
        super();
        this.pullRange = pullRange;
        this.createTime = System.currentTimeMillis();
        this.protocol = "livePull";
    }

    public long getQueueTime(){
        return System.currentTimeMillis() - this.createTime;
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
