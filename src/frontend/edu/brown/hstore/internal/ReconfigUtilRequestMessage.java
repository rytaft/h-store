package edu.brown.hstore.internal;

import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.hstore.reconfiguration.ReconfigurationCoordinator.ReconfigurationState;
import edu.brown.profilers.ProfileMeasurement;

public class ReconfigUtilRequestMessage extends InternalMessage {
	public enum RequestType {
		INIT_RECONFIGURATION,
		END_RECONFIGURATION,
		QUEUE_ASYNC_REQUEST
	}
    
	private RequestType requestType;	
    private ReconfigurationPlan reconfig_plan;
	private ReconfigurationProtocols reconfig_protocol;
	private ReconfigurationState reconfig_state;
	private ExplicitPartitions planned_partitions;
	private boolean final_plan;
	public long createTime;
    public AsyncDataPullRequestMessage asyncDataPull;
	
    
    
    public ReconfigUtilRequestMessage(
    		RequestType requestType,
    		ReconfigurationPlan reconfig_plan, 
    		ReconfigurationProtocols reconfig_protocol, 
            ReconfigurationState reconfig_state, 
            ExplicitPartitions planned_partitions) {
        super();
        this.createTime = ProfileMeasurement.getTime();
        this.requestType = requestType;
        this.reconfig_plan = reconfig_plan; 
        this.reconfig_protocol = reconfig_protocol;
        this.reconfig_state = reconfig_state;
        this.planned_partitions = planned_partitions;
        this.final_plan = false;
    }
    
    public ReconfigUtilRequestMessage(RequestType requestType) {
        super();
        this.createTime = ProfileMeasurement.getTime();
        this.requestType = requestType;
        this.reconfig_plan = null; 
        this.reconfig_protocol = null;
        this.reconfig_state = null;
        this.planned_partitions = null;
        this.final_plan = false;
    }
    
    public ReconfigUtilRequestMessage(AsyncDataPullRequestMessage msg){
        this(RequestType.QUEUE_ASYNC_REQUEST);
        this.asyncDataPull = msg;
    }
    
    public ReconfigUtilRequestMessage(RequestType requestType, boolean final_plan) {
        super();
        this.createTime = ProfileMeasurement.getTime();
        this.requestType = requestType;
        this.reconfig_plan = null; 
        this.reconfig_protocol = null;
        this.reconfig_state = null;
        this.planned_partitions = null;
        this.final_plan = final_plan;
    }

    public long getQueueTime(){
        return ProfileMeasurement.getTime() - this.createTime;
    }

    public RequestType getRequestType(){
        return requestType;
    }
    
    public ReconfigurationPlan getReconfigPlan() {
    	return reconfig_plan;
    }
    
	public ReconfigurationProtocols getReconfigProtocol() {
		return reconfig_protocol;
	}
	
    public ReconfigurationState getReconfigState() {
    	return reconfig_state;
    }
    
    public ExplicitPartitions getExplicitPartitions() {
    	return planned_partitions;
    }
    
    public boolean getFinalPlan() {
    	return final_plan;
    }
    
}
