package edu.brown.profilers;

public class ReconfigurationProfiler extends AbstractProfiler {

    public ReconfigurationProfiler() {
        // TODO Auto-generated constructor stub
    }
    
    public ProfileMeasurement on_demand_pull_time= new ProfileMeasurement("DEMAND_PULL");
    
    //public ProfileMeasurement on_demand_pull_time= new ProfileMeasurement("DEMAND_PULL");
    
    public ProfileMeasurement async_pull_time = new ProfileMeasurement("ASYNC_PULL");
    public ProfileMeasurement async_dest_queue_time = new ProfileMeasurement("ASYNC_DEST_QUEUE_PULL");
    public ProfileMeasurement on_demand_pull_response_queue = new ProfileMeasurement("DEMAND_PULL_RESPONSE_QUEUE");
     

    public ProfileMeasurement src_data_pull_req_init_time= new ProfileMeasurement("SRC_DATA_PULL_REQUEST_INIT_TIME");
    public ProfileMeasurement src_data_pull_req_proc_time= new ProfileMeasurement("SRC_DATA_PULL_REQUEST_PROCESSING_TIME");
}
