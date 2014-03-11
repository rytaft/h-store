package edu.brown.profilers;

import edu.brown.statistics.FastIntHistogram;

public class ReconfigurationProfiler extends AbstractProfiler {

    public ReconfigurationProfiler() {
        // TODO Auto-generated constructor stub
    }
    
    public ProfileMeasurement on_demand_pull_time= new ProfileMeasurement("DEMAND_PULL");
    
    //public ProfileMeasurement on_demand_pull_time= new ProfileMeasurement("DEMAND_PULL");
    
    public ProfileMeasurement async_pull_time = new ProfileMeasurement("ASYNC_PULL");
    public ProfileMeasurement async_dest_queue_time = new ProfileMeasurement("ASYNC_DEST_QUEUE_PULL");
    public ProfileMeasurement on_demand_pull_response_queue = new ProfileMeasurement("DEMAND_PULL_RESPONSE_QUEUE");
     
    public ProfileMeasurement pe_check_txn_time= new ProfileMeasurement("PE_CHECK_TXN_TIME");
    public ProfileMeasurement pe_live_pull_block_time= new ProfileMeasurement("PE_LIVE_PULL_BLOCK_TIME");

    
    public ProfileMeasurement src_data_pull_req_init_time= new ProfileMeasurement("SRC_DATA_PULL_REQUEST_INIT_TIME");
    public ProfileMeasurement src_data_pull_req_proc_time= new ProfileMeasurement("SRC_DATA_PULL_REQUEST_PROCESSING_TIME");
    public ProfileMeasurement src_extract_proc_time= new ProfileMeasurement("SRC_DATA_EXTRACT_PROCESSING_TIME");

    public int empty_loads = 0;

    public FastIntHistogram pe_block_queue_size = new FastIntHistogram(true);
    public FastIntHistogram pe_block_queue_size_growth = new FastIntHistogram(true);
    public FastIntHistogram pe_extract_queue_size_growth = new FastIntHistogram(true);
    
}
