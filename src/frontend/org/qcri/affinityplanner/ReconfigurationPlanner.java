package org.qcri.affinityplanner;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.qcri.affinityplanner.Plan.Range;

public class ReconfigurationPlanner implements Partitioner {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPlanner.class);

    private Plan plan;
    private int partitions_before;
    private int partitions_after;
    
    // Takes an existing plan and the number of partitions specified by partitions_after to
    // come up with a new plan
    //
    // This partitioner assumes that the tuples are divided evenly across partitions in the 
    // existing plan, and that the workload is uniform across tuples.  It finds a new plan in
    // which total data movement is minimized while still spreading tuples evenly across
    // the partitions
    public ReconfigurationPlanner (File planFile, int partitions_after) {
        this.plan = new Plan(planFile.toString());
        this.partitions_before = this.plan.getAllPartitions().size();
        this.partitions_after = partitions_after;
    }
    
    public ReconfigurationPlanner (String planString, int partitions_after) {
        this.plan = new Plan(planString, "");
        this.partitions_before = this.plan.getAllPartitions().size();
        this.partitions_after = partitions_after;
    }
        
    @Override
    public void writePlan(String plan_out) {
        plan.toJSON(plan_out);
    }
    
    public String getPlan() throws JSONException {
        return plan.toJSONObject().toString(2);
    }
    
    public boolean repartition() {
        if (this.partitions_before < this.partitions_after) { // scale out
            for(String table : plan.table_names){
                long data_moving_out_per_partition = (long) (keysPerTable(table) * (1.0/this.partitions_before - 1.0/this.partitions_after));
                int num_new_partitions = this.partitions_after - this.partitions_before;
                long sliceWidth = (long) Math.ceil((double) data_moving_out_per_partition / num_new_partitions);
                
                // each old partition will be giving data to each new partition
                for (int old_part = 0; old_part < this.partitions_before; ++old_part) {
                    List<List<Range>> chunks = plan.getRangeChunks(table, old_part, sliceWidth);
                    for (int new_part = this.partitions_before; 
                            new_part < this.partitions_after && new_part - this.partitions_before < chunks.size();
                            ++new_part) {
                        
                        for(Range movedRange : chunks.get(new_part - this.partitions_before)) {
                            plan.moveColdRange(table, movedRange, old_part, new_part);
                        }
                    }
                }            
            }
        }
        else if (this.partitions_before > this.partitions_after) { // scale in
            for(String table : plan.table_names){
                long data_moving_out_per_partition = (long) (keysPerTable(table) * (1.0/this.partitions_before));
                long sliceWidth = (long) Math.ceil((double) data_moving_out_per_partition / this.partitions_after);
                
                // each old partition that is going away will be giving data to each partition that is staying
                for (int old_part = this.partitions_after; old_part < this.partitions_before; ++old_part) {
                    List<List<Range>> chunks = plan.getRangeChunks(table, old_part, sliceWidth);
                    for (int new_part = 0; new_part < this.partitions_after && new_part < chunks.size(); ++new_part) {
                        
                        for(Range movedRange : chunks.get(new_part)) {
                            plan.moveColdRange(table, movedRange, old_part, new_part);
                        }
                    }
                }            
            }
        }
        
        return true;
    }

    private long keysPerTable(String table) {
        long keys_count = 0;
    
        Map<Integer, List<Range>> partition_ranges = plan.getAllRanges(table);
        for (List<Range> ranges : partition_ranges.values()) {
            keys_count += Plan.getRangeListWidth(ranges);
        }
        
        return keys_count;
    }
        
    public double getLoadPerPartition(int partition) {
        long keys_count = 0;
        for(String table : plan.table_names){
            List<Range> ranges = plan.getAllRanges(table, partition);
            if(ranges != null) {
                keys_count += Plan.getRangeListWidth(ranges);
            }
        }
       
        return keys_count;
    }

}
