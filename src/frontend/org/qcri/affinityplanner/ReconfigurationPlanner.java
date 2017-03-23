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
    private int partitions_per_site;
    
    // Takes an existing plan and the number of partitions specified by partitions_after to
    // come up with a new plan
    //
    // This partitioner assumes that the tuples are divided evenly across partitions in the 
    // existing plan, and that the workload is uniform across tuples.  It finds a new plan in
    // which total data movement is minimized while still spreading tuples evenly across
    // the partitions
    public ReconfigurationPlanner (File planFile, int partitions_after, int partitions_per_site) {
        this.plan = new Plan(planFile.toString());
        this.partitions_before = this.plan.getAllPartitions().size();
        this.partitions_after = partitions_after;
        this.partitions_per_site = partitions_per_site;
    }
    
    public ReconfigurationPlanner (String planString, int partitions_after, int partitions_per_site) {
        this.plan = new Plan(planString, "");
        this.partitions_before = this.plan.getAllPartitions().size();
        this.partitions_after = partitions_after;
        this.partitions_per_site = partitions_per_site;
    }
        
    @Override
    public void writePlan(String plan_out) {
        plan.toJSON(plan_out);
    }
    
    public String getPlanString() throws JSONException {
        return plan.toJSONObject().toString(2);
    }
    
    public Plan getPlan() {
        return plan;
    }
    
    public boolean repartition() {
        if (this.partitions_before < this.partitions_after) { // scale out
            for(String table : plan.table_names){
                int num_new_machines = (this.partitions_after - this.partitions_before) / this.partitions_per_site;
                double sliceWidth = keysPerTable(table) * (1.0/this.partitions_before - 1.0/this.partitions_after) / num_new_machines;
                for (int new_part = this.partitions_before; new_part < this.partitions_after; ++new_part) {
                    plan.addPartition(table, new_part);
                }
                
                // each old machine will be giving data to each new machine
                for (int old_part = 0; old_part < this.partitions_before; ++old_part) {
                    List<List<Range>> chunks = plan.getRangeChunks(table, old_part, sliceWidth);
                    // send from the end of the list of chunks so higher keys go to higher partitions
                    for (int new_mach = 0, chunk = Math.max(chunks.size() - num_new_machines, 0); new_mach < num_new_machines && chunk < chunks.size(); ++new_mach, ++chunk) {
                        int new_part = this.partitions_before + new_mach * this.partitions_per_site + old_part % this.partitions_per_site;
                        for(Range movedRange : chunks.get(chunk)) {
                            plan.moveColdRange(table, movedRange, old_part, new_part);
                        }
                    }
                }            
            }
        }
        else if (this.partitions_before > this.partitions_after) { // scale in
            for(String table : plan.table_names){
                int machines_after = this.partitions_after / this.partitions_per_site;
                double sliceWidth = keysPerTable(table) * (1.0/this.partitions_before) / machines_after;
                
                // each old machine that is going away will be giving data to each machine that is staying
                for (int old_part = this.partitions_after; old_part < this.partitions_before; ++old_part) {
                    
                    List<List<Range>> chunks = plan.getRangeChunks(table, old_part, sliceWidth);
                    for (int new_mach = 0; new_mach < machines_after && new_mach < chunks.size(); ++new_mach) {
                        int new_part = new_mach * this.partitions_per_site + old_part % this.partitions_per_site;
                        for(Range movedRange : chunks.get(new_mach)) {
                            plan.moveColdRange(table, movedRange, old_part, new_part);
                        }
                    }
                }  
                
                for (int old_part = this.partitions_after; old_part < this.partitions_before; ++old_part) {
                    plan.removePartition(table, old_part);
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
