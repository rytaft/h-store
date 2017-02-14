package org.qcri.affinityplanner;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;


public class ReconfigurationPredictor {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPredictor.class);
    private double capacity_per_partition;
    private ArrayList<Double> predicted_load;
    private int partitions_start;
    private int db_migration_time;
    private int max_partitions;
    private CostPath[][] min_cost;
    private int part_per_site;
    
    int getMaxPartitions() {
        return max_partitions;
    }
    
    public class Move {
        public int partitions;
        public int time;
        
        Move(int time, int partition) {
            this.time = time;
            this.partitions = partition;
        }
        
        @Override
        public String toString() {
            return "Partitions: " + partitions + ", Time: " + time;
        }
    }
    
    private class CostPath {
        public double cost;
        public int partitions_before;
        public int t_before;
        
        @Override
        public String toString() {
            return "Cost: " + cost + ", Partitions before last move: " + partitions_before + ", Time before last move: " + t_before;
        }
    }
    
    // Create a new instance of the ReconfigurationPredictor
    // @param capacity_per_partition - The maximum number of transactions per second a single partition can serve
    // @param predicted_load - An array of predicted aggregate load on the database (transactions per second).  
    //                         Assumes predictions are evenly spaced in time (e.g., one prediction for each second)
    // @param partitions_start - the number of partitions at time t = 0
    // @param db_migration_time - the number of time steps required to migrate all the data in the database once
    public ReconfigurationPredictor (double capacity_per_partition, int part_per_site, int db_migration_time) {
        this.capacity_per_partition = capacity_per_partition;
        this.db_migration_time = db_migration_time;
        this.part_per_site = part_per_site;
    }
    
    // The maximum number of transactions the given number of partitions can serve
    double capacity(double partition) {
        return partition * capacity_per_partition;
    }
    
    // Number of time steps needed to scale from partitions_before to partitions_after
    int reconfigTime(int partitions_before, int partitions_after) {
        if (partitions_before == partitions_after) {
            return 0;
        }
        else if (partitions_before < partitions_after) { // scale out
            return (int) Math.ceil((1.0/partitions_before - 1.0/partitions_after) * this.db_migration_time * partitions_before);
        }
        else { // scale in
            return (int) Math.ceil((1.0/partitions_after - 1.0/partitions_before) * this.db_migration_time * partitions_after);
        }
    }
    
    // Calculates the effective capacity at time step i (out of reconfig_time steps total) 
    // during the move from partitions_before to partitions_after
    double effectiveCapacity(int i, int reconfig_time, int partitions_before, int partitions_after) {
        if (partitions_before == partitions_after) {
            return capacity(partitions_before);
        }
        else if (partitions_after > partitions_before) { // scale out
            return capacity(1.0/(1.0/partitions_before - ((double) i/reconfig_time) * (1.0/partitions_before - 1.0/partitions_after)));
        }
        else { // scale in
            return capacity(1.0/(1.0/partitions_before + ((double) i/reconfig_time) * (1.0/partitions_after - 1.0/partitions_before)));
        }
    }

    // Calculates the minimum total cost of ending at time t, with the last move 
    // moving from partitions_before to partitions_after (which takes reconfig_time steps total)
    private double subCost(int t, int reconfig_time, int partitions_before, int partitions_after) {
        if (t - reconfig_time < 0) {
            return Double.POSITIVE_INFINITY; // this reconfiguration would need to start in the past 
        }
        
        for (int i = 1; i <= reconfig_time; ++i) {
            double load = predicted_load.get(t - reconfig_time + i);
            if (load > effectiveCapacity(i, reconfig_time, partitions_before, partitions_after)) {
                return Double.POSITIVE_INFINITY; // penalty for not enough capacity during the move
            }
        }
        return cost(t - reconfig_time, partitions_before) + reconfig_time * (int) Math.ceil((partitions_after + partitions_before)/2.0);
    }

    // Calculates the minimum total cost to end up with the given number of partitions at time t
    private double cost(int t, int partitions) {
        if (t < 0 || predicted_load.get(t) > capacity(partitions) || (t == 0 && partitions != this.partitions_start)) { // constraints
            return Double.POSITIVE_INFINITY;
        }
        else if (min_cost[t][partitions-1] != null) {
            return min_cost[t][partitions-1].cost;
        }
        else if (t == 0) { // base case
            min_cost[t][partitions-1] = new CostPath();
            min_cost[t][partitions-1].cost = partitions; // equals this.partitions_start
        }
        else { // recursive step           
            min_cost[t][partitions-1] = new CostPath();
            
            for (int partitions_before = 1; partitions_before <= this.max_partitions; ++partitions_before) {
                int reconfig_time = reconfigTime(partitions_before, partitions);
                if (partitions_before == partitions) {
                    reconfig_time = 1; // special case for no data movement - a "move" must last at least one time slice
                }
                
                double cost = subCost(t, reconfig_time, partitions_before, partitions);
                if (partitions_before == 1 || cost < min_cost[t][partitions-1].cost) {
                    min_cost[t][partitions-1].cost = cost;
                    min_cost[t][partitions-1].partitions_before = partitions_before;
                    min_cost[t][partitions-1].t_before = t - reconfig_time;
                }
            }
            
            LOG.debug("At time " + t + " with " + partitions + " partitions: " + min_cost[t][partitions-1].toString());
        }

        return min_cost[t][partitions-1].cost;
    }
         
    // Calculate the best series of moves
    public ArrayList<Move> bestMoves(ArrayList<Double> predicted_load, int partitions_start) {
        this.predicted_load = predicted_load;
        this.partitions_start = partitions_start;
        this.max_partitions = Math.max((int) Math.ceil(Collections.max(predicted_load) / capacity_per_partition), partitions_start);
        this.min_cost = new CostPath[this.predicted_load.size()][this.max_partitions];

        ArrayList<Move> moves = new ArrayList<>();
        int end_time = this.predicted_load.size() - 1;
        for (int end_partitions = 1; end_partitions <= this.max_partitions; ++end_partitions) {
            double cost = cost(end_time, end_partitions);
            if (cost != Double.POSITIVE_INFINITY) {
                int t = end_time;
                int partitions = end_partitions;
                while (t > 0) {
                    moves.add(new Move(t, partitions));
                    CostPath cp = min_cost[t][partitions-1];
                    t = cp.t_before;
                    partitions = cp.partitions_before;
                }
                moves.add(new Move(0, this.partitions_start));
                Collections.reverse(moves);
                return moves;
            }
        }
        return null;
    }


}
