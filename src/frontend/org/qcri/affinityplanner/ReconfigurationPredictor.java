package org.qcri.affinityplanner;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;


public class ReconfigurationPredictor {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPredictor.class);
    private long capacity_per_node;
    private ArrayList<Long> predicted_load;
    private int nodes_start;
    private int db_migration_time;
    private int max_nodes;
    private CostPath[][] min_cost;
    
    int getMaxNodes() {
        return max_nodes;
    }
    
    public class Move {
        public int nodes;
        public int time;
        
        Move(int time, int nodes) {
            this.time = time;
            this.nodes = nodes;
        }
        
        @Override
        public String toString() {
            return "Nodes: " + nodes + ", Time: " + time;
        }
    }
    
    private class CostPath {
        public double cost;
        public int nodes_before;
        public int t_before;
        
        @Override
        public String toString() {
            return "Cost: " + cost + ", Nodes before last move: " + nodes_before + ", Time before last move: " + t_before;
        }
    }
    
    // Create a new instance of the ReconfigurationPredictor
    // @param capacity_per_node - The maximum number of transactions per second a single node can serve
    // @param predicted_load - An array of predicted aggregate load on the database (transactions per second).  
    //                         Assumes predictions are evenly spaced in time (e.g., one prediction for each second)
    // @param nodes_start - the number of nodes at time t = 0
    // @param db_migration_time - the number of time steps required to migrate all the data in the database once
    public ReconfigurationPredictor (long capacity_per_node, int db_migration_time) {
        this.capacity_per_node = capacity_per_node;
        this.db_migration_time = db_migration_time;
    }
    
    // The maximum number of transactions the given number of nodes can serve
    // Number of nodes may be a non-integer since this function is called by effectiveCapacity()
    double capacity(double nodes) {
        return nodes * capacity_per_node;
    }
    
    // Number of time steps needed to scale from nodes_before to nodes_after
    int reconfigTime(int nodes_before, int nodes_after) {
        if (nodes_before == nodes_after) {
            return 0;
        }
        else if (nodes_before < nodes_after) { // scale out
            return (int) Math.ceil((1.0/nodes_before - 1.0/nodes_after) * this.db_migration_time * nodes_before);
        }
        else { // scale in
            return (int) Math.ceil((1.0/nodes_after - 1.0/nodes_before) * this.db_migration_time * nodes_after);
        }
    }
    
    // Calculates the effective capacity at time step i (out of reconfig_time steps total) 
    // during the move from nodes_before to nodes_after
    double effectiveCapacity(int i, int reconfig_time, int nodes_before, int nodes_after) {
        if (nodes_before == nodes_after) {
            return capacity(nodes_before);
        }
        else if (nodes_after > nodes_before) { // scale out
            return capacity(1.0/(1.0/nodes_before - ((double) i/reconfig_time) * (1.0/nodes_before - 1.0/nodes_after)));
        }
        else { // scale in
            return capacity(1.0/(1.0/nodes_before + ((double) i/reconfig_time) * (1.0/nodes_after - 1.0/nodes_before)));
        }
    }

    // Calculates the minimum total cost of ending at time t, with the last move 
    // moving from nodes_before to nodes_after (which takes reconfig_time steps total)
    private double subCost(int t, int reconfig_time, int nodes_before, int nodes_after) {
        if (t - reconfig_time < 0) {
            return Double.POSITIVE_INFINITY; // this reconfiguration would need to start in the past
        }
        
        for (int i = 1; i <= reconfig_time; ++i) {
            long load = predicted_load.get(t - reconfig_time + i);
            if (load > effectiveCapacity(i, reconfig_time, nodes_before, nodes_after)) {
                return Double.POSITIVE_INFINITY; // penalty for not enough capacity during the move
            }
        }
        return cost(t - reconfig_time, nodes_before) + reconfig_time * (int) Math.ceil((nodes_after + nodes_before)/2.0);
    }

    // Calculates the minimum total cost to end up with the given number of nodes at time t
    private double cost(int t, int nodes) {
        if (t < 0 || predicted_load.get(t) > capacity(nodes) || (t == 0 && nodes != this.nodes_start)) { // constraints
            return Double.POSITIVE_INFINITY;
        }
        else if (min_cost[t][nodes-1] != null) {
            return min_cost[t][nodes-1].cost;
        }
        else if (t == 0) { // base case
            min_cost[t][nodes-1] = new CostPath();
            min_cost[t][nodes-1].cost = nodes; // equals this.nodes_start
        }
        else { // recursive step           
            min_cost[t][nodes-1] = new CostPath();
            
            for (int nodes_before = 1; nodes_before <= this.max_nodes; ++nodes_before) {
                int reconfig_time = reconfigTime(nodes_before, nodes);
                if (nodes_before == nodes) {
                    reconfig_time = 1; // special case for no data movement - a "move" must last at least one time slice
                }

                double cost = subCost(t, reconfig_time, nodes_before, nodes);
                if (nodes_before == 1 || cost < min_cost[t][nodes-1].cost) {
                    min_cost[t][nodes-1].cost = cost;
                    min_cost[t][nodes-1].nodes_before = nodes_before;
                    min_cost[t][nodes-1].t_before = t - reconfig_time;
                }
            }
            
            LOG.debug("At time " + t + " with " + nodes + " nodes: " + min_cost[t][nodes-1].toString());
        }

        return min_cost[t][nodes-1].cost;
    }
         
    // Calculate the best series of moves
    public ArrayList<Move> bestMoves(ArrayList<Long> predicted_load, int nodes_start) {
        this.predicted_load = predicted_load;
        this.nodes_start = nodes_start;
        this.max_nodes = Math.max((int) Math.ceil((double) Collections.max(predicted_load) / capacity_per_node), nodes_start);
        this.min_cost = new CostPath[this.predicted_load.size()][this.max_nodes];

        ArrayList<Move> moves = new ArrayList<>();
        int end_time = this.predicted_load.size() - 1;
        for (int end_nodes = 1; end_nodes <= this.max_nodes; ++end_nodes) {
            double cost = cost(end_time, end_nodes);
            if (cost != Double.POSITIVE_INFINITY) {
                int t = end_time;
                int nodes = end_nodes;
                while (t > 0) {
                    moves.add(new Move(t, nodes));
                    CostPath cp = min_cost[t][nodes-1];
                    t = cp.t_before;
                    nodes = cp.nodes_before;
                }
                moves.add(new Move(0, this.nodes_start));
                Collections.reverse(moves);
                return moves;
            }
        }
        return null;
    }


}
