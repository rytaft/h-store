package org.qcri.affinityplanner;

public class ParallelMigration implements Migration {       
    int partitions_per_site;
    int db_migration_time;
    
    // @param partitions_per_site - the number of partitions per site
    // @param db_migration_time - the number of time steps required to migrate all the data 
    //                            in the database once with a single thread
    public ParallelMigration(int partitions_per_site, int db_migration_time) {
        this.partitions_per_site = partitions_per_site;
        this.db_migration_time = db_migration_time;
    }
    
    public void setDbMigrationTime(int db_migration_time) {
        this.db_migration_time = db_migration_time;
    }
    
    // Number of time steps needed to scale from nodes_before to nodes_after
    // with maximum parallel migration
    public int reconfigTime(int nodes_before, int nodes_after) {
        if (nodes_before == nodes_after) {
            return 0;
        }
        else if (nodes_before < nodes_after) { // scale out
            double max_parallel = this.partitions_per_site * Math.min(nodes_before, nodes_after - nodes_before);
            return (int) Math.ceil((1 - (double) nodes_before / nodes_after) * this.db_migration_time / max_parallel);
        }
        else { // scale in
            double max_parallel = this.partitions_per_site * Math.min(nodes_after, nodes_before - nodes_after);
            return (int) Math.ceil((1 - (double) nodes_after / nodes_before) * this.db_migration_time / max_parallel);
        }
    }

    // The cost of a move from nodes_before to nodes_after with maximum parallel migration
    public double moveCost(int reconfig_time, int nodes_before, int nodes_after) {
        // Machine allocation symmetric for scale-in and scale-out.  
        // Important distinction is not before/after but larger/smaller.
        int l = Math.max(nodes_before, nodes_after); // larger cluster
        int s = Math.min(nodes_before, nodes_after); // smaller cluster
        int delta = l - s;
        int r = delta % s; // remainder

        // ============================================== 
        // Case 1: All machines added or removed at once 
        // ============================================== 
        if (s >= delta) return l;
        
        // ============================================== 
        // Case 2: delta is multiple of smaller cluster
        // ============================================== 
        if (r == 0) return (2*s + l) / 2.0;
        
        // ==============================================
        // Case 3:  Machines added or removed in 3 phases
        // ==============================================
        
        // Phase 1: N1 sets of s machines added and filled completely
        int N1 = delta / s - 1;         // number of steps in phase1
        double T1 = (double) s / delta; // time per step in phase1
        double M1 = (s + l - r) / 2.0;  // average machines in phase1
        double phase1 = N1 * T1 * M1;        
        
        // Phase 2: s machines added and filled r/s fraction of the way
        double T2 = (double) r / delta; // time for phase2
        int M2 = l - r;                 // machines in phase2
        double phase2 = T2 * M2;
         
        // Phase 3: r machines added and remaining machines filled completely
        double T3 = (double) s / delta; // time for phase3
        int M3 = l;                     // machines in phase3
        double phase3 = T3 * M3;

        return reconfig_time * (phase1 + phase2 + phase3);
    }
}
