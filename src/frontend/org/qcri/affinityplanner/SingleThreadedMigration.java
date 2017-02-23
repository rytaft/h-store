package org.qcri.affinityplanner;

public class SingleThreadedMigration implements Migration {       
    int db_migration_time;
    
    // @param db_migration_time - the number of time steps required to migrate all the data 
    // in the database once with a single thread
    public SingleThreadedMigration(int db_migration_time) {
        this.db_migration_time = db_migration_time;
    }
    
    public void setDbMigrationTime(int db_migration_time) {
        this.db_migration_time = db_migration_time;
    }
    
    // Number of time steps needed to scale from nodes_before to nodes_after
    public int reconfigTime(int nodes_before, int nodes_after) {
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
    
    // The cost of a move from nodes_before to nodes_after
    public double moveCost(int reconfig_time, int nodes_before, int nodes_after) {
        if (nodes_before == nodes_after) {
            return reconfig_time * nodes_before;
        }
        return reconfig_time * (nodes_after + nodes_before + 1)/2.0;
    }   
}
