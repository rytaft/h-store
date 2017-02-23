package org.qcri.affinityplanner;

// Convenient interface for different migration strategies which have different reconfiguration
// time and cost
public interface Migration {
    int reconfigTime(int nodes_before, int nodes_after);
    double moveCost(int reconfig_time, int nodes_before, int nodes_after);
    void setDbMigrationTime(int db_migration_time);
}
