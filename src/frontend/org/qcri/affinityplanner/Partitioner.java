package org.qcri.affinityplanner;

public interface Partitioner {
    public boolean repartition();

    public void writePlan(String plan_out);

    public double getLoadPerPartition(int j);
}
