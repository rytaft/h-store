package org.qcri.affinityplanner;

import java.nio.file.Path;

public interface Partitioner {
    public boolean repartition();

    public void writePlan(String plan_out);
    public void graphToMetisFile(Path out, Path mapOut);

    public double getLoadPerPartition(int j);
}
