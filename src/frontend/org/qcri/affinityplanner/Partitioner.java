package org.qcri.affinityplanner;

import java.util.Collection;
import java.util.Set;

public abstract class Partitioner {
    
    public static int MIN_LOAD_PER_PART = Integer.MIN_VALUE;
    public static int MAX_LOAD_PER_PART = Integer.MAX_VALUE;
    public static double LMPT_COST = 1.1;
    public static double DTXN_COST = 5.0;
    public static int MAX_MOVED_TUPLES_PER_PART = 100;
    public static int MIN_GAIN_MOVE = 0;
    public static int MAX_PARTITIONS_ADDED = 6;

    protected AffinityGraph m_graph;

    public abstract boolean repartition ();
    
    /*
     *     LOCAL delta a partition gets by REMOVING (if forSender = true) or ADDING (else) a SET of local vertices out
     *     it ASSUMES that the vertices are on the same partition
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     *     
     *     if newPartition = -1 we evaluate moving to an unknown REMOTE partition
     */
    protected abstract double getDeltaVertices(Set<String> movingVertices, int toPartition, boolean forSender);

    public abstract Double getLoadPerPartition(int partition);
    
    public Double getLoadPerSite(int site){
        Collection<Integer> partitions = PlanHandler.getPartitionsSite(site);
        double load = 0;
        for (Integer partition : partitions){
            load += getLoadPerPartition(partition);
        }
        return load;
    }
    
    /**
     * Tries to move movingVertices from overloadedPartition to toPartition. 
     * 
     * Fails if the move does not result in a minimal gain threshold for the fromPartition OR
     *      if the toPartition becomes overloaded as an effect of the transfer.
     * 
     * If the move is allowed, it updates the graph and the plan
     * 
     * @param movingVertices
     * @param fromPartition
     * @param toPartition
     * @return number of partitions actually moved
     */
    protected int tryMoveVertices(Set<String> movingVertices, Integer fromPartition, Integer toPartition) {

        int numMovedVertices = 0;
        double deltaFromPartition = getDeltaVertices(movingVertices, toPartition, true);
        double deltaToPartition = getDeltaVertices(movingVertices, toPartition, false);

        //      LOG.debug("Deltas from " + deltaFromPartition + " - to " + deltaToPartition);

        // check that I get enough overall gain and the additional load of the receiving site does not make it overloaded
        if(deltaFromPartition <= MIN_GAIN_MOVE * -1
                && getLoadPerPartition(toPartition) + deltaToPartition < MAX_LOAD_PER_PART){   // if gainToSite is negative, the load of the receiving site grows
            //            LOG.debug("Moving to partition " + toPartition);
            //            LOG.debug("Weights before moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));

            m_graph.moveVertices(movingVertices, fromPartition, toPartition);
            //            LOG.debug("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));

            numMovedVertices = movingVertices.size();
        }
        return numMovedVertices;
    }

    public void writePlan(String newPlanFile){
        m_graph.planToJSON(newPlanFile);
    }

}
