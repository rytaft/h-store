package org.qcri.affinityplanner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

public abstract class Partitioner {
    
    private static final Logger LOG = Logger.getLogger(Partitioner.class);

    public static int MIN_LOAD_PER_PART = Integer.MIN_VALUE;
    public static int MAX_LOAD_PER_PART = Integer.MAX_VALUE;
    public static double LMPT_COST = 1.1;
    public static double DTXN_COST = 5.0;
    public static int MAX_MOVED_TUPLES_PER_PART = 100;
    public static int MIN_GAIN_MOVE = 0;
    public static int MAX_PARTITIONS_ADDED = 6;

    protected AffinityGraph m_graph;

    public abstract boolean repartition ();
    
    /**
     * Adds active partitions (i.e., non-empty) and overloaded partitions to the input sets
     * 
     * @param activePartitions
     * @param overloadedPartitions
     */
    protected void measureLoad(Set<Integer> activePartitions, Set<Integer> overloadedPartitions){
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(!m_graph.m_partitionVertices.get(i).isEmpty()){
                activePartitions.add(i);
                System.out.println(getLoadPerPartition(i));
                if (getLoadPerPartition(i) > MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }
    }
    
    /**
     * computes load of a set of vertices in the current partition. this is different from the weight of a vertex because it considers
     * both direct accesses of the vertex and the cost of remote accesses
     */
    protected abstract Double getLoadInCurrPartition(Set<String> vertices);
    
    /**
     * Returns sorted (descending order) list of top-k vertices from site
     * 
     * @param partition
     * @param k
     * @return
     */
    protected List<String> getHottestVertices(int partition, int k){

        List<String> res = new ArrayList<String>(k);
        final Map<String, Double> hotnessMap = new HashMap<String,Double>(k);

        k = Math.min(k, m_graph.m_partitionVertices.get(partition).size());
        int lowestPos = 0;
        double lowestLoad = Double.MAX_VALUE;

        for(String vertex : m_graph.m_partitionVertices.get(partition)){

            double vertexLoad = getLoadInCurrPartition(Collections.singleton(vertex));

            if (res.size() < k){

                res.add(vertex);
                hotnessMap.put(vertex, vertexLoad);
                if (lowestLoad > vertexLoad){
                    lowestPos = res.size() - 1;
                    lowestLoad = vertexLoad;
                }
            }

            else{
                if(vertexLoad > lowestLoad){

                    hotnessMap.remove(res.get(lowestPos));

                    res.set(lowestPos, vertex);
                    hotnessMap.put(vertex, vertexLoad);

                    // find new lowest load
                    lowestLoad = vertexLoad;
                    for (int i = 0; i < k; i++){
                        double currLoad = hotnessMap.get(res.get(i)); 
                        if(currLoad < lowestLoad){
                            lowestPos = i;
                            lowestLoad = currLoad; 
                        }
                    }
                }
            }
        }

        // sort determines an _ascending_ order
        // Comparator should return "a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second"
        // We want a _descending_ order, so we need to invert the comparator result
        Collections.sort(res, new Comparator<String>(){
            @Override
            public int compare(String o1, String o2) {
                if (hotnessMap.get(o1) < hotnessMap.get(o2)){
                    return 1;
                }
                else if (hotnessMap.get(o1) > hotnessMap.get(o2)){
                    return -1;
                }
                return 0;
            }
        });

        return res;
    }

    /**
     *     LOCAL delta a partition gets by REMOVING (if forSender = true) or ADDING (else) a SET of local vertices out
     *     it ASSUMES that the vertices are on the same partition
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     *     
     *     if newPartition = -1 we evaluate moving to an unknown REMOTE partition
     */
    protected abstract double getDeltaVertices(Set<String> movingVertices, int toPartition, boolean forSender);

    protected abstract Double getLoadPerPartition(int partition);
    
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
    
    /**
     *  SCALE IN
     *  
     *  very simple policy: if a partition is underloaded, try to move its whole content to another partition
     */
    protected void scaleIn(Set<Integer> overloadedPartitions, Set<Integer> activePartitions){

        // detect underloaded partitions
        TreeSet<Integer> underloadedPartitions = new TreeSet<Integer>();
        for(Integer part : activePartitions){
            if (getLoadPerPartition(part) < MIN_LOAD_PER_PART){
                underloadedPartitions.add(part);
            }
        }

        if (!underloadedPartitions.isEmpty()){
            System.out.println("SCALING IN");
        }


        // offload from partitions with higher id to partitions with lower id. this helps emptying up the latest servers.
        Iterator<Integer> descending = underloadedPartitions.descendingIterator();
        HashSet<Integer> removedPartitions = new HashSet<Integer>();

        while(descending.hasNext()){

            Integer underloadedPartition = descending.next();
            System.out.println("Offloading partition " + underloadedPartition);
            Set<String> movingVertices = new HashSet<String>();
            movingVertices.addAll(m_graph.m_partitionVertices.get(underloadedPartition));

            // try to offload to remote partitions
            Collection<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(underloadedPartition));
            for(Integer toPartition : activePartitions){

                if(!localPartitions.contains(toPartition) && !removedPartitions.contains(toPartition)){

                    LOG.debug("Trying with partition " + toPartition);
                    int movedVertices = tryMoveVertices(movingVertices, underloadedPartition, toPartition);

                    if(movedVertices > 0){
                        removedPartitions.add(underloadedPartition);
                        break;                            
                    }
                }
            }
        }
        activePartitions.removeAll(removedPartitions);
    }

    public void writePlan(String newPlanFile){
        m_graph.planToJSON(newPlanFile);
    }

}
