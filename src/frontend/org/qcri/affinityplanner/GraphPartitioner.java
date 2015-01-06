package org.qcri.affinityplanner;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.CatalogContext;

public class GraphPartitioner {
    
    public static int MIN_LOAD_PER_PART = Integer.MIN_VALUE;
    public static int MAX_LOAD_PER_PART = Integer.MAX_VALUE;
    public static double LMPT_COST = 1.1;
    public static double DTXN_COST = 5.0;
    public static int MAX_MOVED_TUPLES_PER_PART = Integer.MAX_VALUE;
    public static int MIN_GAIN_MOVE = 0;
    public static int MAX_PARTITIONS_ADDED = Integer.MAX_VALUE;

    public static int PART_PER_SITE = -1;
    public static int MAX_PARTITIONS = -1;

    private AffinityGraph m_graph;
    private PlanHandler m_plan_handler;
    
    public GraphPartitioner (AffinityGraph graph, File planFile, CatalogContext catalogContext){
        m_graph = graph;
        m_plan_handler = new PlanHandler(planFile, catalogContext);
    }
    
    /**
     * Partitions the graph
     * 
     * @return true if could find feasible partitioning, false otherwise
     */
    
    public boolean repartition () {
        if (PART_PER_SITE == -1 || MAX_PARTITIONS == -1){
            System.out.println("GraphPartitioner: Must initialize PART_PER_SITE and MAX_PARTITIONS");
            return false;
        }
        
        System.out.println("Calculating site loads");
        
        /*
         *  SCALE OUT
         */

        // detect overloaded and active partitions
        Set<Integer> activePartitions = new HashSet<Integer>();
        Set<Integer> overloadedPartitions = new HashSet<Integer>();
        for(int i = 0; i < MAX_PARTITIONS; i++){
            if(!m_graph.m_partitionVertices.get(i).isEmpty()){
                activePartitions.add(i);
                System.out.println(getLoadPerPartition(i));
                if (getLoadPerPartition(i) > MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        int addedPartitions = 0;
        if (! overloadedPartitions.isEmpty()){
            // offload each overloaded partition
            for(Integer overloadedPartition : overloadedPartitions){
                
                // DEBUG
                System.out.println("SCALING OUT");
                System.out.println("#######################");
                System.out.println("offloading site " + overloadedPartition);
    
                // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
                List<String> hotVerticesNotMoved = getHottestVertices(overloadedPartition, MAX_MOVED_TUPLES_PER_PART);
                final int actualMaxMovedVertices = hotVerticesNotMoved.size();
    
                // DEBUG
    //            System.out.println("hot vertices:");
    //            for (String hotVertex : hotVerticesNotMoved){
    //                System.out.println(hotVertex);
    //            }
    
                int currHotVertex = 0;
                int numMovedVertices = 0;
                Set<String> movingVertices = new HashSet<String>();
                boolean movingVerticesExpandable = true;
               
                while(getLoadPerPartition(overloadedPartition) > MAX_LOAD_PER_PART){
                    
                    System.out.println("--------------------");
    
                    // Step 1) add partition if I have over-expanded movingVertices or if I cannot expand it anymore
                    
                    // We expand movingVertices only if all partitions reject the previous movingVertices sets.
                    // We fill up low-order partitions first to minimize the number of servers
                    
                    if (numMovedVertices + movingVertices.size() >= actualMaxMovedVertices || movingVerticesExpandable == false){
                        System.out.println("Adding a new partition");
                        if(activePartitions.size() < MAX_PARTITIONS && addedPartitions < MAX_PARTITIONS_ADDED){
                            currHotVertex = 0;
                            movingVertices.clear();
                            movingVerticesExpandable = true;
                            m_graph.addPartitions(1);
                            addedPartitions++;
                            for(int i = 0; i < MAX_PARTITIONS; i++){
                                if(!activePartitions.contains(i)){
                                    activePartitions.add(i);
                                }
                            }
                        }
                        else{
                            System.out.println("Partition " + overloadedPartitions + " cannot be offloaded.");
                            return false;
                        }
                    }
                    System.out.println("Adding a new vertex");
                    
                    // Step 2) add one vertex to movingVertices
                    if (movingVertices.isEmpty()){
                        // if empty, add a new hot vertex
                        assert (currHotVertex < hotVerticesNotMoved.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 
                        String nextHotVertex = null;
                        do{
                            nextHotVertex = hotVerticesNotMoved.get(currHotVertex);
                            currHotVertex++;
                        } while (m_graph.m_vertexPartition.get(nextHotVertex) != overloadedPartition);
                        assert (nextHotVertex != null); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
                        System.out.println("Adding hot vertex " + nextHotVertex);
                        movingVertices.add(nextHotVertex);
                    } // END if(movedVertices.isEmpty())
                    
                    else{
                        
                        // extend current moved set
                        
                        // assess gain with extension
                        double deltaEdgeExtension;                        
                        String nextEdgeExtension = getMostAffineExtension(movingVertices);
                        if(nextEdgeExtension != null){
                            movingVertices.add(nextEdgeExtension);
                            deltaEdgeExtension = getDeltaGiveVertices(movingVertices, -1);
                            movingVertices.remove(nextEdgeExtension);
                        }
                        else{
                            // no available edge extension
                            deltaEdgeExtension = Double.MIN_VALUE;
                        }
                        
                        // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
                        double deltaHotTuple;
                        String nextHotVertex = null;
                        int skip = 0;
                        do{
                            nextHotVertex = hotVerticesNotMoved.get(currHotVertex + skip);
                            skip ++;
                        } while (movingVertices.contains(nextHotVertex) && currHotVertex + skip < hotVerticesNotMoved.size()); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
                        if (! movingVertices.contains(nextHotVertex)){
                            assert(nextHotVertex != null);
                            movingVertices.add(nextHotVertex);
                            deltaHotTuple = getDeltaGiveVertices(movingVertices, -1);
                            movingVertices.remove(nextHotVertex);
                        }
                        else{
                            // no available hot vertex
                            deltaHotTuple = Double.MIN_VALUE;
                        }
                        
                        // pick best available choice
                        if(deltaEdgeExtension == Double.MIN_VALUE && deltaHotTuple == Double.MIN_VALUE){
                            // no choice available
                            movingVerticesExpandable = false;
                        }
                        else{
                            if (deltaEdgeExtension < deltaHotTuple){
                                movingVertices.add(nextEdgeExtension);
                                System.out.println("Adding edge extension " + nextEdgeExtension);
                            }
                            else{
                                movingVertices.add(nextHotVertex);
                                System.out.println("Adding hot vertex " + nextHotVertex);
                                currHotVertex += skip;
                            }
                        }
                    
                    } // END if(!movedVertices.isEmpty())
    
                    // DEBUG
                    System.out.println("Moving vertices: ");
                    for (String vertex : movingVertices){
                        System.out.println(vertex);
                    }
    
                    // Step 3) search site to offload (might not find it)
                    
                    // first try to offload to local partitions
                    boolean moved = false;
                    Collection<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(overloadedPartition));
                    for(Integer toPartition : localPartitions){
                        if(!overloadedPartitions.contains(toPartition)){
                            System.out.println("Trying with partition " + toPartition);
                            int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition, new Double(MAX_LOAD_PER_PART), hotVerticesNotMoved);
                            if(movedVertices > 0){
                                numMovedVertices += movedVertices;                            
                                moved = true;
                                // update the plan
                                moveVerticesPlan(movingVertices, overloadedPartition, toPartition);
                                break;
                            }
                        }
                    }
                    
                    // then try to offload to remote partitions
                    if (!moved){
                        for(Integer toPartition : activePartitions){
                            if(!overloadedPartitions.contains(toPartition) && !localPartitions.contains(toPartition)){
                                System.out.println("Trying with partition " + toPartition);
                                int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition, new Double(MAX_LOAD_PER_PART), hotVerticesNotMoved);
                                if(movedVertices > 0){
                                    numMovedVertices += movedVertices;    
                                    // update the plan
                                    moveVerticesPlan(movingVertices, overloadedPartition, toPartition);
                                    break;
                                }
                            }
                        }
                    }
                } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
            }// END for(Integer overloadedPartition : overloadedPartitions)
        } // END if (!overloadedPartitions.isEmpty())
        else{
            /*
             *  SCALE IN
             *  
             *  very simple policy: if a partition is underloaded, try to move its whole content to another partition
             */
            
            System.out.println("SCALING IN");
            
            // detect underloaded partitions
            TreeSet<Integer> underloadedPartitions = new TreeSet<Integer>();
            for(Integer part : activePartitions){
                if (getLoadPerPartition(part) < MIN_LOAD_PER_PART){
                    underloadedPartitions.add(part);
                }
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
                    if(!overloadedPartitions.contains(toPartition) 
                            && !localPartitions.contains(toPartition) 
                            && !removedPartitions.contains(toPartition)){
                        System.out.println("Trying with partition " + toPartition);
                        int movedVertices = tryMoveVertices(movingVertices, underloadedPartition, toPartition, new Double(MAX_LOAD_PER_PART), null);
                        if(movedVertices > 0){
                            removedPartitions.add(underloadedPartition);
                            break;                            
                        }
                    }
                }
                
            }
            activePartitions.removeAll(removedPartitions);
        } // END if(overloadedPartitions.isEmpty())
        return true;
    }
    
    private void moveVerticesPlan(Set<String> movingVertices, Integer fromPartition, Integer toPartition){
        for (String vertex : movingVertices){
            m_plan_handler.removeTupleId(fromPartition, Long.parseLong(vertex));
            m_plan_handler.addRange(toPartition, Long.parseLong(vertex), Long.parseLong(vertex));
        }
    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    private String getMostAffineExtension(Set<String> vertices){
        double maxAdjacency = -1;
        String res = null;
        int partition = m_graph.m_vertexPartition.get(vertices.iterator().next());
        for(String vertex : vertices){
            Map<String,Double> adjacency = m_graph.m_edges.get(vertex);
            if(adjacency != null){
                for(Map.Entry<String, Double> edge : adjacency.entrySet()){
                    if (edge.getValue() > maxAdjacency
                            && m_graph.m_vertexPartition.get(edge.getKey()) == partition
                            && !vertices.contains(edge.getKey())){
                        maxAdjacency = edge.getValue();
                        res = edge.getKey();
                    }
                }
            }
        }
        return res;
    }
    
    /*
     * Tries to move movingVertices from overloadedPartition to toPartition. 
     * Fails if the move does not result in a minimal gain threshold for the fromPartition OR
     * if the toPartition becomes overloaded as an effect of the transfer.
     * Returns the number of partitions actually moved.
     */
    private int tryMoveVertices(Set<String> movingVertices, Integer fromPartition, Integer toPartition, Double maxLoadPerPartition, List<String> hotVerticesNotMoved) {
        int numMovedVertices = 0;
        double deltaFromPartition = getDeltaGiveVertices(movingVertices, toPartition);
        double deltaToPartition = getDeltaReceiveVertices(movingVertices, toPartition);

        // check that I get enough overall gain and the additional load of the receiving site does not make it overloaded
        System.out.println("Deltas from " + deltaFromPartition + " - to " + deltaToPartition);
        if(deltaFromPartition <= MIN_GAIN_MOVE 
                && getLoadPerPartition(toPartition) + deltaToPartition < maxLoadPerPartition){   // if gainToSite is negative, the load of the receiving site grows
            System.out.println("Moving to partition " + toPartition);
            System.out.println("Weights before moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));
            m_graph.moveVertices(movingVertices, fromPartition, toPartition);
            System.out.println("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));
            if(hotVerticesNotMoved != null){
                hotVerticesNotMoved.removeAll(movingVertices);
            }
            numMovedVertices = movingVertices.size();
            movingVertices.clear();
        }
        return numMovedVertices;
    }
    
    /*
     * computes load of a set of vertices in the current partition. this is different from the weight of a vertex because it considers
     * both direct accesses of the vertex and the cost of remote accesses
     */
    public Double getLoadInCurrPartition(Set<String> vertices){
        double load = 0;
        for(String vertex : vertices){
            // local accesses
            load += m_graph.m_vertices.get(vertex);
            // remote accesses
            int fromVertexPartition = m_graph.m_vertexPartition.get(vertex);
            int fromVertexSite = PlanHandler.getSitePartition(fromVertexPartition);
            Map<String,Double> adjacencyList = m_graph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Map.Entry<String, Double> edge : adjacencyList.entrySet()){
                    String toVertex = edge.getKey();
                    int toVertexPartition = m_graph.m_vertexPartition.get(toVertex);
                    int toVertexSite = PlanHandler.getSitePartition(toVertexPartition);
                    if(toVertexSite != fromVertexSite){
                        load += edge.getValue() * DTXN_COST;
                    }
                    else if(toVertexPartition != fromVertexPartition){
                        load += edge.getValue() * LMPT_COST;
                    }
                }
            }
        }
        return load;
    }
    
    public Double getLoadPerPartition(int partition){
        return getLoadInCurrPartition(m_graph.m_partitionVertices.get(partition));
    }
    
    
    public Double getLoadPerSite(int site){
        Collection<Integer> partitions = PlanHandler.getPartitionsSite(site);
        double load = 0;
        for (Integer partition : partitions){
            load += getLoadPerPartition(partition);
        }
        return load;
    }
    
    /*
     * returns top-k vertices from site 
     * if site has less than k vertices, return all vertices
     */
    public List<String> getHottestVertices(int partition, int k){
        k = Math.min(k, m_graph.m_partitionVertices.get(partition).size());
        List<String> res = new LinkedList<String>();
        double[] loads = new double[k];
        double lowestLoad = Double.MAX_VALUE;
        int lowestPos = 0;
        int filled = 0;
        
        for(String vertex : m_graph.m_partitionVertices.get(partition)){
 
            if (filled < k){
                res.add(vertex);
                loads[filled] = getLoadInCurrPartition(Collections.singleton(vertex));
                filled++;
                if(filled == k){
                    for (int i = 0; i < k; i++){
                        if(loads[i] < lowestLoad){
                            lowestLoad = loads[i];
                            lowestPos = i;
                        }
                    }
                }
            }

            else{
                double vertexLoad = getLoadInCurrPartition(Collections.singleton(vertex));
                if(vertexLoad > lowestLoad){
                    lowestLoad = vertexLoad;
                    res.set(lowestPos, vertex);
                    loads[lowestPos] = vertexLoad;
                    // find new lowest load
                    for (int i = 0; i < k; i++){
                        if(loads[i] < lowestLoad){
                            lowestLoad = loads[i];
                            lowestPos = i;
                        }
                    }
                }
            }
        }
        return res;
    }
    
    /*
     *     LOCAL delta a partition gets by REMOVING a SET of local vertices out
     *     it ASSUMES that the vertices are on the same partition
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     *     
     *     if newPartition = -1 we evaluate moving to an unknown REMOTE partition
     */
    public double getDeltaGiveVertices(Set<String> movedVertices, int newPartition) {

        if (movedVertices == null || movedVertices.isEmpty()){
            System.out.println("Trying to move an empty set of vertices");
            return 0;
        }
        double delta = 0;
        int fromPartition = m_graph.m_vertexPartition.get(movedVertices.iterator().next());
 
        for(String vertex : movedVertices){ 
//            System.out.println("REMOVE delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            Double vertexWeight = m_graph.m_vertices.get(vertex);
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            Map<String,Double> adjacency = m_graph.m_edges.get(vertex);
            if(adjacency != null){
                double outPull = 0;
                double inPull = 0;
                for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                    //                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                    String toVertex = edge.getKey();
                    Double edgeWeight = edge.getValue();

                    // edges to vertices that are moved together do not contribute to in- or out-pull
                    if(!movedVertices.contains(toVertex)){
                        int toPartition = m_graph.m_vertexPartition.get(toVertex); 
                        if(toPartition == fromPartition){
                            // edge to local vertex which will not be moved out
                            //                        System.out.println("Add weight to inpull: edge to local vertex which will not be moved out");
                            inPull += edgeWeight;
                        }
                        else {
                            // edge to remote vertex or vertex that will be moved out
                            //                        System.out.println("Add weight to outpull: edge to remote vertex which will be moved out");
                            outPull += edgeWeight;
                        }
                    }
                }

                // decides multiplier depending on whether the newPartition is local or not
                double outMultiplier;
                if (newPartition == -1 || PlanHandler.getSitePartition(newPartition) != PlanHandler.getSitePartition(fromPartition)){
                    outMultiplier = DTXN_COST;
                }
                else{
                    outMultiplier = LMPT_COST;
                }

                // update delta
                delta -= vertexWeight + outPull * outMultiplier;
                delta += inPull * outMultiplier;
                //          System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, vertexWeight + (outPull - inPull) * Controller.DTXN_MULTIPLIER, delta));
            }
        }
        return delta;
    }
    
    /*
     *     LOCAL delta a partition gets by ADDING a SET of remote vertices
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     */
    public double getDeltaReceiveVertices(Set<String> movedVertices, int newPartition) {

        if (movedVertices == null || movedVertices.isEmpty()){
            System.out.println("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        for(String vertex : movedVertices){
            // get tuple weight from original site
            Double vertexWeight = m_graph.m_vertices.get(vertex);
//            System.out.println("ADD delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }
            
            // get adjacency list from original site
            Map<String,Double> adjacency = m_graph.m_edges.get(vertex);
            if(adjacency != null){
                double outPull = 0;
                double inPull = 0;
                for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                    //                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                    String toVertex = edge.getKey();
                    Double edgeWeight = edge.getValue();

                    // edges to vertices that are moved together do not contribute to in- or out-pull
                    if(!movedVertices.contains(toVertex)){
                        int toPartition = m_graph.m_vertexPartition.get(toVertex); 

                        if(toPartition == newPartition){
                            // edge to local vertex or to vertex that will be moved in
                            //                        System.out.println("Add weight to inpull");
                            inPull += edgeWeight;
                        }
                        else{
                            // edge to remote vertex that will not be moved in
                            //                        System.out.println("Add weight to outpull");
                            outPull += edgeWeight;
                        }
                    }
                }

                // determine multiplier
                double outMultiplier;
                int fromPartition = m_graph.m_vertexPartition.get(movedVertices.iterator().next());
                if (PlanHandler.getSitePartition(newPartition) != PlanHandler.getSitePartition(fromPartition)){
                    outMultiplier = DTXN_COST;
                }
                else{
                    outMultiplier = LMPT_COST;
                }

                // determine delta
                delta += vertexWeight + outPull * outMultiplier;
                delta -= inPull * outMultiplier;
                //            System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, (inPull - outPull)  * Controller.DTXN_MULTIPLIER - vertexWeight, delta));
            }
        }
        return delta;
    }
    
    public void writePlan(String oldPlanFile, String newPlanFile){
        m_plan_handler.toJSON(oldPlanFile, newPlanFile);
    }
}
