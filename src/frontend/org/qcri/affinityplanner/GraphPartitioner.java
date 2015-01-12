package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.voltdb.CatalogContext;

public class GraphPartitioner {

    private static final Logger LOG = Logger.getLogger(GraphPartitioner.class);

    public static int MIN_LOAD_PER_PART = Integer.MIN_VALUE;
    public static int MAX_LOAD_PER_PART = Integer.MAX_VALUE;
    public static double LMPT_COST = 1.1;
    public static double DTXN_COST = 5.0;
    public static int MAX_MOVED_TUPLES_PER_PART = 100;
    public static int MIN_GAIN_MOVE = 0;
    public static int MAX_PARTITIONS_ADDED = 6;

    private AffinityGraph m_graph;

    public GraphPartitioner (AffinityGraph graph, File planFile, CatalogContext catalogContext){
        m_graph = graph;
    }

    /**
     * Repartitions the graph by using several heuristics
     * 
     * @return true if could find feasible partitioning, false otherwise
     */   
    public boolean repartition () {
        if (Controller.PARTITIONS_PER_SITE == -1 || Controller.MAX_PARTITIONS == -1){
            System.out.println("GraphPartitioner: Must initialize PART_PER_SITE and MAX_PARTITIONS");
            return false;
        }

        System.out.println("Loads per partition before reconfiguration");

        // detect overloaded and active partitions
        Set<Integer> activePartitions = new HashSet<Integer>();
        Set<Integer> overloadedPartitions = new HashSet<Integer>();
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(!m_graph.m_partitionVertices.get(i).isEmpty()){
                activePartitions.add(i);
                System.out.println(getLoadPerPartition(i));
                if (getLoadPerPartition(i) > MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        /*
         * MOVE BORDER TUPLES
         */

        offloadBorderTuples(activePartitions);

        if (! overloadedPartitions.isEmpty()){

            /*
             *  SCALE OUT
             */

            return offloadHottestTuples(overloadedPartitions, activePartitions);
        }
        else{
            /*
             *  SCALE IN
             */

            scaleIn(overloadedPartitions, activePartitions);
        }
        return true;
    }

    private void offloadBorderTuples(Set<Integer> activePartitions){

        LOG.debug("Moving border vertices");

//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        for (Integer from_part : activePartitions){
            
            List<List<String>> allBorderVertices = getBorderVertices(from_part, MAX_MOVED_TUPLES_PER_PART);

            // vertices that will have to be removed from other lists
            Set<String> movedVerticesSet = new HashSet<String>();

            for (Integer to_part : activePartitions){

                LOG.debug("Moving FROM partition " + from_part + " TO partition " + to_part);

                List<String> borderVertices = allBorderVertices.get(to_part);
                
                LOG.debug("Border vertices:");
                for(String vertex: borderVertices){
                    LOG.debug(vertex);
                }

                final int actualMaxMovedVertices = borderVertices.size();

                int numMovedVertices = 0;
                Set<String> movingVertices = new HashSet<String>();
                int nextPosToMove = 0;
                int lastHotVertexMoved = -1;
                int retryCount = 1;

                while(numMovedVertices + movingVertices.size() < actualMaxMovedVertices && nextPosToMove < borderVertices.size()){
                    nextPosToMove = expandMovingVertices (movingVertices, borderVertices, nextPosToMove, from_part);

                    if (nextPosToMove == -1){
                        // if cannot expand anymore restart the process after skipping the first not moved vertex in the list
                        nextPosToMove = lastHotVertexMoved + 1 + retryCount;
                        movingVertices.clear();
                        retryCount++;
                    }
                    else{
                        int movedVertices = tryMoveVertices(movingVertices, from_part, to_part);
                        if(movedVertices > 0){
                            LOG.debug("Moving!");
                            numMovedVertices += movedVertices;  
                            lastHotVertexMoved = nextPosToMove - 1;
                            movedVerticesSet.addAll(movingVertices);
                            movingVertices.clear();
                        }
                    }
                    
                    //DEBUG
//                    try {
//                        br.readLine();
//                    } catch (IOException e) {
//                        // TODO Auto-generated catch block
//                        e.printStackTrace();
//                    }
                }

            } // END for (int other_part = 0; other_part < MAX_PARTITIONS; other_part++)
        } // END for (int part = 0; part < MAX_PARTITIONS; part++)

        // 1) get list of tuples that have the highest external pull to a single other partition
        // 2) expand the tuple with the most affine tuples such that adding these tuples reduces the cost after movement
        // 3) assess if we can move the tuple
    }

    private boolean offloadHottestTuples(Set<Integer> overloadedPartitions, Set<Integer> activePartitions){
        int addedPartitions = 0;
        // offload each overloaded partition
        System.out.println("SCALING OUT");
        System.out.println("#######################");

        for(Integer overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("offloading site " + overloadedPartition);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            List<String> hotVerticesList = getHottestVertices(overloadedPartition, MAX_MOVED_TUPLES_PER_PART);
            final int actualMaxMovedVertices = hotVerticesList.size();

            // DEBUG
            //            System.out.println("hot vertices:");
            //            for (String hotVertex : hotVerticesNotMoved){
            //                System.out.println(hotVertex);
            //            }

            int numMovedVertices = 0;
            Set<String> movingVertices = new HashSet<String>();

            int nextPosToMove = 0;
            int lastHotVertexMoved = -1;
            int retryCount = 1;

            // DEBUG
//            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            int count_iter = 0;

            while(getLoadPerPartition(overloadedPartition) > MAX_LOAD_PER_PART){

                // DEBUG
                //                LOG.debug("Press ENTER to continue");
                //                try {
                //                    br.readLine();
                //                } catch (IOException e) {
                //                    // TODO Auto-generated catch block
                //                    e.printStackTrace();
                //                }
                LOG.debug("Iteration " + (count_iter++));

                // Step 1) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple
                nextPosToMove = expandMovingVertices (movingVertices, hotVerticesList, nextPosToMove, overloadedPartition);

                // if cannot expand, skip the first element in the hot vertex list and retry 
                if (nextPosToMove == -1){
                    nextPosToMove = lastHotVertexMoved + 1 + retryCount;
                    movingVertices.clear();
                    retryCount++;
                }

                // Step 2) add partition if I have over-expanded movingVertices or if I cannot expand it anymore

                if (numMovedVertices + movingVertices.size() > actualMaxMovedVertices || nextPosToMove >= hotVerticesList.size()){
                    System.out.println("Adding a new partition");

                    if(activePartitions.size() < Controller.MAX_PARTITIONS && addedPartitions < MAX_PARTITIONS_ADDED){

                        // We fill up low-order partitions first to minimize the number of servers
                        addedPartitions++;
                        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                            if(!activePartitions.contains(i)){
                                activePartitions.add(i);
                                break;
                            }
                        }

                        nextPosToMove = lastHotVertexMoved + 1;
                        retryCount = 1;
                        movingVertices.clear();
                    }
                    else{
                        System.out.println("Partition " + overloadedPartitions + " cannot be offloaded.");
                        return false;
                    }
                }
                //                System.out.println("Adding a new vertex");

                
                // Step 3) search site to offload (might not find it)

                // first try to offload to local partitions
                boolean moved = false;
                Collection<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(overloadedPartition));

                for(Integer toPartition : localPartitions){

                    LOG.debug("Trying with partition " + toPartition);
                    int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition);

                    if(movedVertices > 0){
                        numMovedVertices += movedVertices;  
                        lastHotVertexMoved = nextPosToMove - 1;
                        movingVertices.clear();
                        moved = true;
                        break;
                    }
                }

                // then try to offload to remote partitions
                if (!moved){

                    for(Integer toPartition : activePartitions){

                        if(!localPartitions.contains(toPartition)){
                            LOG.debug("Trying with partition " + toPartition);
                            int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition);

                            if(movedVertices > 0){
                                numMovedVertices += movedVertices;    
                                lastHotVertexMoved = nextPosToMove - 1;
                                movingVertices.clear();
                                break;
                            }
                        }
                    }
                }
            } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
        }// END for(Integer overloadedPartition : overloadedPartitions)
        return true;
    }

    private void scaleIn(Set<Integer> overloadedPartitions, Set<Integer> activePartitions){
        /*
         *  SCALE IN
         *  
         *  very simple policy: if a partition is underloaded, try to move its whole content to another partition
         */

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

    /**
     * updates the set movingVertices with one more vertex, either the most affine to the current movingVertices 
     * or the next vertex in the verticesToMove list, depending on which one is more convenient to move out.
     * 
     * @param partition
     * @param movingVertices
     * @param verticesToMove
     * @param nextPosToMove
     * @return the new nextVertexToMove such that all previous elements of verticesToMove have been added already
     */
    private int expandMovingVertices (Set<String> movingVertices, List<String> verticesToMove, int nextPosToMove, int partition){

        if (movingVertices.isEmpty()){
            // if empty, add a new hot vertex
            assert (nextPosToMove < verticesToMove.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 
            String nextHotVertex = null;
            do{
                nextHotVertex = verticesToMove.get(nextPosToMove);
                nextPosToMove++;
            } while (m_graph.m_vertexPartition.get(nextHotVertex) != partition);
            assert (nextHotVertex != null); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
            LOG.debug("Adding hot vertex " + nextHotVertex);
            movingVertices.add(nextHotVertex);
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            // assess gain with extension
            double deltaEdgeExtension = Double.MIN_VALUE;                        

            String nextEdgeExtension = getMostAffineExtension(movingVertices);
            if(nextEdgeExtension != null){
                movingVertices.add(nextEdgeExtension);
                deltaEdgeExtension = getDeltaGiveVertices(movingVertices, -1);
                movingVertices.remove(nextEdgeExtension);
            }

            // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
            double deltaHotTuple = Double.MIN_VALUE;

            String nextVertexToMove = null;
            int skip = 0;

            if(nextPosToMove < verticesToMove.size()){

                do{
                    nextVertexToMove = verticesToMove.get(nextPosToMove + skip);
                    skip ++;
                } while (movingVertices.contains(nextVertexToMove) && nextPosToMove + skip < verticesToMove.size()); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here

                if (! movingVertices.contains(nextVertexToMove)){
                    assert(nextVertexToMove != null);
                    movingVertices.add(nextVertexToMove);
                    deltaHotTuple = getDeltaGiveVertices(movingVertices, -1);
                    movingVertices.remove(nextVertexToMove);
                }
            }

            // pick best available choice
            if(deltaEdgeExtension == Double.MIN_VALUE && deltaHotTuple == Double.MIN_VALUE){
                // no choice available
                return -1;
            }
            else{
                if (deltaEdgeExtension < deltaHotTuple){
                    movingVertices.add(nextEdgeExtension);
                    LOG.debug("Adding edge extension: " + nextEdgeExtension);
                }
                else{
                    movingVertices.add(nextVertexToMove);
                    LOG.debug("Adding vertex from list: " + nextVertexToMove);
                    nextPosToMove += skip;
                }
            }

        } // END if(!movedVertices.isEmpty())

        // DEBUG
        LOG.debug("Moving vertices: ");
        for (String vertex : movingVertices){
            LOG.debug(vertex);
        }       
        return nextPosToMove;

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
    private int tryMoveVertices(Set<String> movingVertices, Integer fromPartition, Integer toPartition) {

        int numMovedVertices = 0;
        double deltaFromPartition = getDeltaGiveVertices(movingVertices, toPartition);
        double deltaToPartition = getDeltaReceiveVertices(movingVertices, toPartition);

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

    /*
     * computes load of a set of vertices in the current partition. this is different from the weight of a vertex because it considers
     * both direct accesses of the vertex and the cost of remote accesses
     */
    private Double getLoadInCurrPartition(Set<String> vertices){
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

    /**
     * Returns sorted (descending order) list of top-k vertices from site
     * 
     * @param partition
     * @param k
     * @return
     */
    private List<String> getHottestVertices(int partition, int k){

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
     * Returns a list of lists of vertices - one list for every remote partition
     * Each list includes up to k elements with the highest outside attraction to that partition
     * The list is sorted by attraction in a descending order
     * 
     * @param this_partition
     * @param k
     * @return
     */
    private List<List<String>> getBorderVertices (int this_partition, int k){

        k = Math.min(k, m_graph.m_partitionVertices.get(this_partition).size());

        List<List<String>> res = new ArrayList<List<String>>(Controller.MAX_PARTITIONS);
        for (int i = 0; i < Controller.MAX_PARTITIONS; i++){
            res.add(new ArrayList<String> (k));
        }
        final Map<String, double[]> attractionMap = new HashMap <String, double[]> ();

        int[] lowest_attraction_position = new int[Controller.MAX_PARTITIONS];
        double[] lowest_attraction = new double[Controller.MAX_PARTITIONS];


        for(String from_vertex : m_graph.m_partitionVertices.get(this_partition)){

            // compute out attraction
            double in_attraction = 0;
            double[] out_attraction = new double[Controller.MAX_PARTITIONS];

            Map<String,Double> adjacency = m_graph.m_edges.get(from_vertex);
            if (adjacency != null){
                
                for (String toVertex : adjacency.keySet()){
                    
                    int other_partition = m_graph.m_vertexPartition.get(toVertex);
                    double edge_weight = adjacency.get(toVertex);
                    if(other_partition != this_partition){
                        out_attraction[other_partition] += edge_weight;
                    }
                    else{
                        in_attraction += edge_weight;
                    }
                } // END for (String toVertex : adjacency.keySet())

                // rank for each partition
                for(int otherPart = 0; otherPart < Controller.MAX_PARTITIONS; otherPart++){

                    // consider deltas and ignore negative attraction
                    out_attraction[otherPart] -= in_attraction;
                    if (out_attraction[otherPart] <= 0){
                        continue;
                    }
                    
                    List<String> topk = res.get(otherPart);
    
                    if(topk.size() < k){
                        
                        topk.add(from_vertex);
    
                        double[] attractions = attractionMap.get(from_vertex);
                        if (attractions == null){
                            attractions = new double[Controller.MAX_PARTITIONS];
                            attractionMap.put(from_vertex, attractions);
                        }
                        attractions[otherPart] = out_attraction[otherPart];
                        attractionMap.put(from_vertex, attractions);
    
                        if (out_attraction[otherPart] < lowest_attraction[otherPart]){
                            lowest_attraction[otherPart] = out_attraction[otherPart];
                            lowest_attraction_position[otherPart] = topk.size() - 1;
                        }
                    }
                    else{
                        if (out_attraction[otherPart] > lowest_attraction[otherPart]){
    
                            // cleanup attractionMap
                            String lowestVertex = topk.get(lowest_attraction_position[otherPart]);
                            double[] attractions = attractionMap.get(lowestVertex);
                            int nonZeroPos = -1;
                            for(int j = 0; j < attractions.length; j++){
                                if (attractions[j] != 0){
                                    nonZeroPos = j;
                                    break;
                                }
                            }
                            if (nonZeroPos == -1){
                                attractionMap.remove(lowestVertex);
                            }
    
                            topk.set(lowest_attraction_position[otherPart], from_vertex);
    
                            attractions = attractionMap.get(from_vertex);
                            if (attractions == null){
                                attractions = new double[Controller.MAX_PARTITIONS];
                                attractionMap.put(from_vertex, attractions);
                            }
                            attractions[otherPart] = out_attraction[otherPart];
                            attractionMap.put(from_vertex, attractions);
    
                            // recompute minimum
                            lowest_attraction[otherPart] = out_attraction[otherPart];
                            for (int posList = 0; posList < k; posList++){
                                String vertex = topk.get(posList);
                                double attraction = attractionMap.get(vertex)[otherPart];
                                if(attraction < lowest_attraction[otherPart]){
                                    lowest_attraction[otherPart] = attraction;
                                    lowest_attraction_position[otherPart] = posList;
                                }
                            }
                        }
                    }
                } // END for(int otherPart = 1; otherPart < MAX_PARTITIONS; otherPart++)
            } // END if (adjacency != null)
        } // END for(String from_vertex : m_graph.m_partitionVertices.get(this_partition))
       
        // sorting
        for(int otherPart = 1; otherPart < Controller.MAX_PARTITIONS; otherPart++){
            List<String> topk = res.get(otherPart);

            // sort determines an _ascending_ order
            // Comparator should return "a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second"
            // We want a _descending_ order, so we need to invert the comparator result

            final int part = otherPart; // make Java happy

            Collections.sort(topk, new Comparator<String>(){                
                @Override
                public int compare(String o1, String o2) {
                    if (attractionMap.get(o1)[part] < attractionMap.get(o2)[part]){
                        return 1;
                    }
                    else if (attractionMap.get(o1)[part] > attractionMap.get(o2)[part]){
                        return -1;
                    }
                    return 0;
                }                
            });
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
    private double getDeltaGiveVertices(Set<String> movedVertices, int newPartition) {

        if (movedVertices == null || movedVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }
        double delta = 0;
        int fromPartition = m_graph.m_vertexPartition.get(movedVertices.iterator().next());

        for(String vertex : movedVertices){ 
            //            LOG.debug("REMOVE delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            Double vertexWeight = m_graph.m_vertices.get(vertex);
            if (vertexWeight == null){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            Map<String,Double> adjacency = m_graph.m_edges.get(vertex);
            if(adjacency != null){
                double outPull = 0;
                double inPull = 0;
                for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                    //                LOG.debug("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                    String toVertex = edge.getKey();
                    Double edgeWeight = edge.getValue();

                    // edges to vertices that are moved together do not contribute to in- or out-pull
                    if(!movedVertices.contains(toVertex)){
                        int toPartition = m_graph.m_vertexPartition.get(toVertex); 
                        if(toPartition == fromPartition){
                            // edge to local vertex which will not be moved out
                            //                        LOG.debug("Add weight to inpull: edge to local vertex which will not be moved out");
                            inPull += edgeWeight;
                        }
                        else {
                            // edge to remote vertex or vertex that will be moved out
                            //                        LOG.debug("Add weight to outpull: edge to remote vertex which will be moved out");
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
                //          LOG.debug(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, vertexWeight + (outPull - inPull) * Controller.DTXN_MULTIPLIER, delta));
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
    private double getDeltaReceiveVertices(Set<String> movedVertices, int newPartition) {

        if (movedVertices == null || movedVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        for(String vertex : movedVertices){
            // get tuple weight from original site
            Double vertexWeight = m_graph.m_vertices.get(vertex);
            //            LOG.debug("ADD delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            if (vertexWeight == null){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            // get adjacency list from original site
            Map<String,Double> adjacency = m_graph.m_edges.get(vertex);
            if(adjacency != null){
                double outPull = 0;
                double inPull = 0;
                for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                    //                LOG.debug("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                    String toVertex = edge.getKey();
                    Double edgeWeight = edge.getValue();

                    // edges to vertices that are moved together do not contribute to in- or out-pull
                    if(!movedVertices.contains(toVertex)){
                        int toPartition = m_graph.m_vertexPartition.get(toVertex); 

                        if(toPartition == newPartition){
                            // edge to local vertex or to vertex that will be moved in
                            //                        LOG.debug("Add weight to inpull");
                            inPull += edgeWeight;
                        }
                        else{
                            // edge to remote vertex that will not be moved in
                            //                        LOG.debug("Add weight to outpull");
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
                //            LOG.debug(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, (inPull - outPull)  * Controller.DTXN_MULTIPLIER - vertexWeight, delta));
            }
        }
        return delta;
    }

    public void writePlan(String newPlanFile){
        m_graph.planToJSON(newPlanFile);
    }
}
