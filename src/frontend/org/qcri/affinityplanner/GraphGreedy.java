package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

public class GraphGreedy extends PartitionerAffinity {

    private static final Logger LOG = Logger.getLogger(GraphGreedy.class);

    //    HashSet<String> DEBUG = new HashSet<String>();
    //    boolean DEB = false;

    public GraphGreedy (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles){

        long t1 = System.currentTimeMillis();
        try{
            m_graph = new AffinityGraph(true, catalogContext, planFile, logFiles, intervalFiles, Controller.MAX_PARTITIONS);
        }
        catch (Exception e){
            Controller.record("Problem while loading graph. Exiting");
            return;
        }

        long t2 = System.currentTimeMillis();
//        Controller.record("Time taken:" + (t2-t1));


        // DEBUG
//        m_graph.toFile(new File("Graph.txt").toPath());
    }

    /**
     * Repartitions the graph by using several heuristics
     * 
     * @return true if could find feasible partitioning, false otherwise
     */   
    @Override
    public boolean repartition () {

        if (Controller.PARTITIONS_PER_SITE == -1 || Controller.MAX_PARTITIONS == -1){
            System.out.println("GraphPartitioner: Must initialize PART_PER_SITE and MAX_PARTITIONS");
            return false;
        }

        // detect overloaded and active partitions
        IntList activePartitions = new IntArrayList(Controller.MAX_PARTITIONS);

        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(AffinityGraph.isActive(i)){
                activePartitions.add(i);
            }
        }

        // find overloaded partitions

        IntList overloadedPartitions = new IntArrayList();

        System.out.println("Load per partition after moving border tuples");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(activePartitions.contains(i)){
                double load =  getLoadPerPartition(i);
                System.out.println(load);
                if (load > Controller.MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        if (! overloadedPartitions.isEmpty()){

            /*
             *  SCALE OUT
             */

            System.out.println("Move hot tuples");

            return offloadHottestTuples(overloadedPartitions, activePartitions);
        }
        else{
            /*
             *  SCALE IN
             */

            scaleIn(activePartitions);
        }
        return true;
    }
    
//    private void testNoOverload(int toPartition){
//        double load =  getLoadPerPartition(toPartition);
//        if (load > Controller.MAX_LOAD_PER_PART){
//            System.out.println("Partition " + toPartition + " is overloaded");
//            System.exit(0);
//        }
//    }

    private boolean offloadHottestTuples(IntList overloadedPartitions, IntList activePartitions){

        int addedPartitions = 0;
        // offload each overloaded partition
        System.out.println("\nLOAD BALANCING");
        System.out.println("#######################");

        for(int overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% offloading site " + overloadedPartition + " %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            System.out.println("Active partitions " + activePartitions.toString());

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            int topk = Math.min(m_graph.numVertices(overloadedPartition), Controller.TOPK);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, topk);

            int numMovedVertices = 0;
            int nextHotTuplePos = 0;
            int lastHotVertexMoved = -1;

            int count_iter = 0;

            Move currMove = null;
            Move candidateMove = null;

            int greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;

            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                System.out.println("\n ######## ITERATION " + (count_iter++) + " ###########");

                // Step 1) add partition and reset if I have over-expanded movingVertices, or if I cannot expand it anymore

                if (currMove != null && 
                        (nextHotTuplePos >= hotVerticesList.size() 
                        || numMovedVertices + currMove.movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART 
                        || currMove.toPartition == -1
                        || !currMove.wasExtended)){

                    if(candidateMove != null 
                            && numMovedVertices + candidateMove.movingVertices.size() < Controller.MAX_MOVED_TUPLES_PER_PART){
                        // before adding a partition, move current candidate if we have one
                        System.out.println("Giving up on expanding the moving set");
                        System.out.println("ACTUALLY moving to " + candidateMove.toPartition 
                                + " with sender delta " + candidateMove.sndDelta + " and receiver delta " + candidateMove.rcvDelta);
                        System.out.println("Moving:\n" + m_graph.verticesToString(candidateMove.movingVertices));

                        m_graph.moveHotVertices(candidateMove.movingVertices, candidateMove.toPartition);
                        numMovedVertices += candidateMove.movingVertices.size();
                        lastHotVertexMoved = nextHotTuplePos - 1;

//                        testNoOverload(candidateMove.toPartition);

                        currMove = null;
                        candidateMove = null;
                        greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;

                        continue;
                    }

                    System.out.println("Cannot expand - Adding a new partition");

                    // We fill up low-order partitions first to minimize the number of servers
                    addedPartitions++;

                    int newPartition = -1;

                    // add Controller.ADDED_PARTITION_CHUNK_SIZE new partitions, set first to be newPartition
                    int newPartCount = 0;
                    for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                        if(!activePartitions.contains(i)){
                            activePartitions.add(i);
                            if (newPartCount == 0){
                                newPartition = i;
                            }
                            newPartCount++;
                            if (newPartCount >= Controller.ADDED_PARTITION_CHUNK_SIZE){
                                break;
                            }
                        }
                    }

                    if(activePartitions.size() >= Controller.MAX_PARTITIONS 
                            || addedPartitions > Controller.MAX_PARTITIONS_ADDED
                            || newPartition == -1){
                        
                        System.out.println("GIVING UP!! Cannot add new partition to offload " + overloadedPartitions);
                        
                        return false;
                    }

                    if (nextHotTuplePos >= hotVerticesList.size()){
                        System.out.println("GIVING UP!! No more hot tuples");
                        return false;
                    }
                    
                    if (currMove.sndDelta >= 0){
                        System.out.println("GIVING UP!! This move has no benefit for the sender");
                        return false;
                    }
                    
                    m_graph.moveHotVertices(candidateMove.movingVertices, newPartition);
                    numMovedVertices += candidateMove.movingVertices.size();
                    lastHotVertexMoved = nextHotTuplePos - 1;

                    currMove = null;
                    candidateMove = null;
                    greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;
                    
                    continue;

                } // END if (numMovedVertices + movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART || nextPosToMove >= hotVerticesList.size() || (toPart_sndDelta_glbDelta.fst != null && toPart_sndDelta_glbDelta.fst == -1))

                // Step 2) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple

                if (currMove == null){
                    currMove = new Move();
                }
                else {
                    System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                    System.out.println("Current sender delta " + currMove.sndDelta);                    
                }

                nextHotTuplePos = expandMovingVertices (currMove, hotVerticesList, nextHotTuplePos, activePartitions, overloadedPartition);
                if (!currMove.wasExtended){
                    continue;
                }

                // Step 3) move the vertices if could expand

                System.out.println("Moving:\n" + m_graph.verticesToString(currMove.movingVertices));
                System.out.println("Receiver: " + currMove.toPartition + ", receiver delta " + currMove.rcvDelta);

                if(currMove.toPartition != -1
                        && currMove.sndDelta <= Controller.MIN_SENDER_GAIN_MOVE * -1
                        && (getLoadPerPartition(currMove.toPartition) + currMove.rcvDelta < Controller.MAX_LOAD_PER_PART
                                || currMove.rcvDelta <= 0)){
                    
                    if(candidateMove == null || currMove.rcvDelta <= candidateMove.rcvDelta){

                        System.out.println("CANDIDATE for moving to " + currMove.toPartition);

                        // record this move as a candidate
                        candidateMove = currMove.clone();
                        greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;
                    }
                    else{                        
                        // current candidate is better  
                        greedyStepsAhead--;
                    }

                }
                else if(candidateMove != null){
                    // this not a candidate but I have found one
                    greedyStepsAhead--;
                }
                
                // move after making enough steps
                if (greedyStepsAhead == 0){
                    System.out.println("ACTUALLY moving to " + candidateMove.toPartition + " with sender delta " 
                            + candidateMove.sndDelta + " and receiver delta " + candidateMove.rcvDelta);
                    System.out.println("Moving:\n" + m_graph.verticesToString(candidateMove.movingVertices));

                    m_graph.moveHotVertices(candidateMove.movingVertices, candidateMove.toPartition);
                    numMovedVertices += candidateMove.movingVertices.size();
                    lastHotVertexMoved = nextHotTuplePos - 1;
//                    testNoOverload(candidateMove.toPartition);

                    currMove = null;
                    candidateMove = null;
                    greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;
                }

            } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
        }// END for(int overloadedPartition : overloadedPartitions)
        return true;
    }

    /**
     * updates the set movingVertices with one more vertex, either the most affine to the current movingVertices 
     * or the next vertex in the verticesToMove list, depending on which one is more convenient to move out.
     * 
     * Inputs:
     * - toPartitionDelta.fst == null indicates that we don't have a predefined partition to move to
     * 
     * Outputs:
     * - returns the next position to move in verticesToMove
     * - modifies the argument movingVertices to add the new vertex
     * - modifies the argument toPartitionDelta to indicate where should the new vertex moved; (-1, inf) indicates that there is no move possible
     * 
     */
    private int expandMovingVertices (Move move, IntList hotVertices, int nextHotTuplePos, IntList activePartitions, int fromPartition){

        if (move.movingVertices.isEmpty()){

            // if empty, add a new hot vertex
            assert (nextHotTuplePos < hotVertices.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 

            int nextHotVertex;

            do{
                nextHotVertex = hotVertices.get(nextHotTuplePos);
                nextHotTuplePos++;
            } while (nextHotTuplePos < hotVertices.size() 
                    && AffinityGraph.m_vertexPartition.get(nextHotVertex) != fromPartition);
            // the second condition is for the case where the vertex has been moved already 

            if (nextHotTuplePos == hotVertices.size()){
                move.wasExtended = false;
                return nextHotTuplePos;
            }

            move.movingVertices.add(nextHotVertex);
            move.wasExtended = true;
            System.out.println("Adding vertex " + m_graph.m_vertexName.get(nextHotVertex));

            findBestPartition(move, fromPartition, activePartitions);

            if(nextHotVertex == 0){
                throw new Error();
            }
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            move.clearExceptMovingVertices();

            int affineVertex = getMostAffineExtension(move.movingVertices, fromPartition);

            if(affineVertex != 0){

                // DEBUG
                System.out.println("Adding edge extension: " + AffinityGraph.m_vertexName.get(affineVertex));

                move.movingVertices.add(affineVertex);
                move.wasExtended = true;

                // this will populate all the fields of move
                findBestPartition(move, fromPartition, activePartitions);

            }

            else{
                System.out.println("Could not expand");
                move.wasExtended = false;
                return nextHotTuplePos;
            }

        } // END if(!movedVertices.isEmpty())

        return nextHotTuplePos;

    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    protected int getMostAffineExtension(IntSet vertices, int senderPartition){
        
        double maxAffinity = -1;
        int res = 0;

        for(int vertex : vertices){
            
            // DEBUG
//            System.out.println("Looking at the adjacency list of vertex " + AffinityGraph.m_vertexName.get(vertex));

            Int2DoubleMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for(Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                    int adjacentVertex = edge.getIntKey();
                    double affinity = edge.getDoubleValue();

                    // DEBUG
//                    System.out.println("Looking at adjacent vertex " + AffinityGraph.m_vertexName.get(adjacentVertex)
//                        + " with affinity " + affinity);

                    if (affinity > maxAffinity
                            && AffinityGraph.m_vertexPartition.get(adjacentVertex) == senderPartition
                            && !vertices.contains(adjacentVertex)) {

                        //DEBUG
//                        System.out.println("Picked");
                        
                        maxAffinity = affinity;
                        res = adjacentVertex;
                    }
                }
            }
        }

        if (res == 0 || maxAffinity < Controller.LOCAL_AFFINITY_THRESHOLD){

            // look for affine vertices in nearby partition

            IntSet vertexSingleton = new IntOpenHashSet();

            for(int vertex : vertices){

                Int2DoubleMap adjacency = AffinityGraph.m_edges.get(vertex);
                if(adjacency != null){

                    for(Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                        
                        int adjacentVertex = edge.getIntKey();
                        double affinity = edge.getDoubleValue();
                        
                        int newVertexPartition = AffinityGraph.m_vertexPartition.get(adjacentVertex);
                                                
                        vertexSingleton.add(adjacentVertex);
                        // setting the destination partition to be remote = worst case for the sender
                        double newVertexPartitionDelta = getSenderDelta(vertexSingleton, newVertexPartition, false);
                        vertexSingleton.remove(adjacentVertex);
                        
                        if (affinity > maxAffinity
                                && !vertices.contains(adjacentVertex)
                                && newVertexPartition != senderPartition 
                                && newVertexPartitionDelta <= 0) {
                            maxAffinity = affinity;
                            res = adjacentVertex;
                        }
                    }
                }
            }            
        }

        return res;
    }


    @Override
    protected double getLoadVertices(IntSet vertices){

        //        DEBUG.clear();

        double load = 0;

        int fromPartition = -1;

        for(int vertex : vertices){

            // local accesses - vertex weight
            double vertexWeight = AffinityGraph.m_vertices.get(vertex);

            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            load += vertexWeight;

            // remote accesses
            if (fromPartition == -1){
                fromPartition = AffinityGraph.m_vertexPartition.get(vertex);
            }
            else{
                if(fromPartition != AffinityGraph.m_vertexPartition.get(vertex)){
                    int otherPartition = AffinityGraph.m_vertexPartition.get(vertex);
                    System.out.println("vertex with hash " + vertex + " and name " + AffinityGraph.m_vertexName.get(vertex) + " is not on partition " + fromPartition + " but on partition " + otherPartition);
                    System.out.println("Vertex is in PartitionVertex for partition " + fromPartition + ": " + AffinityGraph.m_partitionVertices.get(fromPartition).contains(vertex));
                    System.out.println("Vertex is in PartitionVertex for partition " + otherPartition + ": " + AffinityGraph.m_partitionVertices.get(otherPartition).contains(vertex));
                    System.exit(0);
                }
            }

            int fromSite = PlanHandler.getSitePartition(fromPartition);

            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){

                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){

                    int toVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    int toPartition = AffinityGraph.m_vertexPartition.get(toVertex);

                    if (toPartition != fromPartition){

                        int toSite = PlanHandler.getSitePartition(toPartition);
                        double h = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                        load += edgeWeight * h;
                    }
                }
            }
        }
        return load;
    }

    @Override
    public double getLoadPerPartition(int partition){

        if (!AffinityGraph.isActive(partition)){
            return 0;
        }

        return getLoadVertices(AffinityGraph.m_partitionVertices.get(partition));
    }

    @Override
    protected double getGlobalDelta(IntSet movingVertices, int toPartition){

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);

        for(int movingVertex : movingVertices){ 

            int fromPartition = AffinityGraph.m_vertexPartition.get(movingVertex);
            int fromSite = PlanHandler.getSitePartition(fromPartition);

            // if fromPartition = toPartition, there is no move so the delta is 0
            if (fromPartition != toPartition){

                Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(movingVertex);
                if(adjacency != null){

                    for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                        int adjacentVertex = edge.getIntKey();
                        double edgeWeight = edge.getDoubleValue();

                        if(!movingVertices.contains(adjacentVertex)){
                            int adjacentVertexPartition = AffinityGraph.m_vertexPartition.get(adjacentVertex);
                            int adjacentVertexSite = PlanHandler.getSitePartition(adjacentVertexPartition);

                            if (adjacentVertexPartition == fromPartition){
                                // new MPTs from fromPartition to toPartition
                                double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                                delta += edgeWeight * k;
                            }
                            else if (adjacentVertexPartition == toPartition){
                                // eliminating MTPs from fromPartition to toPartition
                                double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                                delta -= edgeWeight * k;
                            }
                            else{
                                // from fromPartition -> adjacentPartition to toPartition -> adjacentPartition
                                double h = 0;
                                if (adjacentVertexSite == fromSite && adjacentVertexSite != toSite){
                                    // we had a local mpt, now we have a dtxn 
                                    h = Controller.DTXN_COST - Controller.LMPT_COST;
                                }
                                else if (adjacentVertexSite != fromSite && adjacentVertexSite == toSite){
                                    // we had a dtxn, now we have a local mpt 
                                    h = Controller.LMPT_COST - Controller.DTXN_COST;
                                }
                                delta += edgeWeight * h;
                            }
                        }
                    }
                }
            }
        }

        return delta;
    }

    @Override
    protected double getReceiverDelta(IntSet movingVertices, int toPartition){

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);

        for(int movedVertex : movingVertices){ 

            double vertexWeight = AffinityGraph.m_vertices.get(movedVertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            int fromPartition = AffinityGraph.m_vertexPartition.get(movedVertex);
            int fromSite = PlanHandler.getSitePartition(fromPartition);

            // if fromPartition == toPartition there is no movement so delta is 0
            if(fromPartition != toPartition){

                delta += vertexWeight;

                // compute cost of new multi-partition transactions
                Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(movedVertex);
                if(adjacency != null){

                    for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                        int adjacentVertex = edge.getIntKey();
                        double edgeWeight = edge.getDoubleValue();

                        int adjacentVertexPartition = AffinityGraph.m_vertexPartition.get(adjacentVertex);
                        int adjacentVertexSite = PlanHandler.getSitePartition(adjacentVertexPartition);

                        if (adjacentVertexPartition == toPartition){
                            // the moved vertex used to be accessed with a tuple in the destination partition
                            // the destination saves old MPTs by moving the vertex
                            double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                            delta -= edgeWeight * k;
                        }
                        else if (!movingVertices.contains(adjacentVertex)){
                            // the destination pays new MPTs unless the adjacent vertex is also moved here 
                            if (adjacentVertexSite == toSite){
                                delta += edgeWeight * Controller.LMPT_COST;
                            }
                            else {
                                delta += edgeWeight * Controller.DTXN_COST;                            
                            }
                        }
                    }
                }
            }
        }

        return delta;
    }

    @Override
    protected double getSenderDelta(IntSet movingVertices, int senderPartition, boolean toPartitionLocal) {

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int senderSite = PlanHandler.getSitePartition(senderPartition);
        
        for(int movingVertex : movingVertices){
            
            double vertexWeight = AffinityGraph.m_vertices.get(movingVertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            int fromPartition = AffinityGraph.m_vertexPartition.get(movingVertex);

            // if fromPartition != senderPartition, there is no change for the sender so no delta
            if(fromPartition == senderPartition){
                
                // lose vertex weight
                delta -= vertexWeight;

                // consider MPTs
                Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(movingVertex);
                if(adjacency != null){

                    for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                        int adjacentVertex = edge.getIntKey();
                        double edgeWeight = edge.getDoubleValue();

                        int adjacentVertexPartition = AffinityGraph.m_vertexPartition.get(adjacentVertex);
                        int adjacentVertexSite = PlanHandler.getSitePartition(adjacentVertexPartition);
                        
                        if (! (adjacentVertexPartition == senderPartition && movingVertices.contains(adjacentVertex))){
                            // if the two vertices are local to the sender and are moved together, only save the vertex weight 
                            // else need to consider MPT costs
                            if (adjacentVertexPartition == senderPartition) {
                                // the sender was paying nothing, now pays the senderPartition -> toPartition MPTs
                                // the two vertices are not moved together
                                double k = toPartitionLocal ? Controller.LMPT_COST : Controller.DTXN_COST;
                                delta += edgeWeight * k;
                            } else if (adjacentVertexSite == senderSite){
                                    // the sender was paying senderPartition -> adjacentPartition MPTs, now pays nothing
                                delta -= edgeWeight * Controller.LMPT_COST;
                            } else {
                                delta -= edgeWeight * Controller.DTXN_COST;
                            }
                        }
                    }
                }
            }
        }

        return delta;
    }

    @Override
    protected void updateAttractions (Int2DoubleMap adjacency, double[] attractions){
        for (int toVertex : adjacency.keySet()){
            int other_partition = AffinityGraph.m_vertexPartition.get(toVertex);
            double edge_weight = adjacency.get(toVertex);
            attractions[other_partition] += edge_weight;
        } // END for (String toVertex : adjacency.keySet())
    }

}
