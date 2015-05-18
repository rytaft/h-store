package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

public class GraphGreedyExtended extends PartitionerAffinity {

    private static final Logger LOG = Logger.getLogger(GraphGreedyExtended.class);

    //    HashSet<String> DEBUG = new HashSet<String>();
    //    boolean DEB = false;

    public GraphGreedyExtended (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles){

        long t1 = System.currentTimeMillis();
        try{
            m_graph = new AffinityGraph(true, catalogContext, planFile, logFiles, intervalFiles, Controller.MAX_PARTITIONS);
        }
        catch (Exception e){
            Controller.record("Problem while loading graph. Exiting");
            return;
        }

        long t2 = System.currentTimeMillis();
        Controller.record("Time taken:" + (t2-t1));
    }

    /**
     * Repartitions the graph by using several heuristics
     * 
     * @return true if could find feasible partitioning, false otherwise
     */   
    @Override
    public boolean repartition () {

        //        System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
        //        System.out.println("Is it in 1? " + AffinityGraph.m_partitionVertices.get(1).contains(-1549464086));
        //        System.out.println("Is it in 2? " + AffinityGraph.m_partitionVertices.get(2).contains(-1549464086));
        //        System.out.println("Is it in 3? " + AffinityGraph.m_partitionVertices.get(3).contains(-1549464086));
        //        System.out.println("Is it in 4? " + AffinityGraph.m_partitionVertices.get(4).contains(-1549464086));
        //        System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
        //        System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));


        if (Controller.PARTITIONS_PER_SITE == -1 || Controller.MAX_PARTITIONS == -1){
            System.out.println("GraphPartitioner: Must initialize PART_PER_SITE and MAX_PARTITIONS");
            return false;
        }

        // detect overloaded and active partitions
        IntList activePartitions = new IntArrayList(Controller.MAX_PARTITIONS);

        System.out.println("Load per partition before reconfiguration");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(AffinityGraph.isActive(i)){
                activePartitions.add(i);
                System.out.println(getLoadPerPartition(i));
            }
        }

        /*
         * MOVE BORDER TUPLES
         */

        System.out.println("Move border tuples");

        offloadBorderTuples(activePartitions);

        // find overloaded partitions

        IntList overloadedPartitions = new IntArrayList(Controller.MAX_PARTITIONS);

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

    private void offloadBorderTuples(IntList activePartitions){

        System.out.println("MOVING BORDER VERTICES");
        System.out.println("#######################");

        for (int from_part : activePartitions){

            List<IntList> allBorderVertices = getBorderVertices(from_part, Controller.MAX_MOVED_TUPLES_PER_PART);

            // vertices that will have to be removed from other lists
            IntSet movedVerticesSet = new IntOpenHashSet ();

            for (int to_part : activePartitions){

                if (from_part == to_part){
                    continue;
                }

                //                System.out.println("Moving FROM partition " + from_part + " TO partition " + to_part);

                // 1) get list of tuples that have the highest external pull to a single other partition

                IntList borderVertices = allBorderVertices.get(to_part);

                LOG.debug("Border vertices:");
                for(int vertex: borderVertices){
                    LOG.debug(vertex);
                }

                int numMovedVertices = 0;
                IntOpenHashSet movingVertices = new IntOpenHashSet();
                int nextPosToMove = 0;
                //                int lastHotVertexMoved = -1;
                //                int retryCount = 1;

                Pair<Integer,Double> toPartitionDelta = new Pair<Integer,Double> (to_part, 1.0);

                while(numMovedVertices + movingVertices.size() < Controller.MAX_MOVED_TUPLES_PER_PART 
                        && nextPosToMove < borderVertices.size()
                        && nextPosToMove != -1){

                    // 2) expand the tuple with the most affine tuples such that adding these tuples reduces the cost after movement

                    nextPosToMove = expandMovingVertices (movingVertices, toPartitionDelta, borderVertices, nextPosToMove, activePartitions, from_part);

                    //                    if (nextPosToMove == -1){
                    //                        
                    //                        System.out.println("Got here");
                    //                        
                    //                        // if cannot expand anymore restart the process after skipping the first not moved vertex in the list
                    //                        nextPosToMove = lastHotVertexMoved + 1 + retryCount;
                    //                        retryCount++;
                    //
                    //                        movingVertices.clear();
                    //                    }
                    //                    else{

                    // 3) move the tuple

                    double receiverDelta = getReceiverDelta(movingVertices, from_part, toPartitionDelta.fst);
                    System.out.println("Receiver delta " + receiverDelta);

                    if(!movingVertices.isEmpty() 
                            && toPartitionDelta.fst != -1
                            && toPartitionDelta.snd <= Controller.MIN_GAIN_MOVE * -1
                            && receiverDelta < 0){

                        LOG.debug("Moving " + movingVertices);
                        m_graph.moveHotVertices(movingVertices, from_part, to_part);
                        //            LOG.debug("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));

                        numMovedVertices += movingVertices.size();  
                        //                            lastHotVertexMoved = nextPosToMove - 1;
                        movedVerticesSet.addAll(movingVertices);

                        movingVertices.clear();
                    }
                }

            } // END for (int other_part = 0; other_part < MAX_PARTITIONS; other_part++)
        } // END for (int part = 0; part < MAX_PARTITIONS; part++)

    }

    private boolean offloadHottestTuples(IntList overloadedPartitions, IntList activePartitions){

        int addedPartitions = 0;
        // offload each overloaded partition
        System.out.println("LOAD BALANCING");
        System.out.println("#######################");

        for(int overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("offloading site " + overloadedPartition);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, Controller.MAX_MOVED_TUPLES_PER_PART);

            int numMovedVertices = 0;
            IntOpenHashSet movingVertices = new IntOpenHashSet();

            int nextPosToMove = 0;
            int lastHotVertexMoved = -1;
            //            int retryCount = 1;

            int count_iter = 0;

            Pair<Integer, Double> toPartitionDelta = new Pair<Integer, Double> ();

            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                System.out.println("Iteration " + (count_iter++));
                //                System.out.println("Size of moving vertices: " + movingVertices.size());
                //                System.out.println("Size of this partition " + AffinityGraph.m_partitionVertices.get(overloadedPartition).size());

                // Step 1) first move cold tuples, then add partition and reset if I have over-expanded movingVertices, or if I cannot expand it anymore

                if (numMovedVertices + movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART 
                        || nextPosToMove >= hotVerticesList.size()
                        || (toPartitionDelta.fst != null && toPartitionDelta.fst == -1)){

                    System.out.println(numMovedVertices + movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART );
                    System.out.println(nextPosToMove >= hotVerticesList.size());
                    System.out.println(toPartitionDelta.fst != null && toPartitionDelta.fst == -1);

                    // MOVE COLD TUPLES

                    System.out.println("Move cold tuples");

                    numMovedVertices = moveColdChunks(overloadedPartition, activePartitions, numMovedVertices);

                    if (getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                        System.out.println("Cannot expand - Adding a new partition");

                        if(activePartitions.size() < Controller.MAX_PARTITIONS 
                                && addedPartitions < Controller.MAX_PARTITIONS_ADDED){

                            // We fill up low-order partitions first to minimize the number of servers
                            addedPartitions++;
                            for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                                if(!activePartitions.contains(i)){
                                    activePartitions.add(i);
                                    break;
                                }
                            }

                            nextPosToMove = lastHotVertexMoved + 1;
                            //                        retryCount = 1;
                            movingVertices.clear();
                            System.out.println(nextPosToMove);
                            System.out.println(hotVerticesList.size());
                        }
                        else{
                            System.out.println("Cannot add new partition to offload " + overloadedPartitions);
                            return false;
                        }
                    }
                }

                // Step 2) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple

                toPartitionDelta.fst = null;
                toPartitionDelta.snd = null;

                System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                System.out.println("Current sender delta " + getSenderDelta(movingVertices, overloadedPartition, 1));

                nextPosToMove = expandMovingVertices (movingVertices, toPartitionDelta, hotVerticesList, nextPosToMove, activePartitions, overloadedPartition);

                // Step 3) move the vertices

                double receiverDelta = getReceiverDelta(movingVertices, overloadedPartition, toPartitionDelta.fst);

                if(!movingVertices.isEmpty() 
                        && toPartitionDelta.fst != -1
                        && toPartitionDelta.snd <= Controller.MIN_GAIN_MOVE * -1
                        && (receiverDelta < 0 
                                || getLoadPerPartition(toPartitionDelta.fst) + receiverDelta < Controller.MAX_LOAD_PER_PART)){

                    System.out.println("Actually moving to " + toPartitionDelta.fst);

                    m_graph.moveHotVertices(movingVertices, overloadedPartition, toPartitionDelta.fst);
                    numMovedVertices += movingVertices.size();
                    lastHotVertexMoved = nextPosToMove - 1;
                    movingVertices.clear();
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
    private int expandMovingVertices (IntSet movingVertices, Pair<Integer,Double> toPartitionDelta, 
            IntList verticesToMove, int nextPosToMove, IntList activePartitions, int fromPartition){

        if (movingVertices.isEmpty()){

            // if empty, add a new hot vertex
            assert (nextPosToMove < verticesToMove.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 

            int nextVertexList = -1;

            do{
                nextVertexList = verticesToMove.get(nextPosToMove);
                nextPosToMove++;
            } while (nextPosToMove < verticesToMove.size() 
                    && AffinityGraph.m_vertexPartition.get(nextVertexList) != fromPartition);
            // the second condition is for the case where the vertex has been moved already 

            if (nextPosToMove == verticesToMove.size()){
                toPartitionDelta.fst = -1;
                toPartitionDelta.snd = Double.MAX_VALUE;
                return nextPosToMove;
            }

            movingVertices.add(nextVertexList);

            if(toPartitionDelta.fst == null){
                findBestPartition(movingVertices, fromPartition, activePartitions, toPartitionDelta);
            }

            if(nextVertexList == 0){
                throw new Error();
            }
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            // assess gain with extension
            Pair<Integer,Double> affineEdgeExtension = new Pair <Integer,Double>(-1, Double.MAX_VALUE);

            int affineVertex = getMostAffineExtension(movingVertices);

            if(affineVertex != 0){

                movingVertices.add(affineVertex);

                if(toPartitionDelta.fst == null){

                    findBestPartition(movingVertices, fromPartition, activePartitions, affineEdgeExtension);
                }
                else{
                    affineEdgeExtension.fst = toPartitionDelta.fst;
                    affineEdgeExtension.snd = getSenderDelta(movingVertices, fromPartition, toPartitionDelta.fst);
                }

                movingVertices.remove(affineVertex);
            }

            // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
            Pair<Integer,Double> nextVertexListExtension = new Pair <Integer,Double>(-1, Double.MAX_VALUE);

            int nextVertexList = 0;
            int skip = 0;

            if(nextPosToMove < verticesToMove.size()){

                // skip elements that are already in the set
                do{
                    nextVertexList = verticesToMove.get(nextPosToMove + skip);
                    skip ++;
                } while ((movingVertices.contains(nextVertexList)
                        || AffinityGraph.m_vertexPartition.get(nextVertexList) != fromPartition)
                        && nextPosToMove + skip < verticesToMove.size()); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here


                // if I have not reached the end of the list
                if (! movingVertices.contains(nextVertexList) && AffinityGraph.m_vertexPartition.get(nextVertexList) == fromPartition){
                    assert(nextVertexList != 0);

                    movingVertices.add(nextVertexList);

                    if(toPartitionDelta.fst == null){
                        findBestPartition(movingVertices, fromPartition, activePartitions, nextVertexListExtension);
                    }
                    else{
                        nextVertexListExtension.fst = toPartitionDelta.fst;
                        nextVertexListExtension.snd = getSenderDelta(movingVertices, fromPartition, toPartitionDelta.fst);
                    }

                    movingVertices.remove(nextVertexList);
                }
            }

            // pick best available choice
            if (affineEdgeExtension.snd < nextVertexListExtension.snd){

                movingVertices.add(affineVertex);
                toPartitionDelta.fst = affineEdgeExtension.fst;
                toPartitionDelta.snd = affineEdgeExtension.snd;

                LOG.debug("Adding edge extension: " + affineVertex);

            }
            else{

                movingVertices.add(nextVertexList);
                toPartitionDelta.fst = nextVertexListExtension.fst;
                toPartitionDelta.snd = nextVertexListExtension.snd;

                LOG.debug("Adding vertex from list: " + nextVertexList);

                nextPosToMove += skip;
                if(nextVertexList == 0){
                    throw new Error();
                }
            }

        } // END if(!movedVertices.isEmpty())

        return nextPosToMove;

    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    protected int getMostAffineExtension(IntSet vertices){

        double maxAffinity = -1;
        int res = 0;
        int partition = AffinityGraph.m_vertexPartition.get(vertices.iterator().next());

        for(int vertex : vertices){

            Int2DoubleMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for(Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                    if (edge.getDoubleValue() > maxAffinity
                            && AffinityGraph.m_vertexPartition.get(edge.getIntKey()) == partition
                            && !vertices.contains(edge.getIntKey())) {

                        maxAffinity = edge.getDoubleValue();
                        res = edge.getIntKey();
                    }
                }
            }
        }
        return res;
    }


    @Override
    protected double getLoadVertices(IntSet vertices){

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
            fromPartition = AffinityGraph.m_vertexPartition.get(vertex);
            int fromSite = PlanHandler.getSitePartition(fromPartition);

            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){

                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){

                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);

                    if (otherPartition != fromPartition){

                        int otherSite = PlanHandler.getSitePartition(otherPartition);
                        double h = (fromSite == otherSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                        load += edgeWeight * h;

                    }
                }
            }
        }


        long countColdTuples = 0;
        for(String table : m_graph.getTableNames()){

            List<Plan.Range> partitionRanges = m_graph.getAllRanges(table, fromPartition);

            if(partitionRanges.size() > 0) {

                for(Plan.Range r: partitionRanges) {
                    countColdTuples += r.to - r.from + 1;
                    countColdTuples -= m_graph.getHotVertexCount(fromPartition);
                }
            }
        }

        double coldIncrement = m_graph.getColdTupleWeight(fromPartition);
        load += countColdTuples * coldIncrement;

        return load;
    }

    @Override
    public double getLoadPerPartition(int partition){
        return getLoadVertices(AffinityGraph.m_partitionVertices.get(partition));
    }

    @Override
    protected double getGlobalDelta(IntSet movingVertices, int fromPartition, int toPartition){

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int fromSite = PlanHandler.getSitePartition(fromPartition);
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);

        double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;

        for(int vertex : movingVertices){ 

            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    if(!movingVertices.contains(otherVertex)){
                        int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);

                        if (otherPartition == fromPartition){
                            delta += edgeWeight * k;
                        }
                        else if (otherPartition == toPartition){
                            delta -= edgeWeight * k;
                        }
                        else{
                            int otherSite = PlanHandler.getSitePartition(otherPartition);
                            double h = 0;
                            if (otherSite == fromSite && otherSite != toSite){
                                h = Controller.DTXN_COST - Controller.LMPT_COST;
                            }
                            else if (otherSite != fromSite && otherSite == toSite){
                                h = Controller.LMPT_COST - Controller.DTXN_COST;
                            }
                            delta += edgeWeight * h;
                        }
                    }
                }
            }
        }

        return delta;
    }

    @Override
    protected double getReceiverDelta(IntSet movingVertices, int fromPartition, int toPartition){

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int fromSite = PlanHandler.getSitePartition(fromPartition);
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);

        double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;

        for(int vertex : movingVertices){ 

            double vertexWeight = AffinityGraph.m_vertices.get(vertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            delta += vertexWeight;

            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    if(!movingVertices.contains(otherVertex)){
                        int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);

                        if (otherPartition == toPartition){
                            delta -= edgeWeight * k;
                        }
                        else if (otherPartition == fromPartition) {
                            delta += edgeWeight * k;
                        }
                        else{
                            int otherSite = PlanHandler.getSitePartition(otherPartition);
                            double h = (toSite == otherSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                            delta += edgeWeight * h;
                        }
                    }
                }
            }
        }

        return delta;
    }

    @Override
    protected double getSenderDelta(IntSet movingVertices, int fromPartition, int toPartition) {

        if (movingVertices == null || movingVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }

        double delta = 0;
        int fromSite = PlanHandler.getSitePartition(fromPartition);
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);

        double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;

        for(int vertex : movingVertices){ 

            double vertexWeight = AffinityGraph.m_vertices.get(vertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            delta -= vertexWeight;

            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){

                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    if(!movingVertices.contains(otherVertex)){
                        int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);

                        if (otherPartition == toPartition){
                            delta -= edgeWeight * k;
                        }
                        else if (otherPartition == fromPartition) {
                            delta += edgeWeight * k;
                        }
                        else{
                            int otherSite = PlanHandler.getSitePartition(otherPartition);
                            double h = (fromSite == otherSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                            delta -= edgeWeight * h;
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
