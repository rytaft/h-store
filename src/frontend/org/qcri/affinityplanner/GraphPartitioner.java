package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

public class GraphPartitioner extends Partitioner {

    private static final Logger LOG = Logger.getLogger(GraphPartitioner.class);

    public GraphPartitioner (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles){
                
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
        if (Controller.PARTITIONS_PER_SITE == -1 || Controller.MAX_PARTITIONS == -1){
            System.out.println("GraphPartitioner: Must initialize PART_PER_SITE and MAX_PARTITIONS");
            return false;
        }

        System.out.println("Loads per partition before reconfiguration");

        // detect overloaded and active partitions
        IntSet activePartitions = new IntOpenHashSet();
        IntSet overloadedPartitions = new IntOpenHashSet();
        scanPartitions(activePartitions, overloadedPartitions);

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

            scaleIn(activePartitions);
        }
        return true;
    }

    private void offloadBorderTuples(IntSet activePartitions){

        LOG.debug("Moving border vertices");

//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        for (int from_part : activePartitions){
            
            List<IntList> allBorderVertices = getBorderVertices(from_part, MAX_MOVED_TUPLES_PER_PART);

            // vertices that will have to be removed from other lists
            IntSet movedVerticesSet = new IntOpenHashSet ();

            for (int to_part : activePartitions){

                LOG.debug("Moving FROM partition " + from_part + " TO partition " + to_part);

                // 1) get list of tuples that have the highest external pull to a single other partition

                IntList borderVertices = allBorderVertices.get(to_part);
                
                LOG.debug("Border vertices:");
                for(int vertex: borderVertices){
                    LOG.debug(vertex);
                }

                final int actualMaxMovedVertices = borderVertices.size();

                int numMovedVertices = 0;
                IntOpenHashSet movingVertices = new IntOpenHashSet();
                int nextPosToMove = 0;
                int lastHotVertexMoved = -1;
                int retryCount = 1;

                while(numMovedVertices + movingVertices.size() < actualMaxMovedVertices && nextPosToMove < borderVertices.size()){

                    // 2) expand the tuple with the most affine tuples such that adding these tuples reduces the cost after movement

                    nextPosToMove = expandMovingVertices (movingVertices, borderVertices, nextPosToMove, from_part);

                    if (nextPosToMove == -1){
                        // if cannot expand anymore restart the process after skipping the first not moved vertex in the list
                        nextPosToMove = lastHotVertexMoved + 1 + retryCount;
                        movingVertices.clear();
                        retryCount++;
                    }
                    else{

                        // 3) assess if we can move the tuple

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

    }

    private boolean offloadHottestTuples(IntSet overloadedPartitions, IntSet activePartitions){
        int addedPartitions = 0;
        // offload each overloaded partition
        System.out.println("SCALING OUT");
        System.out.println("#######################");

        for(int overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("offloading site " + overloadedPartition);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, MAX_MOVED_TUPLES_PER_PART);
            final int actualMaxMovedVertices = hotVerticesList.size();

            // DEBUG
            //            System.out.println("hot vertices:");
            //            for (String hotVertex : hotVerticesNotMoved){
            //                System.out.println(hotVertex);
            //            }

            int numMovedVertices = 0;
            IntOpenHashSet movingVertices = new IntOpenHashSet();

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
                IntList localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(overloadedPartition));

                for(int toPartition : localPartitions){

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

                    for(int toPartition : activePartitions){

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
        }// END for(int overloadedPartition : overloadedPartitions)
        return true;
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
    private int expandMovingVertices (IntSet movingVertices, IntList verticesToMove, int nextPosToMove, int partition){

        if (movingVertices.isEmpty()){
            // if empty, add a new hot vertex
            assert (nextPosToMove < verticesToMove.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 
            int nextVertexToMove = -1;
            do{
                nextVertexToMove = verticesToMove.get(nextPosToMove);
                nextPosToMove++;
            } while (nextPosToMove < verticesToMove.size() && AffinityGraph.m_vertexPartition.get(nextVertexToMove) != partition);
            // the second condition is for the case where the vertex has been moved already 
//            assert (nextHotVertex != 0); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
            if (nextPosToMove == verticesToMove.size()){
                return -1;
            }
            LOG.debug("Adding hot vertex " + nextVertexToMove);
            movingVertices.add(nextVertexToMove);
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            // assess gain with extension
            double deltaEdgeExtension = Double.MIN_VALUE;                        

            int nextEdgeExtension = getMostAffineExtension(movingVertices);
            if(nextEdgeExtension != 0){
                movingVertices.add(nextEdgeExtension);
                deltaEdgeExtension = getDeltaVertices(movingVertices, -1, true);
                movingVertices.remove(nextEdgeExtension);
            }

            // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
            double deltaHotTuple = Double.MIN_VALUE;

            int nextVertexToMove = 0;
            int skip = 0;

            if(nextPosToMove < verticesToMove.size()){

                do{
                    nextVertexToMove = verticesToMove.get(nextPosToMove + skip);
                    skip ++;
                } while (movingVertices.contains(nextVertexToMove) && nextPosToMove + skip < verticesToMove.size()); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here

                if (! movingVertices.contains(nextVertexToMove)){
                    assert(nextVertexToMove != 0);
                    movingVertices.add(nextVertexToMove);
                    deltaHotTuple = getDeltaVertices(movingVertices, -1, true);
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
        for (int vertex : movingVertices){
            LOG.debug(vertex);
        }       
        return nextPosToMove;

    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    private int getMostAffineExtension(IntSet vertices){

        double maxAdjacency = -1;
        int res = 0;
        int partition = AffinityGraph.m_vertexPartition.get(vertices.iterator().next());
 
        for(int vertex : vertices){
            Int2DoubleMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for(Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                    if (edge.getDoubleValue() > maxAdjacency
                            && AffinityGraph.m_vertexPartition.get(edge.getIntKey()) == partition
                            && !vertices.contains(edge.getIntKey())) {
                        maxAdjacency = edge.getDoubleValue();
                        res = edge.getIntKey();
                    }
                }
            }
        }
        return res;
    }


    @Override
    protected double getLoadInCurrPartition(IntSet vertices){
        double load = 0;
        for(int vertex : vertices){
            // local accesses
            load += AffinityGraph.m_vertices.get(vertex);
            // remote accesses
            int fromVertexPartition = AffinityGraph.m_vertexPartition.get(vertex);
            int fromVertexSite = PlanHandler.getSitePartition(fromVertexPartition);
            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    int toVertex = edge.getIntKey();
                    int toVertexPartition = AffinityGraph.m_vertexPartition.get(toVertex);
                    int toVertexSite = PlanHandler.getSitePartition(toVertexPartition);
                    if(toVertexSite != fromVertexSite){
                        load += edge.getDoubleValue() * DTXN_COST;
                    }
                    else if(toVertexPartition != fromVertexPartition){
                        load += edge.getDoubleValue() * LMPT_COST;
                    }
                }
            }
        }
        return load;
    }

    @Override
    public double getLoadPerPartition(int partition){
        return getLoadInCurrPartition(AffinityGraph.m_partitionVertices.get(partition));
    }

    @Override
    protected double getDeltaVertices(IntSet movedVertices, int newPartition, boolean forSender) {

        if (movedVertices == null || movedVertices.isEmpty()){
            LOG.debug("Trying to move an empty set of vertices");
            return 0;
        }
        double delta = 0;
        int fromPartition = AffinityGraph.m_vertexPartition.get(movedVertices.iterator().next());

        for(int vertex : movedVertices){ 
            //            LOG.debug("REMOVE delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            double vertexWeight = AffinityGraph.m_vertices.get(vertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){
                double outPull = 0;
                double inPull = 0;
                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                    //                LOG.debug("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                    int toVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();

                    // edges to vertices that are moved together do not contribute to in- or out-pull
                    if(!movedVertices.contains(toVertex)){
                        int toPartition = AffinityGraph.m_vertexPartition.get(toVertex); 
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
                if(forSender){
                    delta -= vertexWeight;
                    delta -= outPull * outMultiplier;
                    delta += inPull * outMultiplier;
                }
                else{                    
                    delta += vertexWeight;
                    delta -= outPull * outMultiplier;
                    delta += inPull * outMultiplier;
                }
                //          LOG.debug(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, vertexWeight + (outPull - inPull) * Controller.DTXN_MULTIPLIER, delta));
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
