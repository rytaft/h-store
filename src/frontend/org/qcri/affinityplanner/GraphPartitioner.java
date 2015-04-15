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

        // detect overloaded and active partitions
        IntSet activePartitions = new IntOpenHashSet();

        System.out.println("Load per partition before reconfiguration");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(!AffinityGraph.m_partitionVertices.get(i).isEmpty()){
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
        
        IntSet overloadedPartitions = new IntOpenHashSet();
        
        System.out.println("Load per partition after moving border tuples");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(activePartitions.contains(i)){
                double load =  getLoadPerPartition(i);
                System.out.println(load);
                if (load > MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        if (! overloadedPartitions.isEmpty()){

            /*
             *  SCALE OUT
             */

            System.out.println("Move overloaded tuples");

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
                
                if (from_part == to_part){
                    continue;
                }

                LOG.debug("Moving FROM partition " + from_part + " TO partition " + to_part);

                // 1) get list of tuples that have the highest external pull to a single other partition

                IntList borderVertices = allBorderVertices.get(to_part);
                
                LOG.debug("Border vertices:");
                for(int vertex: borderVertices){
                    LOG.debug(vertex);
                }

                int numMovedVertices = 0;
                IntOpenHashSet movingVertices = new IntOpenHashSet();
                int nextPosToMove = 0;
                int lastHotVertexMoved = -1;
                int retryCount = 1;

                Pair<Integer,Double> toPartitionDelta = new Pair<Integer,Double> (to_part, 1.0);

                while(numMovedVertices + movingVertices.size() < MAX_MOVED_TUPLES_PER_PART 
                        && nextPosToMove < borderVertices.size()){

                    // 2) expand the tuple with the most affine tuples such that adding these tuples reduces the cost after movement
                    
                    nextPosToMove = expandMovingVertices (movingVertices, toPartitionDelta, borderVertices, nextPosToMove, activePartitions, from_part);

                    if (nextPosToMove == -1){
                        
                        System.out.println("Got here");
                        
                        // if cannot expand anymore restart the process after skipping the first not moved vertex in the list
                        nextPosToMove = lastHotVertexMoved + 1 + retryCount;
                        retryCount++;

                        movingVertices.clear();
                    }
                    else{

                        // 3) move the tuple

                        LOG.debug("Moving " + movingVertices);
                        m_graph.moveVertices(movingVertices, from_part, to_part);
                        //            LOG.debug("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));

                        numMovedVertices += movingVertices.size();  
                        lastHotVertexMoved = nextPosToMove - 1;
                        movedVerticesSet.addAll(movingVertices);

                        movingVertices.clear();
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
        System.out.println("LOAD BALANCING");
        System.out.println("#######################");

        for(int overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("offloading site " + overloadedPartition);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, MAX_MOVED_TUPLES_PER_PART);
//            final int actualMaxMovedVertices = hotVerticesList.size();

            // DEBUG
            //            System.out.println("hot vertices:");
            //            for (String hotVertex : hotVerticesNotMoved){
            //                System.out.println(hotVertex);
            //            }

            int numMovedVertices = 0;
            IntOpenHashSet movingVertices = new IntOpenHashSet();

            int posToMove = 0;
            int lastHotVertexMoved = -1;
//            int retryCount = 1;

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
                System.out.println("Iteration " + (count_iter++));

                // Step 1) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple
                Pair<Integer, Double> toPartitionDelta = new Pair<Integer, Double> ();
                int nextPosToMove = expandMovingVertices (movingVertices, toPartitionDelta, hotVerticesList, posToMove, activePartitions, overloadedPartition);

                // Step 2) add partition and reset if I have over-expanded movingVertices, or if I cannot expand it anymore

                if (numMovedVertices + movingVertices.size() > MAX_MOVED_TUPLES_PER_PART || nextPosToMove >= hotVerticesList.size() || posToMove >= hotVerticesList.size()){
                    System.out.println("Cannot expand - Adding a new partition");

                    if(activePartitions.size() < Controller.MAX_PARTITIONS && addedPartitions < MAX_PARTITIONS_ADDED){

                        // We fill up low-order partitions first to minimize the number of servers
                        addedPartitions++;
                        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                            if(!activePartitions.contains(i)){
                                activePartitions.add(i);
                                break;
                            }
                        }

                        posToMove = lastHotVertexMoved + 1;
//                        retryCount = 1;
                        movingVertices.clear();
                    }
                    else{
                        System.out.println("Partition " + overloadedPartitions + " cannot be offloaded.");
                        return false;
                    }
                }

                // if cannot expand, skip the first element in the hot vertex list and retry 
                if (nextPosToMove == -1){
                    posToMove ++;
//                    System.out.println("Cannot expand, skipping first hot element");
//                    nextPosToMove = lastHotVertexMoved + 1 + retryCount;
//                    movingVertices.clear();
//                    retryCount++;
                    continue;
                }
                else{
                    posToMove = nextPosToMove;
                }

                
                // if I must am migrate to a new partition, try to expand more

                if (toPartitionDelta.fst == -1){
                    continue;
                }

                
                // Step 3) move the vertices

                System.out.println("Moving to " + toPartitionDelta.fst);
                m_graph.moveVertices(movingVertices, overloadedPartition, toPartitionDelta.fst);
                numMovedVertices += movingVertices.size();
                lastHotVertexMoved = posToMove - 1;
                movingVertices.clear();
                
            } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
        }// END for(int overloadedPartition : overloadedPartitions)
        return true;
    }

    /**
     * updates the set movingVertices with one more vertex, either the most affine to the current movingVertices 
     * or the next vertex in the verticesToMove list, depending on which one is more convenient to move out.
     * 
     * Outputs:
     * - returns the next position to move in verticesToMove; -1 if there is no move possible
     * - modifies the argument movingVertices to add the new vertex
     * - modifies the argument toPartitionDelta to indicate where should the new vertex moved; partition -1 indicates that a new partition should be added
     * 
     */
    private int expandMovingVertices (IntSet movingVertices, Pair<Integer,Double> toPartitionDelta, IntList verticesToMove, int nextPosToMove, IntSet activePartitions, int fromPartition){

        System.out.println("movingVertices " + movingVertices);
        
        if (movingVertices.isEmpty()){
            
            // if empty, add a new hot vertex
            assert (nextPosToMove < verticesToMove.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 

            int nextVertexToMove = -1;

            do{
                nextVertexToMove = verticesToMove.get(nextPosToMove);
                nextPosToMove++;
            } while (nextPosToMove < verticesToMove.size() && AffinityGraph.m_vertexPartition.get(nextVertexToMove) != fromPartition);
            // the second condition is for the case where the vertex has been moved already 

            if (nextPosToMove == verticesToMove.size()){
                return -1;
            }

            movingVertices.add(nextVertexToMove);

            if(toPartitionDelta.fst == null){
                findBestPartition(movingVertices, fromPartition, activePartitions, toPartitionDelta);
            }
            
            if(nextVertexToMove == 0){
                throw new Error();
            }
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            // assess gain with extension
            Pair<Integer,Double> affineEdgeExtension = new Pair <Integer,Double>();
            
            int affineEdge = getMostAffineExtension(movingVertices);
            
            System.out.println("affineEdge " + affineEdge);
            
            if(affineEdge != 0){

                movingVertices.add(affineEdge);
                
                if(toPartitionDelta.fst == null){
                    findBestPartition(movingVertices, fromPartition, activePartitions, affineEdgeExtension);
                }
                else{
                    affineEdgeExtension.fst = toPartitionDelta.fst;
                    affineEdgeExtension.snd = getDeltaMove(movingVertices, fromPartition, toPartitionDelta.fst);
                }
                
                movingVertices.remove(affineEdge);
            }

            // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
            Pair<Integer,Double> nextVertexListExtension = new Pair <Integer,Double>();

            int nextVertexList = 0;
            int skip = 0;

            System.out.println("nextPosToMove " + nextPosToMove);
            System.out.println("verticesToMove.size() " + verticesToMove.size());
            
            if(nextPosToMove < verticesToMove.size()){
                
                do{
                    nextVertexList = verticesToMove.get(nextPosToMove + skip);
                    skip ++;
                } while (movingVertices.contains(nextVertexList) && nextPosToMove + skip < verticesToMove.size()); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here

                if (! movingVertices.contains(nextVertexList)){
                    assert(nextVertexList != 0);
                    
                    movingVertices.add(nextVertexList);

                    if(toPartitionDelta.fst == null){
                        findBestPartition(movingVertices, fromPartition, activePartitions, nextVertexListExtension);
                    }
                    else{
                        nextVertexListExtension.fst = toPartitionDelta.fst;
                        nextVertexListExtension.snd = getDeltaMove(movingVertices, fromPartition, toPartitionDelta.fst);
                    }

                    movingVertices.remove(nextVertexList);
                }
            }

            // pick best available choice
            if((affineEdgeExtension.snd == null || affineEdgeExtension.snd >= 0) 
                    && (nextVertexListExtension.snd == null || nextVertexListExtension.snd >= 0)){
                System.out.println("no choice");
                // no choice available
                return -1;
            }
            else{
                if (affineEdgeExtension.snd < nextVertexListExtension.snd){
                    
                    movingVertices.add(affineEdge);
                    toPartitionDelta.fst = affineEdgeExtension.fst;
                    
                    LOG.debug("Adding edge extension: " + affineEdge);
                }
                else{
                    
                    movingVertices.add(nextVertexList);
                    toPartitionDelta.fst = nextVertexListExtension.fst;
                    
                    LOG.debug("Adding vertex from list: " + nextVertexList);
                    
                    nextPosToMove += skip;
                    if(nextVertexList == 0){
                        throw new Error();
                    }
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
    protected double getLoadVertices(IntSet vertices){
        
        double load = 0;
        
        for(int vertex : vertices){
            
            // local accesses - vertex weight
            load += AffinityGraph.m_vertices.get(vertex);
            
            // remote accesses
            int fromVertexPartition = AffinityGraph.m_vertexPartition.get(vertex);
            int fromVertexSite = PlanHandler.getSitePartition(fromVertexPartition);
            
            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){
                
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    
                    int toVertex = edge.getIntKey();
                    int toVertexPartition = AffinityGraph.m_vertexPartition.get(toVertex);
                    
                    if (toVertexPartition != fromVertexPartition){
                        
                        int toVertexSite = PlanHandler.getSitePartition(toVertexPartition);
                        
                        if(toVertexSite != fromVertexSite){
                            load += edge.getDoubleValue() * DTXN_COST;
                        }
                        
                        else {
                            load += edge.getDoubleValue() * LMPT_COST;
                        }
                    }
                }
            }
        }
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
        
        double k = (fromSite == toSite) ? LMPT_COST : DTXN_COST;

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
                                h = DTXN_COST - LMPT_COST;
                            }
                            else if (otherSite != fromSite && otherSite == toSite){
                                h = LMPT_COST - DTXN_COST;
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
        
        double k = (fromSite == toSite) ? LMPT_COST : DTXN_COST;

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
                            double h = (toSite == otherSite) ? LMPT_COST : DTXN_COST;
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
        
        double k = (fromSite == toSite) ? LMPT_COST : DTXN_COST;

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
                            double h = (fromSite == otherSite) ? LMPT_COST : DTXN_COST;
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
