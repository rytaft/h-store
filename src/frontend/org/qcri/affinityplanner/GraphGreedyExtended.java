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

    private static final Logger LOG = Logger.getLogger(GraphGreedy.class);
    
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

        LOG.debug("Moving border vertices");

//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

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
//                    if (movingVertices.contains(-1549464086)){
//                        System.out.println("in moving set");
//                        System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
//                        System.out.println("Is it in 1? " + AffinityGraph.m_partitionVertices.get(1).contains(-1549464086));
//                        System.out.println("Is it in 2? " + AffinityGraph.m_partitionVertices.get(2).contains(-1549464086));
//                        System.out.println("Is it in 3? " + AffinityGraph.m_partitionVertices.get(3).contains(-1549464086));
//                        System.out.println("Is it in 4? " + AffinityGraph.m_partitionVertices.get(4).contains(-1549464086));
//                        System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
//                        System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));
//                  }
                    
                        if(toPartitionDelta.snd <= Controller.MIN_GAIN_MOVE * -1){
                            
//                            if (movingVertices.contains(-1549464086)){
//                                System.out.println("moving it!");
//                                System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
//                                System.out.println("Is it in 1? " + AffinityGraph.m_partitionVertices.get(1).contains(-1549464086));
//                                System.out.println("Is it in 2? " + AffinityGraph.m_partitionVertices.get(2).contains(-1549464086));
//                                System.out.println("Is it in 3? " + AffinityGraph.m_partitionVertices.get(3).contains(-1549464086));
//                                System.out.println("Is it in 4? " + AffinityGraph.m_partitionVertices.get(4).contains(-1549464086));
//                                System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
//                                System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));
//                            }
                            
                            LOG.debug("Moving " + movingVertices);
                            m_graph.moveHotVertices(movingVertices, from_part, to_part);
                            //            LOG.debug("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));
    
                            numMovedVertices += movingVertices.size();  
//                            lastHotVertexMoved = nextPosToMove - 1;
                            movedVerticesSet.addAll(movingVertices);
    
//                            if (movingVertices.contains(-1549464086)){
//                                System.out.println("moved");
//                                System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
//                                System.out.println("Is it in 1? " + AffinityGraph.m_partitionVertices.get(1).contains(-1549464086));
//                                System.out.println("Is it in 2? " + AffinityGraph.m_partitionVertices.get(2).contains(-1549464086));
//                                System.out.println("Is it in 3? " + AffinityGraph.m_partitionVertices.get(3).contains(-1549464086));
//                                System.out.println("Is it in 4? " + AffinityGraph.m_partitionVertices.get(4).contains(-1549464086));
//                                System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
//                                System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));
//                            }
                            movingVertices.clear();
                        }
//                    }

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

    private boolean offloadHottestTuples(IntList overloadedPartitions, IntList activePartitions){
        
//        System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
//        System.out.println("Is it in 1? " + AffinityGraph.m_partitionVertices.get(1).contains(-1549464086));
//        System.out.println("Is it in 2? " + AffinityGraph.m_partitionVertices.get(2).contains(-1549464086));
//        System.out.println("Is it in 3? " + AffinityGraph.m_partitionVertices.get(3).contains(-1549464086));
//        System.out.println("Is it in 4? " + AffinityGraph.m_partitionVertices.get(4).contains(-1549464086));
//        System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
//        System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));

        int addedPartitions = 0;
        // offload each overloaded partition
        System.out.println("LOAD BALANCING");
        System.out.println("#######################");

        for(int overloadedPartition : overloadedPartitions){

            // DEBUG
            System.out.println("offloading site " + overloadedPartition);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, Controller.MAX_MOVED_TUPLES_PER_PART);
            
//            if(overloadedPartition == 0 && hotVerticesList.contains(-1549464086)){
//                System.out.println("It is a hot vertex");
//                System.out.println("Is it in 0? " + AffinityGraph.m_partitionVertices.get(0).contains(-1549464086));
//                System.out.println("Is it in 5? " + AffinityGraph.m_partitionVertices.get(5).contains(-1549464086));
//                System.out.println("Is it in? " + AffinityGraph.m_vertexPartition.get(-1549464086));
//                System.exit(1);
//            }
//            final int actualMaxMovedVertices = hotVerticesList.size();

            // DEBUG
            //            System.out.println("hot vertices:");
            //            for (String hotVertex : hotVerticesNotMoved){
            //                System.out.println(hotVertex);
            //            }

            int numMovedVertices = 0;
            IntOpenHashSet movingVertices = new IntOpenHashSet();

            int nextPosToMove = 0;
            int lastHotVertexMoved = -1;
//            int retryCount = 1;

            int count_iter = 0;

            Pair<Integer, Double> toPartitionDelta = new Pair<Integer, Double> ();
    
            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){
            
                // DEBUG
                //                LOG.debug("Press ENTER to continue");
                //                try {
                //                    br.readLine();
                //                } catch (IOException e) {
                //                    // TODO Auto-generated catch block
                //                    e.printStackTrace();
                //                }
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
                    
                    boolean success = moveColdChunks(overloadedPartition, activePartitions);
                    
                    if (!success){
                    
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

//                if(movingVertices.size() == 1925){
//                    System.out.println("Should start now");
//                    DEB = true;
//                }
                System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                System.out.println("Current sender delta " + getSenderDelta(movingVertices, overloadedPartition, 1));
                
                nextPosToMove = expandMovingVertices (movingVertices, toPartitionDelta, hotVerticesList, nextPosToMove, activePartitions, overloadedPartition);
                                
                // Step 3) move the vertices
//                System.out.println("Considering moving to " + toPartitionDelta.fst);
//
//                System.out.println("Sender delta " + toPartitionDelta.snd);
                
                double receiverDelta = getReceiverDelta(movingVertices, overloadedPartition, toPartitionDelta.fst);
//                System.out.println("Receiver delta " + receiverDelta);
                
//                System.out.println(!movingVertices.isEmpty());
//                System.out.println(toPartitionDelta.snd <= MIN_GAIN_MOVE * -1);
//                System.out.println(toPartitionDelta.fst != -1 && (receiverDelta < 0 
//                                || getLoadPerPartition(toPartitionDelta.fst) + receiverDelta < MAX_LOAD_PER_PART));
                
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

//            if(nextVertexList == -1549464086){
//                System.out.println("Found as border vertex! AAA");
//            }

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
            
//            if(affineVertex == -1549464086){
//                System.out.println("Found as affine edge!");
//            }
            
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
            
//            else{
//                System.out.println("No more affine edges");
//            }

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

//                    if(nextVertexList == -1549464086){
//                        System.out.println("Found as border vertex!");
//                    }
                    
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
//                else{
//                    System.out.println("No more hot tuples to add");
//                }
            }

            // pick best available choice
            if (affineEdgeExtension.snd < nextVertexListExtension.snd){

                movingVertices.add(affineVertex);
                toPartitionDelta.fst = affineEdgeExtension.fst;
                toPartitionDelta.snd = affineEdgeExtension.snd;

                LOG.debug("Adding edge extension: " + affineVertex);
                
//                if(affineVertex == -1549464086){
//                    System.out.println("It was an affine vertex");
//                }
            }
            else{

                movingVertices.add(nextVertexList);
                toPartitionDelta.fst = nextVertexListExtension.fst;
                toPartitionDelta.snd = nextVertexListExtension.snd;

                LOG.debug("Adding vertex from list: " + nextVertexList);

//                if(affineVertex == -1549464086){
//                    System.out.println("It was a hot vertex");
//                }

                nextPosToMove += skip;
                if(nextVertexList == 0){
                    throw new Error();
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
            
//            if (vertices.size() == 1925 && DEB){
//                DEBUG.add("Vertex " + vertex + " " + vertexWeight);
//            }
            
            // remote accesses
            fromPartition = AffinityGraph.m_vertexPartition.get(vertex);
            int fromSite = PlanHandler.getSitePartition(fromPartition);
            
            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){
                
//                if(DEB && vertex == -1549464086){
//                    System.out.println(adjacencyList);
//                }
                
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    
                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();
                    
                    int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);
                    
//                    if(DEB && vertex == -1549464086 && otherVertex == -794373915){
//                        System.out.println("AAA");
//                        System.out.println(fromPartition + " " + otherPartition);
//                    }
                    
                    if (otherPartition != fromPartition){
                        
//                        if(DEB && vertex == -1549464086 && otherVertex == -794373915){
//                            System.out.println("BBB");
//                        }

                        int otherSite = PlanHandler.getSitePartition(otherPartition);
                        double h = (fromSite == otherSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                        load += edgeWeight * h;
                        
//                        if (vertices.size() == 1925 && DEB){
//                            DEBUG.add("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * h);
//                        }
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

//        if (movingVertices.size() == 1925 && DEB){
//            System.out.println("Operands in senderDelta not Load");
//        }
        
        for(int vertex : movingVertices){ 
            
            double vertexWeight = AffinityGraph.m_vertices.get(vertex);
            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            delta -= vertexWeight;

//            if (movingVertices.size() == 1925 && DEB){
//                if(!DEBUG.contains("Vertex " + vertex + " " + vertexWeight)){
//                    System.out.println("Vertex " + vertex + " " + vertexWeight);
//                }
//                else{
//                    DEBUG.remove("Vertex " + vertex + " " + vertexWeight);
//                }
//            }

            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                    
                    int otherVertex = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();
                    
                    if(!movingVertices.contains(otherVertex)){
                        int otherPartition = AffinityGraph.m_vertexPartition.get(otherVertex);
                        
                        if (otherPartition == toPartition){
//                            if(movingVertices.size() == 1925 && DEB){
//                                if(!DEBUG.contains("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k)){
//                                    System.out.println("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                    System.out.println("Partitions " + fromPartition + " " + otherPartition);
//                                    System.out.println("Recomputing fromPartition " + AffinityGraph.m_vertexPartition.get(vertex));
//                                }
//                                else{
//                                    DEBUG.remove("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                }
//                            }
                            delta -= edgeWeight * k;
                        }
                        else if (otherPartition == fromPartition) {
//                            if(movingVertices.size() == 1925 && DEB){
//                                if(!DEBUG.contains("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k)){
//                                    System.out.println("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                    System.out.println("Partitions " + fromPartition + " " + otherPartition);
//                                    System.out.println("Recomputing fromPartition " + AffinityGraph.m_vertexPartition.get(vertex));
//                                }
//                                else{
//                                    DEBUG.remove("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                }
//                            }
                            delta += edgeWeight * k;
                        }
                        else{
                            int otherSite = PlanHandler.getSitePartition(otherPartition);
                            double h = (fromSite == otherSite) ? Controller.LMPT_COST : Controller.DTXN_COST;
                            delta -= edgeWeight * h;
//                            if(movingVertices.size() == 1925 && DEB){
//                                if(!DEBUG.contains("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k)){
//                                    System.out.println("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                    System.out.println("Partitions " + fromPartition + " " + otherPartition);
//                                    System.out.println("Recomputing fromPartition " + AffinityGraph.m_vertexPartition.get(vertex));
//                                }
//                                else{
//                                    DEBUG.remove("Edge (" + vertex + "," + otherVertex + ") " + edgeWeight * k);
//                                }
//                            }
                        }
                    }
                }
            }
        }
        
//        if (movingVertices.size() == 1925 && DEB){
//            System.out.println("Operands in Load not senderDelta");
//            for(String d : DEBUG){
//                System.out.println(d);
//            }
//        }
        
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

    // made a procedure so it is easier to stop when we are done
    private boolean moveColdChunks(int overloadedPartition, IntList activePartitions){

        // clone plan to allow modifications while iterating on the clone
        PlanHandler oldPlan = m_graph.clonePlan();

        System.out.println("Cloned plan\n" + oldPlan);

        double coldIncrement = m_graph.getColdTupleWeight(overloadedPartition);

        while (getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

            for(String table : m_graph.getTableNames()){

                System.out.println("Table " + table);

                List<List<Plan.Range>> partitionChunks = oldPlan.getRangeChunks(table, overloadedPartition,  (long) Controller.COLD_CHUNK_SIZE);
                if(partitionChunks.size() > 0) {

                    for(List<Plan.Range> chunk : partitionChunks) {  // a chunk can consist of multiple ranges if hot tuples are taken away

                        System.out.println("\nnew chunk");

                        for(Plan.Range r : chunk) { 

                            System.out.println("Range " + r.from + " " + r.to);

                            double rangeWeight = Plan.getRangeWidth(r) * coldIncrement;
                            int toPartition = getLeastLoadedPartition(activePartitions);     

                            System.out.println(rangeWeight);
                            System.out.println(toPartition);

                            if (rangeWeight + getLoadPerPartition(toPartition) < Controller.MAX_LOAD_PER_PART){

                                // do the move

                                System.out.println("Moving!");
                                System.out.println("Load before " + getLoadPerPartition(overloadedPartition));
                                
                                m_graph.moveColdRange(table, r, overloadedPartition, toPartition);

                                System.out.println("Load after " + getLoadPerPartition(overloadedPartition));
                                System.out.println("New plan\n" + m_graph.planToString());

                                // after every move, see if I can stop
                                if(getLoadPerPartition(overloadedPartition) <= Controller.MAX_LOAD_PER_PART){
                                    return true;
                                }
                            }
                            else{
                                System.out.println("Cannot offload partition " + overloadedPartition);
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
    
    public int getLeastLoadedPartition(IntList activePartitions){
        double minLoad = Double.MAX_VALUE;
        int res = 0;
        for (int part : activePartitions){
            double newLoad = getLoadPerPartition(part);
            if (newLoad < minLoad){
                res = part;
                minLoad = newLoad; 
            }
        }
        return res;
    }
}
