package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

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
        Controller.record("Time taken:" + (t2-t1));


        // DEBUG
        m_graph.toFile(new File("Graph.txt").toPath());
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
        
        /*
         * MOVE BORDER TUPLES
         */

        System.out.println("Move border tuples");

        offloadBorderTuples(activePartitions);

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

                // 1) get list of tuples that have the highest external pull to a single other partition

                IntList borderVertices = allBorderVertices.get(to_part);

//                System.out.println("The number of border vertices from " + from_part + " to " +to_part + " is " + borderVertices.size());

                int numMovedVertices = 0;
                IntOpenHashSet movingVertices = new IntOpenHashSet();
                int nextPosToMove = 0;
                //                int lastHotVertexMoved = -1;
                //                int retryCount = 1;

                Triplet<Integer,Double,Double> toPart_sndDelta_glbDelta = new Triplet<Integer,Double,Double> (to_part, 1.0, 1.0);

                int count_iter = 0;

                while(numMovedVertices + movingVertices.size() < Controller.MAX_MOVED_TUPLES_PER_PART 
                        && nextPosToMove < borderVertices.size()
                        && nextPosToMove != -1){

                    System.out.println("Iteration " + (count_iter++));

                    // 2) expand the tuple with the most affine tuples such that adding these tuples reduces the cost after movement

                    nextPosToMove = expandMovingVertices (movingVertices, toPart_sndDelta_glbDelta, borderVertices, nextPosToMove, activePartitions, from_part);

                    System.out.println("Moving:\n" + m_graph.verticesToString(movingVertices));

                    double receiverDelta = getReceiverDelta(movingVertices, from_part, toPart_sndDelta_glbDelta.fst);
                    System.out.println("Receiver delta " + receiverDelta);

                    if(!movingVertices.isEmpty() 
                            && toPart_sndDelta_glbDelta.fst != -1
                            && toPart_sndDelta_glbDelta.snd <= Controller.MIN_GAIN_MOVE * -1
                            && receiverDelta < 0){

                        System.out.println("!!!!!! Actually moving to " + toPart_sndDelta_glbDelta.fst);

                        m_graph.moveHotVertices(movingVertices, from_part, toPart_sndDelta_glbDelta.fst);
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
            System.out.println("Active partitions " + activePartitions.toString());

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            IntList hotVerticesList = getHottestVertices(overloadedPartition, Controller.MAX_MOVED_TUPLES_PER_PART);

            int numMovedVertices = 0;
            IntOpenHashSet movingVertices = new IntOpenHashSet();

            int nextPosToMove = 0;
            int lastHotVertexMoved = -1;
            //            int retryCount = 1;

            int count_iter = 0;

            Triplet<Integer, Double, Double> toPart_sndDelta_glbDelta = new Triplet<Integer, Double, Double> ();
            
            IntSet candidateMovingVertices = null;
            Triplet<Integer, Double, Double> candidateMove = null;
            
            int greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;

            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                System.out.println("\nIteration " + (count_iter++));

                // Step 1) add partition and reset if I have over-expanded movingVertices, or if I cannot expand it anymore

                if (numMovedVertices + movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART 
                        || nextPosToMove >= hotVerticesList.size()
                        || (toPart_sndDelta_glbDelta.fst != null && toPart_sndDelta_glbDelta.fst == -1)){
                    
                    if(candidateMovingVertices != null){
                        // before adding a partition, move current candidate if we have one
                        System.out.println("ACTUALLY moving to " + toPart_sndDelta_glbDelta.fst);

                        m_graph.moveHotVertices(candidateMovingVertices, overloadedPartition, candidateMove.fst);
                        numMovedVertices += candidateMovingVertices.size();
                        lastHotVertexMoved = nextPosToMove - 1;
                        movingVertices.clear();
                        
                        candidateMovingVertices = null;
                        candidateMove = null;
                        
                        continue;
                    }
                    
                    if (movingVertices.size() == 0){
                        return false;
                    }

                    System.out.println("Cannot expand - Adding a new partition");

                    if(activePartitions.size() >= Controller.MAX_PARTITIONS 
                            || addedPartitions >= Controller.MAX_PARTITIONS_ADDED){
                        System.out.println("Cannot add new partition to offload " + overloadedPartitions);
                        return false;
                    }

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

                // Step 2) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple

                toPart_sndDelta_glbDelta.fst = null;
                toPart_sndDelta_glbDelta.snd = null;
                toPart_sndDelta_glbDelta.trd = null;

                System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                System.out.println("Current sender delta " + getSenderDelta(movingVertices, overloadedPartition, 1));

                nextPosToMove = expandMovingVertices (movingVertices, toPart_sndDelta_glbDelta, 
                        hotVerticesList, nextPosToMove, activePartitions, overloadedPartition);

                System.out.println("Moving:\n" + m_graph.verticesToString(movingVertices));

                // Step 3) move the vertices
                //                System.out.println("Considering moving to " + toPartitionDelta.fst);
                //
                //                System.out.println("Sender delta " + toPartitionDelta.snd);

                double receiverDelta = getReceiverDelta(movingVertices, overloadedPartition, toPart_sndDelta_glbDelta.fst);
                System.out.println("Receiver: " + toPart_sndDelta_glbDelta.fst + ", received delta " + receiverDelta + ", global delta " + toPart_sndDelta_glbDelta.trd);

                if(!movingVertices.isEmpty() 
                        && toPart_sndDelta_glbDelta.fst != -1
                        && toPart_sndDelta_glbDelta.snd <= Controller.MIN_GAIN_MOVE * -1
                        && (receiverDelta < 0 
                                || getLoadPerPartition(toPart_sndDelta_glbDelta.fst) + receiverDelta < Controller.MAX_LOAD_PER_PART)){
                    
                    if(candidateMovingVertices == null || toPart_sndDelta_glbDelta.trd < candidateMove.trd){

                        System.out.println("CANDIDATE for moving to " + toPart_sndDelta_glbDelta.fst);
                        
                        // record this move as a candidate
                        candidateMovingVertices = movingVertices.clone();
                        candidateMove = toPart_sndDelta_glbDelta.clone();
                        greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;
                    }
                    else{
                        // current candidate is better  
                        greedyStepsAhead--;
                    }

                }
                else{
                    // this not a candidate
                    greedyStepsAhead--;
                }
                
                // move after making enough steps
                if (greedyStepsAhead == 0){
                    System.out.println("ACTUALLY moving to " + toPart_sndDelta_glbDelta.fst);

                    m_graph.moveHotVertices(movingVertices, overloadedPartition, toPart_sndDelta_glbDelta.fst);
                    numMovedVertices += movingVertices.size();
                    lastHotVertexMoved = nextPosToMove - 1;
                    movingVertices.clear();
                    
                    candidateMovingVertices = null;
                    candidateMove = null;
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
    private int expandMovingVertices (IntSet movingVertices, Triplet<Integer,Double,Double> toPart_sndDelta_glbDelta, 
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
                toPart_sndDelta_glbDelta.fst = -1;
                toPart_sndDelta_glbDelta.snd = Double.MAX_VALUE;
                toPart_sndDelta_glbDelta.trd = Double.MAX_VALUE;
                return nextPosToMove;
            }

            movingVertices.add(nextVertexList);

            if(toPart_sndDelta_glbDelta.fst == null){
                findBestPartition(movingVertices, fromPartition, activePartitions, toPart_sndDelta_glbDelta);
            }

            if(nextVertexList == 0){
                throw new Error();
            }
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set

            // assess gain with extension
            Triplet<Integer,Double,Double> affineEdgeExtension = new Triplet <Integer,Double,Double>(-1, Double.MAX_VALUE, Double.MAX_VALUE);

            int affineVertex = getMostAffineExtension(movingVertices);

            if(affineVertex != 0){

                movingVertices.add(affineVertex);

                if(toPart_sndDelta_glbDelta.fst == null){

                    findBestPartition(movingVertices, fromPartition, activePartitions, affineEdgeExtension);
                }
                else{
                    affineEdgeExtension.fst = toPart_sndDelta_glbDelta.fst;
                    affineEdgeExtension.snd = getSenderDelta(movingVertices, fromPartition, toPart_sndDelta_glbDelta.fst);
                    affineEdgeExtension.trd = getGlobalDelta(movingVertices, fromPartition, toPart_sndDelta_glbDelta.fst);
                }

                movingVertices.remove(affineVertex);
            }

            //            else{
                //                System.out.println("No more affine edges");
            //            }

            // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
            Triplet<Integer,Double,Double> nextVertexListExtension = new Triplet <Integer,Double,Double>(-1, Double.MAX_VALUE, Double.MAX_VALUE);

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

                    if(toPart_sndDelta_glbDelta.fst == null){
                        findBestPartition(movingVertices, fromPartition, activePartitions, nextVertexListExtension);
                    }
                    else{
                        nextVertexListExtension.fst = toPart_sndDelta_glbDelta.fst;
                        nextVertexListExtension.snd = getSenderDelta(movingVertices, fromPartition, toPart_sndDelta_glbDelta.fst);
                        nextVertexListExtension.trd = getGlobalDelta(movingVertices, fromPartition, toPart_sndDelta_glbDelta.fst);
                    }

                    movingVertices.remove(nextVertexList);
                }
            }

            // pick best available choice
            if (affineEdgeExtension.snd < nextVertexListExtension.snd){

                movingVertices.add(affineVertex);
                toPart_sndDelta_glbDelta.fst = affineEdgeExtension.fst;
                toPart_sndDelta_glbDelta.snd = affineEdgeExtension.snd;
                toPart_sndDelta_glbDelta.trd = affineEdgeExtension.trd;

                LOG.debug("Adding edge extension: " + affineVertex);

                System.out.println("It was an affine vertex");
                
            }
            else{

                movingVertices.add(nextVertexList);
                toPart_sndDelta_glbDelta.fst = nextVertexListExtension.fst;
                toPart_sndDelta_glbDelta.snd = nextVertexListExtension.snd;
                toPart_sndDelta_glbDelta.trd = nextVertexListExtension.trd;

                LOG.debug("Adding vertex from list: " + nextVertexList);

                System.out.println("It was a hot vertex");

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

        for(int vertex : vertices){

            // local accesses - vertex weight
            double vertexWeight = AffinityGraph.m_vertices.get(vertex);

            if (vertexWeight == AffinityGraph.m_vertices.defaultReturnValue()){
                LOG.debug("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }

            load += vertexWeight;

            // remote accesses
            int fromPartition = AffinityGraph.m_vertexPartition.get(vertex);
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
