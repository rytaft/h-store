package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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

            int nextHotTuplePos = 0;
            int lastHotVertexMoved = -1;
            //            int retryCount = 1;

            int count_iter = 0;

            Move currMove = null;

            Move candidateMove = null;

            int greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;

            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                System.out.println("\nIteration " + (count_iter++));

                // Step 1) add partition and reset if I have over-expanded movingVertices, or if I cannot expand it anymore

                if (currMove != null && 
                        (nextHotTuplePos >= hotVerticesList.size() 
                        || numMovedVertices + currMove.movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART 
                        || currMove.toPartition == -1)){

                    if(candidateMove != null){
                        // before adding a partition, move current candidate if we have one
                        System.out.println("Was almost going to add a partition");
                        System.out.println("ACTUALLY moving to " + candidateMove.toPartition 
                                + "with sender delta " + candidateMove.sndDelta + " and receiver delta " + candidateMove.rcvDelta);
                        System.out.println("Moving:\n" + m_graph.verticesToString(candidateMove.movingVertices));

                        m_graph.moveHotVertices(candidateMove.movingVertices, overloadedPartition, candidateMove.toPartition);
                        numMovedVertices += candidateMove.movingVertices.size();
                        lastHotVertexMoved = nextHotTuplePos - 1;

                        currMove = null;
                        candidateMove = null;
                        greedyStepsAhead = Controller.GREEDY_STEPS_AHEAD;

                        continue;
                    }

                    if (currMove.movingVertices.size() == 0){
                        // it is not a matter of needing to expand more
                        System.out.println("Moving vertices are empty yet I cannot expand ");
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

                    nextHotTuplePos = lastHotVertexMoved + 1;
                    //                        retryCount = 1;
                    currMove.clear();
                } // END if (numMovedVertices + movingVertices.size() >= Controller.MAX_MOVED_TUPLES_PER_PART || nextPosToMove >= hotVerticesList.size() || (toPart_sndDelta_glbDelta.fst != null && toPart_sndDelta_glbDelta.fst == -1))

                // Step 2) add one vertex to movingVertices - either expand to vertex with highest affinity or with the next hot tuple

                System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                System.out.println("Current sender delta " + getSenderDelta(currMove.movingVertices, overloadedPartition, 1));

                nextHotTuplePos = expandMovingVertices (currMove, hotVerticesList, nextHotTuplePos, activePartitions, overloadedPartition);

                System.out.println("Moving:\n" + m_graph.verticesToString(currMove.movingVertices));

                // Step 3) move the vertices
                //                System.out.println("Considering moving to " + toPartitionDelta.fst);
                //
                //                System.out.println("Sender delta " + toPartitionDelta.snd);

                System.out.println("Receiver: " + currMove.toPartition + ", receiver delta " + currMove.rcvDelta);

                if(!currMove.movingVertices.isEmpty() 
                        && currMove.toPartition != -1
                        && currMove.sndDelta <= Controller.MIN_SENDER_GAIN_MOVE * -1
                        && (getLoadPerPartition(currMove.toPartition) + currMove.rcvDelta < Controller.MAX_LOAD_PER_PART)){

                    if(candidateMove == null || currMove.rcvDelta < candidateMove.rcvDelta){

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
                    System.out.println("ACTUALLY moving to " + candidateMove.toPartition + "with sender delta " 
                            + candidateMove.sndDelta + " and receiver delta " + candidateMove.rcvDelta);
                    System.out.println("Moving:\n" + m_graph.verticesToString(candidateMove.movingVertices));

                    m_graph.moveHotVertices(candidateMove.movingVertices, overloadedPartition, currMove.toPartition);
                    numMovedVertices += candidateMove.movingVertices.size();
                    lastHotVertexMoved = nextHotTuplePos - 1;

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
        
        if (move == null){
            move = new Move();
        }

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
                move.clear();
                return nextHotTuplePos;
            }

            move.movingVertices.add(nextHotVertex);

            findBestPartition(move, fromPartition, activePartitions);

            if(nextHotVertex == 0){
                throw new Error();
            }
        } // END if(movedVertices.isEmpty())

        else{

            // extend current moved set
            
            move.clearExceptMovingVertices();
            
            int affineVertex = getMostAffineExtension(move.movingVertices);

            if(affineVertex != 0){

                move.movingVertices.add(affineVertex);

                // this will populate all the fields of move
                findBestPartition(move, fromPartition, activePartitions);

                LOG.debug("Adding edge extension: " + affineVertex);
                System.out.println("It was an affine vertex");
            }

            else{
                System.out.println("Could not expand");
            }
            
        } // END if(!movedVertices.isEmpty())

        // DEBUG
        LOG.debug("Moving vertices: ");
        for (int vertex : move.movingVertices){
            LOG.debug(vertex);
        }       

        return nextHotTuplePos;

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
