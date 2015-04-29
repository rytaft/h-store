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

public class SimplePartitioner extends PartitionerAffinity {

    private static final Logger LOG = Logger.getLogger(SimplePartitioner.class);

    public SimplePartitioner (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles){
        
        try{
            m_graph = new AffinityGraph(false, catalogContext, planFile, logFiles, intervalFiles, Controller.MAX_PARTITIONS);
        }
        catch (Exception e){
            Controller.record("Problem while loading graph. Exiting");
            return;
        }
    }
    
    @Override
    public boolean repartition() {

        int addedPartitions = 0;

        IntList activePartitions = new IntArrayList(Controller.MAX_PARTITIONS);

        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(AffinityGraph.isActive(i)){
                activePartitions.add(i);
            }
        }
        
        IntSet singleton = new IntOpenHashSet(1);

        // move border vertices        
        for (int fromPart = 0; fromPart < Controller.MAX_PARTITIONS; fromPart ++){
            
            List<IntList> borderVertices = getBorderVertices(fromPart, Controller.MAX_MOVED_TUPLES_PER_PART);

            for(int toPart = 0; toPart < Controller.MAX_PARTITIONS; toPart ++){
                
                for (int vertex : borderVertices.get(toPart)){
                
                    if (fromPart != toPart){
                        singleton.clear();
                        singleton.add(vertex);
                        tryMoveVertices(singleton, fromPart, toPart);
                    }
                }
            }
        }
        
        // offload overloaded partitions
        
        IntList overloadedPartitions = new IntArrayList(Controller.MAX_PARTITIONS);
        
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(activePartitions.contains(i)){
                System.out.println(getLoadPerPartition(i));
                if (getLoadPerPartition(i) > Controller.MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }
        
        if (! overloadedPartitions.isEmpty()){
            
            // move hot vertices
            for(int fromPart : overloadedPartitions){
                
                int numMovedVertices = 0;
                
                // loop over multiple added partitions
                while(getLoadPerPartition(fromPart) > Controller.MAX_LOAD_PER_PART){

                    IntList hotVertices = getHottestVertices(fromPart, Controller.MAX_MOVED_TUPLES_PER_PART);
    
                    for (int vertex : hotVertices){
    
                        for (int toPart = 0; toPart < Controller.MAX_PARTITIONS; toPart ++){
    
                            if (fromPart != toPart){
                                singleton.clear();
                                singleton.add(vertex);
                                numMovedVertices += tryMoveVertices(singleton, fromPart, toPart);
                            }                    
                        }
    
                    }
                    
                    if (getLoadPerPartition(fromPart) > Controller.MAX_LOAD_PER_PART){
                        numMovedVertices += moveColdChunks(fromPart, activePartitions, numMovedVertices);
                    }
                    
                    if (getLoadPerPartition(fromPart) > Controller.MAX_LOAD_PER_PART){
                        
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
                        }
                        else{
                            System.out.println("Cannot add new partition to offload " + overloadedPartitions);
                            return false;
                        }
        
                    }
                }
            }
        }
        else{
            // try to scale in
            scaleIn(activePartitions);
        }
        return true;
    }

    
    @Override
    public double getLoadPerPartition(int fromPartition) {
        
        IntSet vertices = AffinityGraph.m_partitionVertices.get(fromPartition);
        double load = 0;
        for(int vertex : vertices){
            // local accesses
            load += AffinityGraph.m_vertices.get(vertex);
            // remote accesses
            int fromPartitionSite = PlanHandler.getSitePartition(fromPartition);
            Int2DoubleMap adjacencyList = AffinityGraph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    int toPartition = edge.getIntKey();
                    int toPartitionSite = PlanHandler.getSitePartition(toPartition);
                    if(toPartitionSite != fromPartitionSite){
                        load += edge.getDoubleValue() * Controller.DTXN_COST;
                    }
                    else if(toPartition != fromPartition){
                        load += edge.getDoubleValue() * Controller.LMPT_COST;
                    }
                }
            }
        }
        return load;
    }
    
    
    @Override
    protected double getGlobalDelta(IntSet movingVertices, int fromPartition, int toPartition) {
        assert(movingVertices.size() == 1);

        double delta = 0;

        int fromSite = PlanHandler.getSitePartition(fromPartition);
        int toSite = (toPartition == -1) ? -1 : PlanHandler.getSitePartition(toPartition);
        
        double k = (fromSite == toSite) ? Controller.LMPT_COST : Controller.DTXN_COST;

        for(int vertex : movingVertices){
            
            Int2DoubleOpenHashMap adjacency = AffinityGraph.m_edges.get(vertex);
            if(adjacency != null){

                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                    int otherPartition = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();
                    
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
        
        return delta;
    }

    @Override
    protected double getReceiverDelta(IntSet movingVertices, int fromPartition, int toPartition) {
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
                    
                    int otherPartition = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();
                    
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
                    
                    int otherPartition = edge.getIntKey();
                    double edgeWeight = edge.getDoubleValue();
                    
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
        
        return delta;
    }    

//    protected double getDeltaVertices(IntSet movingVertices, int toPartition, boolean forSender) {
//        assert(movingVertices.size() == 1);
//        double delta = 0;
//        for(int vertex : movingVertices){
//
//            double vertexWeight = AffinityGraph.m_vertices.get(vertex);
//
//            int fromPartition = m_graph.getPartition(vertex);
//
//            double outPull= AffinityGraph.m_edges.get(vertex).get(Integer.toString(toPartition));
//            double inPull= AffinityGraph.m_edges.get(vertex).get(Integer.toString(fromPartition)); 
//
//            double outMultiplier;
//            if (toPartition == -1 || PlanHandler.getSitePartition(toPartition) != PlanHandler.getSitePartition(fromPartition)){
//                outMultiplier = DTXN_COST;
//            }
//            else{
//                outMultiplier = LMPT_COST;
//            }
//
//            if(forSender){
//                delta += vertexWeight;
//                delta -= outPull * outMultiplier;
//                delta += inPull * outMultiplier;
//            }
//            else{
//                delta -= vertexWeight;
//                delta -= outPull * outMultiplier;
//                delta += inPull * outMultiplier;
//            }
//        }
//        return delta;
//    }

    @Override
    protected double getLoadVertices(IntSet vertices) {
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
                    int toPartition = edge.getIntKey();
                    int toVertexSite = PlanHandler.getSitePartition(toPartition);
                    if(toVertexSite != fromVertexSite){
                        load += edge.getDoubleValue() * Controller.DTXN_COST;
                    }
                    else if(toPartition != fromVertexPartition){
                        load += edge.getDoubleValue() * Controller.LMPT_COST;
                    }
                }
            }
        }
        return load;
    }
    
    @Override
    protected void updateAttractions (Int2DoubleMap adjacency, double[] attractions){
        for (int toVertex : adjacency.keySet()){
            
            double edge_weight = adjacency.get(toVertex);
            attractions[toVertex] += edge_weight;
        } // END for (String toVertex : adjacency.keySet())
    }
}
