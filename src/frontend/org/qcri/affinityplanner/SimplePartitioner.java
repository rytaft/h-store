package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import org.voltdb.CatalogContext;

public class SimplePartitioner extends Partitioner {

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

        // detect overloaded and active partitions
        IntSet activePartitions = new IntOpenHashSet();
        IntSet overloadedPartitions = new IntOpenHashSet();
        
        scanPartitions(activePartitions, overloadedPartitions);
        
        IntSet singleton = new IntOpenHashSet(1);

        // move border vertices        
        for (int fromPart = 0; fromPart < Controller.MAX_PARTITIONS; fromPart ++){
            
            List<IntList> borderVertices = getBorderVertices(fromPart, MAX_MOVED_TUPLES_PER_PART);

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
        
        if (! overloadedPartitions.isEmpty()){
            // move hot vertices
            for(int fromPart : overloadedPartitions){

                IntList hotVertices = getHottestVertices(fromPart, MAX_MOVED_TUPLES_PER_PART);

                for (int vertex : hotVertices){

                    for (int toPart = 0; toPart < Controller.MAX_PARTITIONS; toPart ++){

                        if (fromPart != toPart){
                            singleton.clear();
                            singleton.add(vertex);
                            tryMoveVertices(singleton, fromPart, toPart);
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
    protected double getLoadPerPartition(int fromPartition) {
        
        IntSet vertices = m_graph.m_partitionVertices.get(fromPartition);
        double load = 0;
        for(int vertex : vertices){
            // local accesses
            load += m_graph.m_vertices.get(vertex);
            // remote accesses
            int fromPartitionSite = PlanHandler.getSitePartition(fromPartition);
            Int2DoubleMap adjacencyList = m_graph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    int toPartition = edge.getIntKey();
                    int toPartitionSite = PlanHandler.getSitePartition(toPartition);
                    if(toPartitionSite != fromPartitionSite){
                        load += edge.getDoubleValue() * DTXN_COST;
                    }
                    else if(toPartition != fromPartition){
                        load += edge.getDoubleValue() * LMPT_COST;
                    }
                }
            }
        }
        return load;
    }

    @Override
    protected double getDeltaVertices(IntSet movingVertices, int toPartition, boolean forSender) {
        assert(movingVertices.size() == 1);
        double delta = 0;
        for(int vertex : movingVertices){

            double vertexWeight = m_graph.m_vertices.get(vertex);

            int fromPartition = m_graph.getPartition(vertex);

            double outPull= m_graph.m_edges.get(vertex).get(Integer.toString(toPartition));
            double inPull= m_graph.m_edges.get(vertex).get(Integer.toString(fromPartition)); 

            double outMultiplier;
            if (toPartition == -1 || PlanHandler.getSitePartition(toPartition) != PlanHandler.getSitePartition(fromPartition)){
                outMultiplier = DTXN_COST;
            }
            else{
                outMultiplier = LMPT_COST;
            }

            if(forSender){
                delta += vertexWeight;
                delta -= outPull * outMultiplier;
                delta += inPull * outMultiplier;
            }
            else{
                delta -= vertexWeight;
                delta -= outPull * outMultiplier;
                delta += inPull * outMultiplier;
            }
        }
        return delta;
    }

    @Override
    protected double getLoadInCurrPartition(IntSet vertices) {
        double load = 0;
        for(int vertex : vertices){
            // local accesses
            load += m_graph.m_vertices.get(vertex);
            // remote accesses
            int fromVertexPartition = m_graph.m_vertexPartition.get(vertex);
            int fromVertexSite = PlanHandler.getSitePartition(fromVertexPartition);
            Int2DoubleMap adjacencyList = m_graph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Int2DoubleMap.Entry edge : adjacencyList.int2DoubleEntrySet()){
                    int toPartition = edge.getIntKey();
                    int toVertexSite = PlanHandler.getSitePartition(toPartition);
                    if(toVertexSite != fromVertexSite){
                        load += edge.getDoubleValue() * DTXN_COST;
                    }
                    else if(toPartition != fromVertexPartition){
                        load += edge.getDoubleValue() * LMPT_COST;
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
