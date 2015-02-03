package org.qcri.affinityplanner;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Set<Integer> activePartitions = new HashSet<Integer>();
        Set<Integer> overloadedPartitions = new HashSet<Integer>();
        
        measureLoad(activePartitions, overloadedPartitions);

        // move border vertices        
        for (int fromPart = 0; fromPart < Controller.MAX_PARTITIONS; fromPart ++){
            
            List<List<String>> borderVertices = getBorderVertices(fromPart, MAX_MOVED_TUPLES_PER_PART);

            for(int toPart = 0; toPart < Controller.MAX_PARTITIONS; toPart ++){
                
                for (String vertex : borderVertices.get(toPart)){
                
                    if (fromPart != toPart){
                        tryMoveVertices(Collections.singleton(vertex), fromPart, toPart);
                    }
                }
            }
        }
        
        if (! overloadedPartitions.isEmpty()){
            // move hot vertices
            for(Integer fromPart : overloadedPartitions){

                List<String> hotVertices = getHottestVertices(fromPart, MAX_MOVED_TUPLES_PER_PART);

                for (String vertex : hotVertices){

                    for (int toPart = 0; toPart < Controller.MAX_PARTITIONS; toPart ++){

                        if (fromPart != toPart){
                            tryMoveVertices(Collections.singleton(vertex), fromPart, toPart);
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
    protected Double getLoadPerPartition(int fromPartition) {
        
        Set<String> vertices = m_graph.m_partitionVertices.get(fromPartition);
        double load = 0;
        for(String vertex : vertices){
            // local accesses
            load += m_graph.m_vertices.get(vertex);
            // remote accesses
            int fromPartitionSite = PlanHandler.getSitePartition(fromPartition);
            Map<String,Double> adjacencyList = m_graph.m_edges.get(vertex);
            if(adjacencyList != null){
                for(Map.Entry<String, Double> edge : adjacencyList.entrySet()){
                    String toPartition = edge.getKey();
                    int toPartitionInt = Integer.parseInt(toPartition);
                    int toPartitionSite = PlanHandler.getSitePartition(toPartitionInt);
                    if(toPartitionSite != fromPartitionSite){
                        load += edge.getValue() * DTXN_COST;
                    }
                    else if(toPartitionInt != fromPartition){
                        load += edge.getValue() * LMPT_COST;
                    }
                }
            }
        }
        return load;
    }

    @Override
    protected double getDeltaVertices(Set<String> movingVertices, int toPartition, boolean forSender) {
        assert(movingVertices.size() == 1);
        double delta = 0;
        for(String vertex : movingVertices){

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
    protected Double getLoadInCurrPartition(Set<String> vertices) {
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
                    String toPartition = edge.getKey();
                    int toPartitionInt = Integer.parseInt(toPartition);
                    int toVertexSite = PlanHandler.getSitePartition(toPartitionInt);
                    if(toVertexSite != fromVertexSite){
                        load += edge.getValue() * DTXN_COST;
                    }
                    else if(toPartitionInt != fromVertexPartition){
                        load += edge.getValue() * LMPT_COST;
                    }
                }
            }
        }
        return load;
    }
    
    @Override
    protected void updateAttractions (Map<String,Double> adjacency, double[] attractions){
        for (String toVertex : adjacency.keySet()){
            
            int other_partition = Integer.parseInt(toVertex);
            double edge_weight = adjacency.get(toVertex);
            attractions[other_partition] += edge_weight;
        } // END for (String toVertex : adjacency.keySet())
    }
}
