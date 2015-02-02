package org.qcri.affinityplanner;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
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

        for (int fromPart = 0; fromPart < Controller.MAX_PARTITIONS; fromPart ++){

            Set<String> vertices = m_graph.m_partitionVertices.get(fromPart);

            for (String vertex : vertices){

                Map<String, Double> adjacency = m_graph.m_edges.get(vertex);
                double inPull = adjacency.get(Integer.toString(fromPart));

                for(String toPart : adjacency.keySet()){

                    int toPartInt = Integer.parseInt(toPart);

                    if (fromPart != toPartInt){
                        double outPull = adjacency.get(toPart);
                        if (inPull < outPull){
                            m_graph.moveVertices(Collections.singleton(vertex), fromPart, toPartInt);
                        }
                    }
                }
            }
        }
        
        return true;
    }

    @Override
    public Double getLoadPerPartition(int partition) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Double getLoadPerSite(int site) {
        // TODO Auto-generated method stub
        return null;
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
}
