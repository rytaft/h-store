package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.CatalogContext;

import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.TwoTieredRangePartitions;
import edu.brown.utils.FileUtil;

public class AffinityGraph {
    private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);
    
    // the first key is the fromVertex, the internal map is the adjacency list of <toVertex, edgeWeight>
    private Map<String,Map<String,Integer>> m_edges = new HashMap<String,Map<String,Integer>> ();
    // weights of vertices
    private Map<String,Integer> m_vertices = new HashMap<String,Integer> ();
    // location of vertices
    private List<Set<String>> m_siteVertices = new ArrayList<Set<String>> ();
    private Map<String,Integer> m_vertexSite = new HashMap<String,Integer> ();
    
    public void loadFromFiles (CatalogContext catalogContext, File planFile, int partitions, Path[] logFiles) throws Exception{
        BufferedReader reader;
        int sites = partitions / Controller.PARTITIONS_PER_SITE;
        if (partitions % Controller.PARTITIONS_PER_SITE != 0){
            sites++;
        }
        System.out.println("Sites = " + sites);
        for (int i = 0; i <= sites; i++){
            m_siteVertices.add(new HashSet<String>());
        }
        
        // get mapping of keys to partitions
        JSONObject json;
        try {
            json = new JSONObject(FileUtil.readFile(planFile.getAbsolutePath()));
        } catch (JSONException e1) {
            e1.printStackTrace();
            System.out.println("Problem while reading JSON file with the plan");
            throw e1;
        }
        ExplicitPartitions p;
        try {
            p = new TwoTieredRangePartitions(catalogContext, json);
        } catch (Exception e1) {
            e1.printStackTrace();
            System.out.println("Problem while creating the partitioner");
            throw e1;
        }
        p.setPartitionPlan(planFile);
        
        // scan files for all partitions
        for (Path logFile : logFiles){
            try {
                reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Error while opening file " + logFile.toString());
                throw e;
            }
            String line;
            // tuples with number of SQL statements they are involved in
            Map<String,Integer> transaction = new HashMap<String,Integer>();
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Error while reading file " + logFile.toString());
                throw e;
            }
            if (line == null){
                LOG.warn("File " + logFile.toString() + " is empty");
            }
            String currTransactionId = (line.split(";"))[0];
            System.out.println("Tran ID = " + currTransactionId);
            while(line != null){
                System.out.println("Reading next line");
                String[] tuple = line.split(";");
                
                // if finished with one transaction, update graph and clear
                if (!tuple[0].equals(currTransactionId)){
                    System.out.println("Size of transaction:" + transaction.size());
                    for(Map.Entry<String,Integer> from : transaction.entrySet()){
                        // update edges first
//                        System.out.println("from: " + from.getKey());
                        Set<String> visitedVertices = new HashSet<String>();
                        for(Map.Entry<String, Integer> to : transaction.entrySet()){
//                            System.out.println("to: " + to.getKey());
                            if (! from.getKey().equals(to.getKey()) && ! visitedVertices.contains(to.getKey())){
                                visitedVertices.add(to.getKey());
                                Map<String,Integer> adjacency = m_edges.get(from.getKey());
                                if(adjacency == null){
                                    adjacency = new HashMap<String,Integer>();
                                    m_edges.put(from.getKey(), adjacency);
                                }
                                Integer currentEdgeWeight = adjacency.get(to.getKey());
                                if (currentEdgeWeight == null){
    //                              adjacency.put(to.getKey(), to.getValue());
                                    adjacency.put(to.getKey(), 1);
                                }
                                else{
    //                              adjacency.put(to.getKey(), currentEdgeWeight + to.getValue());
                                    adjacency.put(to.getKey(), currentEdgeWeight + 1);
                                }
                            }
                        }
                        // update vertices
                        Integer currentVertexWeight = m_vertices.get(from.getKey());
                        if (currentVertexWeight == null){
//                            vertices.put(from.getKey(), from.getValue());
                            m_vertices.put(from.getKey(), 1);
                        }
                        else{
//                            vertices.put(from.getKey(), currentVertexWeight+from.getValue());                            
                            m_vertices.put(from.getKey(), currentVertexWeight+1);                            
                        }
                        // update locations
                        String[] tupleData = from.getKey().split(",");
                        String table = tupleData[0];
                        String attribute = tupleData[1];
                        Long value = Long.parseLong(tupleData[2]);
                        // TODO how to handle multi-column partitioning attributes? need to talk to Aaron and Becca
                        int partitionId = p.getPartitionId(table, new Long[] {value});
                        System.out.println("Tuple " + from.getKey() + " belongs to partition " + partitionId);
                        // TODO assuming that sites get partition IDs in order
                        m_siteVertices.get(partitionId / Controller.PARTITIONS_PER_SITE).add(from.getKey());
                        m_vertexSite.put(from.getKey(), partitionId / Controller.PARTITIONS_PER_SITE);
                    }
                    //clear the transactions set
                    transaction.clear();
                    currTransactionId = tuple[0];
                    System.out.println("New tran ID = " + currTransactionId);
                }
                
                // update the current transaction
                Integer weight = transaction.get(tuple[1]);
                if (weight == null){
                    transaction.put(tuple[1], 1);
                }
                else{
                    transaction.put(tuple[1], weight+1);
                } 
                
                // read next line
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.warn("Error while reading file " + logFile.toString());
                    System.out.println("Error while reading file " + logFile.toString());
                    throw e;
                }
            }
        }
    }
    
    public AffinityGraph[] fold () throws Exception{
        // test inputs
        if (m_siteVertices == null){
            System.out.println("Graph has no mapping of sites to vertices");
            throw new Exception ();
        }
        if (m_vertexSite == null){
            System.out.println("Graph has no mapping of vertices to sites");
            throw new Exception ();
        }
        if (m_edges == null){
            System.out.println("Graph has no edges");
            throw new Exception ();
        }
        if (m_vertices == null){
            System.out.println("Graph has no vertices");
            throw new Exception ();
        }

        // DEBUG
        int i = 0;
        for (Set<String> tupleSet : m_siteVertices){
            System.out.println("Site " + i++);
            for(String tuple : tupleSet){
                System.out.println(tuple);
            }
            System.out.println("");
        }
        
        // folding
        System.out.println("Folding");
        int sitesNo = m_siteVertices.size(); 
        AffinityGraph[] res = new AffinityGraph[sitesNo];
        for(int site = 0; site < sitesNo; site++){
            AffinityGraph currGraph = res[site] = new AffinityGraph();
            // add all local vertices and edges
            Set<String> localTuples = m_siteVertices.get(site); 
            if (localTuples == null) continue; // site has no tuples
            for(String localTuple : localTuples){
                // add local vertex
                currGraph.putVertex(localTuple, m_vertices.get(localTuple));
                // scan edges from local files
                Map<String,Integer> adjacencyFromLocalTuple = m_edges.get(localTuple);
                for(Map.Entry<String, Integer> fromLocalEdge : adjacencyFromLocalTuple.entrySet()){
                    String toTuple = fromLocalEdge.getKey();
                    Integer edgeWeight =  fromLocalEdge.getValue();
                    if(localTuples.contains(toTuple)){
                        currGraph.putEdgeAddWeight(localTuple, toTuple, edgeWeight);
                    }
                    else{
                        // if other end remote, fold
                        String siteName = "Site " + m_vertexSite.get(toTuple);
                        currGraph.putVertexAddWeight(siteName, m_vertices.get(toTuple));
                        currGraph.putEdgeAddWeight(localTuple, siteName, edgeWeight);
                    }
                }               
            }
        }
        return res;
    }
    
    public Map<String, Map<String, Integer>> getEdges() {
        return m_edges;
    }

    public Map<String, Integer> getVertices() {
        return m_vertices;
    }
    
    public Integer getWeight(String tuple){
        return m_vertices.get(tuple);
    }

    public List<Set<String>> getSiteVertices() {
        return m_siteVertices;
    }
    
    public Map<String,Integer> getVertexSite(){
        return m_vertexSite;
    }
    
    public void putVertex(String vertex, Integer weight){
        m_vertices.put(vertex, weight);
    }

    public void putVertexAddWeight(String vertex, Integer weight){
        Integer currWeight = m_vertices.get(vertex);
        if(currWeight == null){
            m_vertices.put(vertex, weight);
        }
        else{
            m_vertices.put(vertex, weight + currWeight);
        }
    }

    public void putEdge(String fromVertex, String toVertex, int weight){
        Map<String,Integer> adjacency = m_edges.get(fromVertex);
        if(adjacency == null){
            adjacency = new HashMap<String,Integer>();
            m_edges.put(fromVertex, adjacency);
        }
        adjacency.put(toVertex, weight);
    }

    public void putEdgeAddWeight(String fromVertex, String toVertex, int weight){
        Map<String,Integer> adjacency = m_edges.get(fromVertex);
        if(adjacency == null){
            adjacency = new HashMap<String,Integer>();
            m_edges.put(fromVertex, adjacency);
        }
        Integer currWeight = adjacency.get(toVertex);
        if (currWeight == null){
            adjacency.put(toVertex, weight);
        }
        else{
            adjacency.put(toVertex, weight + currWeight);
        }
    }

    public void toFile(Path file){
        System.out.println("Writing graph. Size of edges: " + m_edges.size());
        BufferedWriter writer;
        String s;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(String vertex : m_vertices.keySet()){
                s = "Vertex " + vertex + " - weight " + m_vertices.get(vertex);
                writer.write(s, 0, s.length());
                writer.newLine();
                Map<String,Integer> adjacency = m_edges.get(vertex);
                if(adjacency == null){
                    writer.newLine();
                    continue;
                }
                for (Map.Entry<String, Integer> edge : adjacency.entrySet()){
                    s = edge.getKey() + " - weight " + edge.getValue();
                    writer.write(s, 0, s.length());
                    writer.newLine();
                }
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error while opening file " + file.toString());
            System.out.println("Error while opening file " + file.toString());
            return;
       }
    }
}
