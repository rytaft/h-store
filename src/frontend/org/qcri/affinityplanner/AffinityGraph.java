/**
 * @author Marco
 */

package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.nio.charset.Charset;
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
import org.voltdb.CatalogContext;

import edu.brown.hstore.HStoreConstants;

public class AffinityGraph {
    private static final Logger LOG = Logger.getLogger(Controller.class);
    
    // fromVertex -> adjacency list, where adjacency list is a toVertex -> edgeWeight map
    protected Map<String,Map<String,Double>> m_edges = new HashMap<String,Map<String,Double>> ();
    // vertex -> weight map
    protected Map<String,Double> m_vertices = new HashMap<String,Double> ();
    // partition -> vertex and vertex -> partition mappings
    protected List<Set<String>> m_partitionVertices = new ArrayList<Set<String>> ();
    protected Map<String,Integer> m_vertexPartition = new HashMap<String,Integer> ();
    
    // a folded graph has special edges for remote sites
    private boolean folded = false;
    
    public boolean loadFromFiles (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles, int noPartitions) {
        BufferedReader reader;
        for (int i = 0; i < noPartitions; i++){
            m_partitionVertices.add(new HashSet<String>());
        }
        
        PlanHandler planHandler;
        try {
            planHandler = new PlanHandler(planFile, catalogContext);
        } catch (Exception e) {
            LOG.warn("Could not create plan handler " + Controller.stackTraceToString(e));
            System.out.println("Could not create plan handler " + Controller.stackTraceToString(e));
            return false;
        }

        // read monitoring intervals for all sites - in seconds
        long[] intervalsInSecs = new long[intervalFiles.length];
        int currInterval = 0;
        for (Path intervalFile : intervalFiles){
            try {
                reader = Files.newBufferedReader(intervalFile, Charset.forName("US-ASCII"));
                String line = reader.readLine();
                reader.close();
                intervalsInSecs[currInterval] = Long.parseLong(line) / 1000;
                currInterval++;
            } catch (IOException e) {
                LOG.warn("Error while reading file " + intervalFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                System.out.println("Error while reading file " + intervalFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                return false;
            }
        }
        
        // scan files for all partitions
        int currLogFile = 0;
        for (Path logFile : logFiles){
            // SETUP
            double normalizedIncrement = 1.0/intervalsInSecs[currLogFile];
            currLogFile ++;
            try {
                reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
            } catch (IOException e) {
                LOG.warn("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                System.out.println("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                return false;
            }
            String line;
            HashMap<String, Set<String>> transactions = new HashMap<String,Set<String>>();
            // vertices with number of SQL statements they are involved in
            // READ FIRST LINE
            try {
                line = reader.readLine();
            } catch (IOException e) {
                LOG.warn("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                System.out.println("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                return false;
            }
            if (line == null){
                LOG.warn("File " + logFile.toString() + " is empty");
                return false;
            }
//            System.out.println("Tran ID = " + currTransactionId);
            // PROCESS LINE
            while(line != null){
//                System.out.println("Reading next line");
                String[] fields = line.split(";");
                // if finished with one transaction, update graph and clear before moving on
                if (fields[0].equals("END")){
                    String transaction_id = fields[1];
                    Set<String> curr_transaction = transactions.get(transaction_id);
                    if(curr_transaction != null){
    //                    System.out.println("Size of transaction:" + transaction.size());
                        for(String from : curr_transaction){
                            // update FROM vertex in graph
                            Double currentVertexWeight = m_vertices.get(from);
                            if (currentVertexWeight == null){
                                m_vertices.put(from, normalizedIncrement);
                            }
                            else{
                                m_vertices.put(from, currentVertexWeight + normalizedIncrement);                         
                            }
                            // store site mappings for FROM vertex
                            int partition = 0;
                            try {
                                partition = planHandler.getPartition(from);
                            } catch (Exception e) {
                                LOG.warn("Could not get partition from plan handler " + Controller.stackTraceToString(e));
                                System.out.println("Could not get partition from plan handler " + Controller.stackTraceToString(e));
                                return false;                            
                            }
                            if (partition == HStoreConstants.NULL_PARTITION_ID){
                                LOG.info("Exiting graph loading. Could not find partition for key " + from);
                                System.out.println("Exiting graph loading. Could not find partition for key " + from);
                                return false;                            
                            }
                            m_partitionVertices.get(partition).add(from);
                            m_vertexPartition.put(from, partition);
                            // update FROM -> TO edges
                            Set<String> visitedVertices = new HashSet<String>();    // removes duplicate vertex entries in the monitoring output
                            for(String to : curr_transaction){
                                if (! from.equals(to) && ! visitedVertices.contains(to)){
                                    visitedVertices.add(to);
                                    Map<String,Double> adjacency = m_edges.get(from);
                                    if(adjacency == null){
                                        adjacency = new HashMap<String,Double>();
                                        m_edges.put(from, adjacency);
                                    }
                                    Double currentEdgeWeight = adjacency.get(to);
                                    if (currentEdgeWeight == null){
                                        adjacency.put(to, normalizedIncrement);
                                    }
                                    else{
                                        adjacency.put(to, currentEdgeWeight + normalizedIncrement);
                                    }
                                }
                            } // END for(Map.Entry<String, Double> to : transaction.entrySet())
                        } // END for(Map.Entry<String,Double> from : transaction.entrySet())
                        transactions.remove(transaction_id);
    //                    System.out.println("Tran ID = " + currTransactionId);
                    } // END if(transactions.get(transaction_id) != null)
                } // END if (fields[0].equals("END"))
                else{
                    // update the current transaction
                    String transaction_id = fields[0];
                    Set<String> curr_transaction = transactions.get(transaction_id);
                    if(curr_transaction == null){
                        curr_transaction = new HashSet<String>();
                        transactions.put(transaction_id, curr_transaction);
                    }
                    curr_transaction.add(fields[1]);
                }
                
                /* this is what one would to count different accesses within the same transaction. 
                 * For the moment I count only the number of transactions accessing a tuple
                Double weight = transaction.get(vertex[1]);
                if (weight == null){
                    transaction.put(vertex[1], 1.0/intervalsInSecs[currLogFile]);
                }
                else{
                    transaction.put(vertex[1], (weight+1.0)/intervalsInSecs[currLogFile]);
                } 
                */
                
                // READ NEXT LINE
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    LOG.warn("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                    System.out.println("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                    return false;
                }
            }// END  while(line != null)
        } // END for (Path logFile : logFiles)
        
        // normalize all weights using the monitoring intervals
        
        return true;
    }
    
//    public AffinityGraph[] fold () throws Exception{
//        // test inputs
//        if (m_partitionVertices == null){
//            System.out.println("Graph has no mapping of sites to vertices");
//            throw new Exception ();
//        }
//        if (m_vertexPartition== null){
//            System.out.println("Graph has no mapping of vertices to sites");
//            throw new Exception ();
//        }
//        if (m_edges == null){
//            System.out.println("Graph has no edges");
//            throw new Exception ();
//        }
//        if (m_vertices == null){
//            System.out.println("Graph has no vertices");
//            throw new Exception ();
//        }
//        if (this.folded){
//            System.out.println("Graph has been folded already");
//            throw new Exception ();            
//        }
//
//        // DEBUG
//        int i = 0;
//        for (Set<String> vertexSet : m_siteVertices){
//            System.out.println("Site " + i++);
//            for(String vertex : vertexSet){
//                System.out.println(vertex);
//            }
//            System.out.println("");
//        }
//        
//        // folding
//        System.out.println("Folding");
//        int sitesNo = m_siteVertices.size(); 
//        AffinityGraph[] res = new AffinityGraph[sitesNo];
//        for(int site = 0; site < sitesNo; site++){
//            AffinityGraph currGraph = res[site] = new AffinityGraph();
//            currGraph.folded = true;
//            // add all local vertices and edges
//            Set<String> localVertices = m_siteVertices.get(site);
//            if (localVertices == null) continue; // site has no vertices
//            for(String localVertex : localVertices){
//                // add local vertex
//                Integer vertexWeight = m_vertices.get(localVertex);
//                currGraph.putVertex(localVertex, vertexWeight);
//                // scan edges from local files
//                Map<String,Integer> adjacencyFromLocalVertex = m_edges.get(localVertex);
//                for(Map.Entry<String, Integer> fromLocalEdge : adjacencyFromLocalVertex.entrySet()){
//                    String toVertex = fromLocalEdge.getKey();
//                    Integer edgeWeight =  fromLocalEdge.getValue();
//                    if(localVertices.contains(toVertex)){
//                        currGraph.putEdgeAddWeight(localVertex, toVertex, edgeWeight);
//                    }
//                    else{
//                        // if other end remote, fold
//                        String siteName = "Site " + m_vertexSite.get(toVertex);
//                        // the weight of a remote site is the sum of the weights of the edges
//                        currGraph.putVertexAddWeight(siteName, edgeWeight);
//                        currGraph.putEdgeAddWeight(localVertex, siteName, edgeWeight);
//                    }
//                }               
//            }
//        }
//        return res;
//    }
    
    public Map<String, Map<String, Double>> getEdges() {
        return m_edges;
    }

    public Map<String, Double> getVertices() {
        return m_vertices;
    }
    
    public Double getVertexWeight(String vertex){
        return m_vertices.get(vertex);
    }
    
    public int getPartitionsNo(){
        if(m_partitionVertices.size() <= 0){
            return -1;
        }
        return m_partitionVertices.size();
    }
        
    public void addPartitions(int noOfPartitions){
        for (int i = 0; i < noOfPartitions; i++){
            m_partitionVertices.add(new HashSet<String>());
        }
    }

//    public List<Set<String>> getPartitionVertices() {
//        return m_siteVertices;
//    }
    
//    public Map<String,Integer> getVertexSite(){
//        return m_vertexSite;
//    }
    
    public void putVertex(String vertex, Double weight){
        m_vertices.put(vertex, weight);
    }

    public void putVertexAddWeight(String vertex, Double weight){
        Double currWeight = m_vertices.get(vertex);
        if(currWeight == null){
            m_vertices.put(vertex, weight);
        }
        else{
            m_vertices.put(vertex, weight + currWeight);
        }
    }

    public void putEdge(String fromVertex, String toVertex, double weight){
        Map<String,Double> adjacency = m_edges.get(fromVertex);
        if(adjacency == null){
            adjacency = new HashMap<String,Double>();
            m_edges.put(fromVertex, adjacency);
        }
        adjacency.put(toVertex, weight);
    }

    public void putEdgeAddWeight(String fromVertex, String toVertex, double weight){
        Map<String,Double> adjacency = m_edges.get(fromVertex);
        if(adjacency == null){
            adjacency = new HashMap<String,Double>();
            m_edges.put(fromVertex, adjacency);
        }
        Double currWeight = adjacency.get(toVertex);
        if (currWeight == null){
            adjacency.put(toVertex, weight);
        }
        else{
            adjacency.put(toVertex, weight + currWeight);
        }
    }
    
    public boolean isFolded(){
        return folded;
    }
    
    public void moveVertices(Set<String> movedVertices, int fromPartition, int toPartition) {
//        m_cache_vertexLoad = null;
//        m_cache_siteLoad[fromSite] += getDeltaGiveVertices(movedVertices);
//        m_cache_siteLoad[toSite] += getDeltaReceiveVertices(movedVertices, toSite);
        for (String movedVertex : movedVertices){
            m_partitionVertices.get(fromPartition).remove(movedVertex);
            m_partitionVertices.get(toPartition).add(movedVertex);
            m_vertexPartition.put(movedVertex, toPartition);
        }
    }
        
    public void toFile(Path file){
        System.out.println("Writing graph. Size of edges: " + m_edges.size());
        BufferedWriter writer;
        String s;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(String vertex : m_vertices.keySet()){
                s = vertex + "," + m_vertices.get(vertex) + ";";
                writer.write(s, 0, s.length());
                Map<String,Double> adjacency = m_edges.get(vertex);
                if(adjacency != null){
                    for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                        s = ";" + edge.getKey() + "," + edge.getValue();
                        writer.write(s, 0, s.length());
                    }
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

    public void toFileDebug(Path file){
        System.out.println("Writing graph. Number of vertices: " + m_edges.size());
        BufferedWriter writer;
        String s;
        double totalWeight = 0;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(String vertex : m_vertices.keySet()){
                s = "Vertex " + vertex + " - weight " + m_vertices.get(vertex);
                totalWeight += m_vertices.get(vertex);
                writer.write(s, 0, s.length());
                writer.newLine();
                Map<String,Double> adjacency = m_edges.get(vertex);
                if(adjacency == null){
                    writer.newLine();
                    continue;
                }
                for (Map.Entry<String, Double> edge : adjacency.entrySet()){
                    s = edge.getKey() + " - weight " + edge.getValue();
                    writer.write(s, 0, s.length());
                    writer.newLine();
                }
                writer.newLine();
            }
            s = "Total weight: " + totalWeight;
            writer.write(s, 0, s.length());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error while opening file " + file.toString());
            System.out.println("Error while opening file " + file.toString());
            return;
       }
    }
}
