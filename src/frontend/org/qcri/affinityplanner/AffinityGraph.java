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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

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
    
    public boolean loadFromFiles (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles) {
        BufferedReader reader;
        for (int i = 0; i < Controller.MAX_PARTITIONS; i++){
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

        // read intervals - in seconds
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
            double normalizedIncrement = 1.0/intervalsInSecs[currLogFile];
            currLogFile ++;
            // read first line
            try {
                reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
            } catch (IOException e) {
                LOG.warn("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                System.out.println("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                return false;
            }
            String line;
            // vertices with number of SQL statements they are involved in
            Set<String> transaction = new HashSet<String>();
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
            String currTransactionId = (line.split(";"))[0];
//            System.out.println("Tran ID = " + currTransactionId);
            // process line
            while(line != null){
//                System.out.println("Reading next line");
                String[] vertex = line.split(";");
                
                // if finished with one transaction, update graph and clear before moving on
                if (!vertex[0].equals(currTransactionId)){
//                    System.out.println("Size of transaction:" + transaction.size());
                    for(String from : transaction){
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
                        m_partitionVertices.get(partition).add(from);
                        m_vertexPartition.put(from, partition);
                        // update FROM -> TO edges
                        Set<String> visitedVertices = new HashSet<String>();    // removes duplicate vertex entries in the monitoring output
                        for(String to : transaction){
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
                    //clear the transactions set
                    transaction.clear();
                    currTransactionId = vertex[0];
//                    System.out.println("Tran ID = " + currTransactionId);
                } // END if (!vertex[0].equals(currTransactionId))
                
                // update the current transaction
                transaction.add(vertex[1]);
                
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
                
                // read next line
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
    
    /*
     * computes load of a set of vertices in the current partition. this is different from the weight of a vertex because it considers
     * both direct accesses of the vertex and the cost of remote accesses
     */
    public Double getLoadInCurrPartition(Set<String> vertices){
        double load = 0;
        for(String vertex : vertices){
            // local accesses
            load += m_vertices.get(vertex);
            // remote accesses
            int fromVertexPartition = m_vertexPartition.get(vertex);
            int fromVertexSite = PlanHandler.getSitePartition(fromVertexPartition);
            Map<String,Double> adjacencyList = m_edges.get(vertex);
            for(Map.Entry<String, Double> edge : adjacencyList.entrySet()){
                String toVertex = edge.getKey();
                int toVertexPartition = m_vertexPartition.get(toVertex);
                int toVertexSite = PlanHandler.getSitePartition(toVertexPartition);
                if(toVertexSite != fromVertexSite){
                    load += edge.getValue() * Controller.DTXN_MULTIPLIER;
                }
                else if(toVertexPartition != fromVertexPartition){
                    load += edge.getValue() * Controller.LOCAL_MPT_MULTIPLIER;
                }
            }
        }
        return load;
    }
    
    public Double getLoadPerPartition(int partition){
        return getLoadInCurrPartition(m_partitionVertices.get(partition));
    }
    
    public Double getLoadPerSite(int site){
        Collection<Integer> partitions = PlanHandler.getPartitionsSite(site);
        double load = 0;
        for (Integer partition : partitions){
            load += getLoadPerPartition(partition);
        }
        return load;
    }
    
    /*
     * returns top-k vertices from site 
     * if site has less than k vertices, return all vertices
     */
    public List<String> getHottestVertices(int partition, int k){
        k = Math.min(k, m_partitionVertices.get(partition).size());
        List<String> res = new LinkedList<String>();
        double[] loads = new double[k];
        double lowestLoad = Double.MAX_VALUE;
        int lowestPos = 0;
        int filled = 0;
        
        for(String vertex : m_partitionVertices.get(partition)){
            if (filled < k){
                res.add(vertex);
                loads[filled] = getLoadInCurrPartition(Collections.singleton(vertex));
                filled++;
                if(filled == k){
                    for (int i = 0; i < k; i++){
                        if(loads[i] < lowestLoad){
                            lowestLoad = loads[i];
                            lowestPos = i;
                        }
                    }
                }
            }
            else{
                double vertexLoad = getLoadInCurrPartition(Collections.singleton(vertex));
                if(vertexLoad > lowestLoad){
                    lowestLoad = vertexLoad;
                    res.set(lowestPos, vertex);
                    loads[lowestPos] = vertexLoad;
                    // find new lowest load
                    for (int i = 0; i < k; i++){
                        if(loads[i] < lowestLoad){
                            lowestLoad = loads[i];
                            lowestPos = i;
                        }
                    }
                }
            }
        }
        return res;
    }
    
    /*
     *     LOCAL delta a partition gets by REMOVING a SET of local vertices out
     *     it ASSUMES that the vertices are on the same partition
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     *     
     *     if newPartition = -1 we evaluate moving to an unknown REMOTE partition
     */
    public double getDeltaGiveVertices(Set<String> movedVertices, int newPartition) {
        if (movedVertices == null || movedVertices.isEmpty()){
            System.out.println("Trying to move an empty set of vertices");
            return 0;
        }
        double delta = 0;
        int fromPartition = m_vertexPartition.get(movedVertices.iterator().next());
        for(String vertex : movedVertices){ 
//            System.out.println("REMOVE delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            Double vertexWeight = m_vertices.get(vertex);
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }
            
            double outPull = 0;
            double inPull = 0;
            Map<String,Double> adjacency = m_edges.get(vertex);
            for (Map.Entry<String, Double> edge : adjacency.entrySet()){
//                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                String toVertex = edge.getKey();
                Double edgeWeight = edge.getValue();
                // edges to vertices that are moved together do not contribute to in- or out-pull
                if(!movedVertices.contains(toVertex)){
                    int toPartition = m_vertexPartition.get(toVertex); 
                    if(toPartition == fromPartition){
                        // edge to local vertex which will not be moved out
//                        System.out.println("Add weight to inpull: edge to local vertex which will not be moved out");
                        inPull += edgeWeight;
                    }
                    else {
                        // edge to remote vertex or vertex that will be moved out
//                        System.out.println("Add weight to outpull: edge to remote vertex which will be moved out");
                        outPull += edgeWeight;
                    }
                }
            }
            // decides multiplier depending on whether the newPartition is local or not
            double outMultiplier;
            if (newPartition == -1 || PlanHandler.getSitePartition(newPartition) != PlanHandler.getSitePartition(fromPartition)){
                outMultiplier = Controller.DTXN_MULTIPLIER;
            }
            else{
                outMultiplier = Controller.LOCAL_MPT_MULTIPLIER;
            }
            // update delta
            delta -= vertexWeight + outPull * outMultiplier;
            delta += inPull * outMultiplier;
//          System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, vertexWeight + (outPull - inPull) * Controller.DTXN_MULTIPLIER, delta));
        }
        return delta;
    }
    
    /*
     *     LOCAL delta a partition gets by ADDING a SET of remote vertices
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     */
    public double getDeltaReceiveVertices(Set<String> movedVertices, int newPartition) {
        if (movedVertices == null || movedVertices.isEmpty()){
            System.out.println("Trying to move an empty set of vertices");
            return 0;
        }
        double delta = 0;
        for(String vertex : movedVertices){
            // get tuple weight from original site
            Double vertexWeight = m_vertices.get(vertex);
//            System.out.println("ADD delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new IllegalStateException("Cannot include external node for delta computation");
            }
            
            double outPull = 0;
            double inPull = 0;
            // get adjacency list from original site
            Map<String,Double> adjacency = m_edges.get(vertex);
            for (Map.Entry<String, Double> edge : adjacency.entrySet()){
//                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                String toVertex = edge.getKey();
                Double edgeWeight = edge.getValue();
                // edges to vertices that are moved together do not contribute to in- or out-pull
                if(!movedVertices.contains(toVertex)){
                    int toPartition = m_vertexPartition.get(toVertex); 
                    if(toPartition == newPartition){
                        // edge to local vertex or to vertex that will be moved in
//                        System.out.println("Add weight to inpull");
                        inPull += edgeWeight;
                    }
                    else{
                        // edge to remote vertex that will not be moved in
//                        System.out.println("Add weight to outpull");
                        outPull += edgeWeight;
                    }
                }
            }
            // determine multiplier
            double outMultiplier;
            int fromPartition = m_vertexPartition.get(movedVertices.iterator().next());
            if (PlanHandler.getSitePartition(newPartition) != PlanHandler.getSitePartition(fromPartition)){
                outMultiplier = Controller.DTXN_MULTIPLIER;
            }
            else{
                outMultiplier = Controller.LOCAL_MPT_MULTIPLIER;
            }
            // determine delta
            delta += vertexWeight + outPull * outMultiplier;
            delta -= inPull * outMultiplier;
//            System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, (inPull - outPull)  * Controller.DTXN_MULTIPLIER - vertexWeight, delta));
        }
        return delta;
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
