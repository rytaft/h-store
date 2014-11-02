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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.CatalogContext;

import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.TwoTieredRangePartitions;
import edu.brown.utils.FileUtil;

public class AffinityGraph {
    private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);
    
    // fromVertex -> adjacency list, where adjacency list is a toVertex -> edgeWeight map
    protected Map<String,Map<String,Integer>> m_edges = new HashMap<String,Map<String,Integer>> ();
    // vertex -> weight map
    protected Map<String,Integer> m_vertices = new HashMap<String,Integer> ();
    // site -> vertex and vertex -> site mappings
    protected List<Set<String>> m_siteVertices = new ArrayList<Set<String>> ();
    protected Map<String,Integer> m_vertexSite = new HashMap<String,Integer> ();
    
    // a folded graph has special edges for remote sites
    private boolean folded = false;
    
    // load caches
    private int[] m_cache_siteLoad = null;
    private HashMap<String, Integer> m_cache_vertexLoad;
    
    public void loadFromFiles (CatalogContext catalogContext, File planFile, int partitions, Path[] logFiles) throws Exception{
        BufferedReader reader;
        int sites = partitions / Controller.PARTITIONS_PER_SITE;
        if (partitions % Controller.PARTITIONS_PER_SITE != 0){
            sites++;
        }
        System.out.println("Sites = " + sites);
        for (int i = 0; i < sites; i++){
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
            // read first line
            try {
                reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Error while opening file " + logFile.toString());
                throw e;
            }
            String line;
            // vertices with number of SQL statements they are involved in
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
//            System.out.println("Tran ID = " + currTransactionId);
            // process line
            while(line != null){
//                System.out.println("Reading next line");
                String[] vertex = line.split(";");
                
                // if finished with one transaction, update graph and clear before moving on
                if (!vertex[0].equals(currTransactionId)){
//                    System.out.println("Size of transaction:" + transaction.size());
                    for(Map.Entry<String,Integer> from : transaction.entrySet()){
                        // update FROM vertex in graph
                        Integer currentVertexWeight = m_vertices.get(from.getKey());
                        if (currentVertexWeight == null){
                            m_vertices.put(from.getKey(), 1);   // vertices.put(from.getKey(), from.getValue());
                        }
                        else{
                            m_vertices.put(from.getKey(), currentVertexWeight+1);   // vertices.put(from.getKey(), currentVertexWeight+from.getValue());                         
                        }
                        // store site mappings for FROM vertex
                        int site = getSite(from.getKey(), p);
                        m_siteVertices.get(site).add(from.getKey());
                        m_vertexSite.put(from.getKey(), site);
                        // update FROM -> TO edges
                        Set<String> visitedVertices = new HashSet<String>();    // removes duplicate vertex entries in the monitoring output
                        for(Map.Entry<String, Integer> to : transaction.entrySet()){
                            if (! from.getKey().equals(to.getKey()) && ! visitedVertices.contains(to.getKey())){
                                visitedVertices.add(to.getKey());
                                Map<String,Integer> adjacency = m_edges.get(from.getKey());
                                if(adjacency == null){
                                    adjacency = new HashMap<String,Integer>();
                                    m_edges.put(from.getKey(), adjacency);
                                }
                                Integer currentEdgeWeight = adjacency.get(to.getKey());
                                if (currentEdgeWeight == null){
                                    adjacency.put(to.getKey(), 1);  // adjacency.put(to.getKey(), to.getValue());
                                }
                                else{
                                    adjacency.put(to.getKey(), currentEdgeWeight + 1);  // adjacency.put(to.getKey(), currentEdgeWeight + to.getValue());
                                }
                            }
                        }
                    }
                    //clear the transactions set
                    transaction.clear();
                    currTransactionId = vertex[0];
//                    System.out.println("Tran ID = " + currTransactionId);
                }
                
                // update the current transaction
                Integer weight = transaction.get(vertex[1]);
                if (weight == null){
                    transaction.put(vertex[1], 1);
                }
                else{
                    transaction.put(vertex[1], weight+1);
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
    
    /*
     * Returns the site of a vertex
     * vertex is specified as "TABLE_ID,ATTRIBUTE_NAME,VALUE"
     * 
     * *******ASSUMPTIONS (TODO)********
     * - Sites get partition IDs in order, ie., site 0 takes 0,..,N-1, site 1 takes N,...,2N-1 etc.
     * - Does not handle multi-column partitioning attributes. This will depend on the output of monitoring for that case
     */
    private int getSite(String vertex, ExplicitPartitions p) throws Exception{
        String[] vertexData = vertex.split(",");
        String table = vertexData[0];
        String attribute = vertexData[1];
        Long value = Long.parseLong(vertexData[2]);
        int partitionId = p.getPartitionId(table, new Long[] {value});
//        System.out.println("Vertex " + from.getKey() + " belongs to partition " + partitionId);
        return partitionId / Controller.PARTITIONS_PER_SITE;
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
        if (this.folded){
            System.out.println("Graph has been folded already");
            throw new Exception ();            
        }

        // DEBUG
        int i = 0;
        for (Set<String> vertexSet : m_siteVertices){
            System.out.println("Site " + i++);
            for(String vertex : vertexSet){
                System.out.println(vertex);
            }
            System.out.println("");
        }
        
        // folding
        System.out.println("Folding");
        int sitesNo = m_siteVertices.size(); 
        AffinityGraph[] res = new AffinityGraph[sitesNo];
        for(int site = 0; site < sitesNo; site++){
            AffinityGraph currGraph = res[site] = new AffinityGraph();
            currGraph.folded = true;
            // add all local vertices and edges
            Set<String> localVertices = m_siteVertices.get(site);
            if (localVertices == null) continue; // site has no vertices
            for(String localVertex : localVertices){
                // add local vertex
                Integer vertexWeight = m_vertices.get(localVertex);
                currGraph.putVertex(localVertex, vertexWeight);
                // scan edges from local files
                Map<String,Integer> adjacencyFromLocalVertex = m_edges.get(localVertex);
                for(Map.Entry<String, Integer> fromLocalEdge : adjacencyFromLocalVertex.entrySet()){
                    String toVertex = fromLocalEdge.getKey();
                    Integer edgeWeight =  fromLocalEdge.getValue();
                    if(localVertices.contains(toVertex)){
                        currGraph.putEdgeAddWeight(localVertex, toVertex, edgeWeight);
                    }
                    else{
                        // if other end remote, fold
                        String siteName = "Site " + m_vertexSite.get(toVertex);
                        // the weight of a remote site is the sum of the weights of the edges
                        currGraph.putVertexAddWeight(siteName, edgeWeight);
                        currGraph.putEdgeAddWeight(localVertex, siteName, edgeWeight);
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
    
    public Integer getVertexWeight(String vertex){
        return m_vertices.get(vertex);
    }
    
    public int getSitesNo(){
        if(m_siteVertices.size() <= 0){
            return -1;
        }
        return m_siteVertices.size();
    }
    
    /*
     * populates the load caches so that the computation does not need to be done over and over again
     */
    private void fillLoadCaches(){
        m_cache_siteLoad = new int [Controller.MAX_SITES];
        m_cache_vertexLoad = new HashMap<String,Integer>();
        // process each site
        for (int i = 0; i < getSitesNo(); i++){
            // iterate all vertices of site
            Set<String> localVertices = m_siteVertices.get(i);
            for(String vertex : localVertices){
                m_cache_siteLoad[i] += getLoadInCurrSite(vertex);
                m_cache_vertexLoad.put(vertex,getLoadInCurrSite(vertex));
            }
        }
    }
    
    public void addSite(){
        if(m_cache_siteLoad != null){
            m_cache_siteLoad[m_siteVertices.size()] = 0;
            m_siteVertices.add(new HashSet<String>());
        }
    }
    
    private void print(){
        for (int i = 0; i < getSitesNo(); i++){
            System.out.println("Site " + i + " having vertices " + m_siteVertices.get(i).toString());
        }
    }
    
    /*
     * returns -1 if computeLoads has not been called
     */
    public int getLoadPerSite(int site){
        if (m_cache_siteLoad == null){
            fillLoadCaches();
        }
        return m_cache_siteLoad[site];
    }
    
    /*
     * computes load of a vertex if placed on a site. this is different from the weight of a vertex because it considers
     * both direct accesses of the vertex and the cost of remote accesses
     */
    public Integer getLoadInCurrSite(String vertex){
        // the cache is valid only for the current site
        if(m_cache_vertexLoad == null || m_cache_vertexLoad.get(vertex) == null){
            // local accesses
            int load = m_vertices.get(vertex);
            // remote accesses
            Map<String,Integer> adjacencyList = m_edges.get(vertex);
            int site = m_vertexSite.get(vertex);
            for(Map.Entry<String, Integer> edge : adjacencyList.entrySet()){
                if(m_vertexSite.get(edge.getKey()) != site){
                    load += edge.getValue() * Controller.DTXN_MULTIPLIER;
                }
            }
            return load;
        }
        else{
            return m_cache_vertexLoad.get(vertex);
        }
    }
    
    /*
     * returns top-k vertices from site 
     * if site has less than k vertices, return all vertices
     */
    public List<String> getHottestVertices(int site, int k){
        k = Math.min(k, m_siteVertices.get(site).size());
        List<String> res = new LinkedList<String>();
        int[] loads = new int[k];
        int lowestLoad = Integer.MAX_VALUE;
        int lowestPos = 0;
        int filled = 0;
        
        for(String vertex : m_siteVertices.get(site)){
            if (filled < k){
                res.add(vertex);
                loads[filled] = getLoadInCurrSite(vertex);
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
                int vertexLoad = getLoadInCurrSite(vertex);
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
     *     LOCAL delta a site gets by REMOVING a SET of local vertices out of a site
     *     it ASSUMES that the vertices are on the same site
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     */
    public int getDeltaGiveVertices(Set<String> movedVertices) throws Exception{
        if (movedVertices == null){
            System.out.println("Trying to move an empty set of vertices");
            throw new Exception();
        }
        int delta = 0;
        int site = m_vertexSite.get(movedVertices.iterator().next());
        for(String vertex : movedVertices){ 
//            System.out.println("REMOVE delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            Integer vertexWeight = m_vertices.get(vertex);
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new Exception();
            }
            
            int outPull = 0;
            int inPull = 0;
            Map<String,Integer> adjacency = m_edges.get(vertex);
            for (Map.Entry<String, Integer> edge : adjacency.entrySet()){
//                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                String toVertex = edge.getKey();
                Integer edgeWeight = edge.getValue();
                // edges to vertices that are moved together do not contribute to in- or out-pull
                if(!movedVertices.contains(toVertex)){
                    if(m_vertexSite.get(toVertex) == site){
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
            delta -= vertexWeight + outPull * Controller.DTXN_MULTIPLIER;
            delta += inPull * Controller.DTXN_MULTIPLIER;
//          System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, vertexWeight + (outPull - inPull) * Controller.DTXN_MULTIPLIER, delta));
        }
        return delta;
    }
    
    /*
     *     LOCAL delta a site gets by ADDING a SET of remote vertices
     *     
     *     this is NOT like computing the load because local edges come as a cost in this case 
     *     it also considers a SET of vertices to be moved together
     */
    public int getDeltaReceiveVertices(Set<String> movedVertices, int newSite) throws Exception{
        if (movedVertices == null){
            System.out.println("Trying to move an empty set of vertices");
            throw new Exception();
        }
        int delta = 0;
        for(String vertex : movedVertices){
            // get tuple weight from original site
            Integer vertexWeight = m_vertices.get(vertex);
//            System.out.println("ADD delta: vertex " + vertex + " with weight " + m_vertices.get(vertex));
            if (vertexWeight == null){
                System.out.println("Cannot include external node for delta computation");
                throw new Exception();
            }
            
            int outPull = 0;
            int inPull = 0;
            // get adjacency list from original site
            Map<String,Integer> adjacency = m_edges.get(vertex);
            for (Map.Entry<String, Integer> edge : adjacency.entrySet()){
//                System.out.println("Considering edge to vertex " + edge.getKey() + " with weight " + edge.getValue());
                String toVertex = edge.getKey();
                Integer edgeWeight = edge.getValue();
                // edges to vertices that are moved together do not contribute to in- or out-pull
                if(!movedVertices.contains(toVertex)){
                    if(m_vertexSite.get(toVertex) == newSite){
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
            delta += vertexWeight + outPull * Controller.DTXN_MULTIPLIER;
            delta -= inPull * Controller.DTXN_MULTIPLIER;
//            System.out.println(String.format("inpull %d outpull %d addition to delta %d total delta %d", inPull, outPull, (inPull - outPull)  * Controller.DTXN_MULTIPLIER - vertexWeight, delta));
        }
        return delta;
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
    
    public boolean isFolded(){
        return folded;
    }
    
    public void moveVertices(Set<String> movedVertices, int fromSite, int toSite) throws Exception{
        m_cache_vertexLoad = null;
        m_cache_siteLoad[fromSite] += getDeltaGiveVertices(movedVertices);
        m_cache_siteLoad[toSite] += getDeltaReceiveVertices(movedVertices, toSite);
        for (String movedVertex : movedVertices){
            m_siteVertices.get(fromSite).remove(movedVertex);
            m_siteVertices.get(toSite).add(movedVertex);
            m_vertexSite.put(movedVertex, toSite);
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
                Map<String,Integer> adjacency = m_edges.get(vertex);
                if(adjacency != null){
                    for (Map.Entry<String, Integer> edge : adjacency.entrySet()){
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
        int totalWeight = 0;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(String vertex : m_vertices.keySet()){
                s = "Vertex " + vertex + " - weight " + m_vertices.get(vertex);
                totalWeight += m_vertices.get(vertex);
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
