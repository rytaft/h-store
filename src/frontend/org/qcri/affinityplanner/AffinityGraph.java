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
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.hstore.HStoreConstants;

public class AffinityGraph {
  
    private static final Logger LOG = Logger.getLogger(AffinityGraph.class);

    // determines granularity of edges, either vertex - vertex of vertex - partition 
    private static boolean m_tupleGranularity = false;
    
    // fromVertex -> adjacency list, where adjacency list is a toVertex -> edgeWeight map or toPartition -> edgeWeight map, depending on granularity 
    // (weight of edge is # of txns touching both partitions per second)
    protected static Int2ObjectOpenHashMap<Int2DoubleOpenHashMap> m_edges = new Int2ObjectOpenHashMap <Int2DoubleOpenHashMap> ();
    
    // vertex -> full name
    protected static Int2ObjectOpenHashMap<String> m_vertex_to_name = new Int2ObjectOpenHashMap <String> ();

    // vertex -> weight map (number of txns accesses the vertext per second)
    protected static final Int2DoubleOpenHashMap m_vertices = new Int2DoubleOpenHashMap (1000);
    
    // partition -> vertex and vertex -> partition mappings
    protected static List<IntOpenHashSet> m_partitionVertices = new ArrayList<IntOpenHashSet> ();
    protected static Int2IntOpenHashMap m_vertexPartition = new Int2IntOpenHashMap ();
    
    private static PlanHandler m_plan_handler = null;
    
    private static long[] m_intervalsInSecs;
    
    private static class LoaderThread implements Runnable {
        private Path[] logFiles;
        private AtomicInteger nextLogFileCounter;
        
        public LoaderThread(Path[] logFiles, AtomicInteger nextLogFileCounter){
            this.logFiles = logFiles;
            this.nextLogFileCounter = nextLogFileCounter;
        }

        @Override
        public void run() {
            try {
                int nextLogFile = nextLogFileCounter.getAndIncrement();
                
                while (nextLogFile < logFiles.length){
                    
                    double increment = 1.0/m_intervalsInSecs[nextLogFile];
                    
                    loadLogFile(logFiles[nextLogFile], increment);
                    
                    nextLogFile = nextLogFileCounter.getAndIncrement();                    
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        private void loadLogFile(Path logFile, double normalizedIncrement) throws Exception{
            
            // init local data structures to be merged later
            Int2ObjectOpenHashMap<String> vertex_to_name = new Int2ObjectOpenHashMap <String> ();
            Int2IntOpenHashMap vertexPartition = new Int2IntOpenHashMap();
            Int2DoubleOpenHashMap vertices = new Int2DoubleOpenHashMap (1000);
            List<IntOpenHashSet> partitionVertices = new ArrayList<IntOpenHashSet> ();
            for (int i = 0; i < m_partitionVertices.size(); i++){
                partitionVertices.add(new IntOpenHashSet());
            }

            
            System.out.println("Loading from file " + logFile);

            // SETUP
            BufferedReader reader;
            try {
                reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
            } catch (IOException e) {
                Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                throw e;
            }
            String line;
            // vertices with number of SQL statements they are involved in
            // READ FIRST LINE
            try {
                line = reader.readLine();
            } catch (IOException e) {
                Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                throw e;
            }
            if (line == null){
                Controller.record("File " + logFile.toString() + " is empty");
                throw new Exception();
            }

//            System.out.println("Tran ID = " + currTransactionId);
            
            HashMap<String, IntOpenHashSet> transactions = new HashMap<String,IntOpenHashSet>();

            // PROCESS LINE
            while(line != null){
                
                String[] fields = line.split(";");

                // if finished with one transaction, update graph and clear before moving on

                if (fields[0].equals("END")){
                    
                    String transaction_id = fields[1]; 
                    IntOpenHashSet curr_transaction = transactions.get(transaction_id);
                    
                    if(curr_transaction != null){

                        for(int from : curr_transaction){
                            // store partition mappings for FROM vertex
                            String fromName = vertex_to_name.get(from);
                            int partition = m_plan_handler.getPartition(fromName);
                            partitionVertices.get(partition).add(from);
                            vertexPartition.put(from, partition);
                            
                            // update FROM vertex in graph
                            double currentVertexWeight = vertices.get(from);
                            if (currentVertexWeight == vertices.defaultReturnValue()){
                                vertices.put(from, normalizedIncrement);
                            }
                            else{
                                vertices.put(from, currentVertexWeight + normalizedIncrement);                         
                            }
                            
                            // update FROM -> TO edges
                            Int2DoubleOpenHashMap adjacency = null;
    
                            synchronized(m_edges){
                                adjacency = m_edges.get(from);
                                if(adjacency == null){
                                    adjacency = new Int2DoubleOpenHashMap ();
                                    m_edges.put(from, adjacency);
                                }
                            }
    
                            IntOpenHashSet visitedVertices = new IntOpenHashSet();
                            for(int to : curr_transaction){
                                
                                if (from != to && !visitedVertices.contains(to)){
                                    visitedVertices.add(to);
                                    
                                    // if lower granularity, edges link vertices to partitions, not other vertices
                                    if(!m_tupleGranularity){
                                        String toName = vertex_to_name.get(to);
                                        to = m_plan_handler.getPartition(toName);
                                    }
                                    
                                    synchronized(adjacency){
                                        double currentEdgeWeight = adjacency.get(to);
                                        if (currentEdgeWeight == 0){ // TODO use adjacency.defaultReturnValue() instead of 0
                                            adjacency.put(to, normalizedIncrement);
                                        }
                                        else{
                                            adjacency.put(to, currentEdgeWeight + normalizedIncrement);
                                        }
                                    }
                                }
                            } // END for(String to : curr_transaction)
    
                        } // END for(String from : curr_transaction)
    
                        transactions.remove(transaction_id);
                    } // END if (curr_transaction != null)
                    
                    //                    System.out.println("Tran ID = " + currTransactionId);
                } // END if (line.equals("END"))

                else{
                    // add the vertex to the transaction
                    String transaction_id = fields[0];
                    int hash = fields[1].hashCode();

                    IntOpenHashSet curr_transaction = transactions.get(transaction_id);
                    if (curr_transaction == null){
                        curr_transaction = new IntOpenHashSet ();
                        transactions.put(transaction_id, curr_transaction);
                    }
                    curr_transaction.add(hash);

                    if (!vertex_to_name.containsKey(hash)){
                        vertex_to_name.put(hash, fields[1]);
                    }
                } // END if (!line.equals("END"))

                /* this is what one would do to count different accesses within the same transaction. 
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
                    Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                    System.out.println("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                    throw e;
                }
            }// END  while(line != null)
            
            // merge local data structures
            synchronized(m_vertex_to_name){
                m_vertex_to_name.putAll(vertex_to_name);
            }
            synchronized(m_vertexPartition){
                m_vertexPartition.putAll(vertexPartition);
            }
            for (int i = 0; i < partitionVertices.size(); i++){
                IntSet localVertices = partitionVertices.get(i);
                synchronized(m_partitionVertices){
                    m_partitionVertices.get(i).addAll(localVertices);
                }
            }
            for (Int2DoubleMap.Entry vertex : vertices.int2DoubleEntrySet()){
                double curr_weight = 0;
                synchronized(m_vertices){
                    curr_weight = m_vertices.get(vertex.getIntKey());
                    if(curr_weight != vertices.defaultReturnValue()){
                        curr_weight += vertex.getDoubleValue();
                    }
                    else{
                        curr_weight = vertex.getDoubleValue();
                    }
                    m_vertices.put(vertex.getIntKey(), curr_weight);
                }
            }
        }
    }
    
    public AffinityGraph(boolean tupleGranularity, CatalogContext catalogContext, File planFile, 
            Path[] logFiles, Path[] intervalFiles, int noPartitions) throws Exception {
        m_tupleGranularity = tupleGranularity;

        BufferedReader reader;

        for (int i = 0; i < noPartitions; i++){
            m_partitionVertices.add(new IntOpenHashSet());
        }

        try {
            m_plan_handler = new PlanHandler(planFile, catalogContext);
        } catch (Exception e) {
            Controller.record("Could not create plan handler " + Controller.stackTraceToString(e));
            throw e;
        }

        // read monitoring intervals for all sites - in seconds
        m_intervalsInSecs = new long[intervalFiles.length];
        int currInterval = 0;
        for (Path intervalFile : intervalFiles){
            if (intervalFile == null){
                Controller.record("interval file name is null");
                throw new Exception();
            }
            try {
                reader = Files.newBufferedReader(intervalFile, Charset.forName("US-ASCII"));
                String line = reader.readLine();
                reader.close();
                m_intervalsInSecs[currInterval] = Long.parseLong(line) / 1000;
                currInterval++;
            } catch (IOException e) {
                Controller.record("Error while reading interval file " + intervalFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                Thread.sleep(1000);
                throw e;
            }
            catch (NumberFormatException e1){
                Controller.record("Error while converting interval from file " + intervalFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e1));
                Thread.sleep(1000);
                throw e1;
            }
        }
        
        // scan files for all partitions
        int threads = Math.min(Controller.LOAD_THREADS, logFiles.length);
        Thread[] loadingThreads = new Thread[threads];
        AtomicInteger nextLogFileCounter = new AtomicInteger(0);
        
        for (int currLogFile = 0; currLogFile < threads; currLogFile++){
            
            Thread loader = new Thread (new LoaderThread (logFiles, nextLogFileCounter));
            loadingThreads[currLogFile] = loader;
            loader.start();
        }
        
        // wait for all loader threads to finish
        for (Thread loader : loadingThreads){
            loader.join();
        }
    }
    
    public void moveHotVertices(IntSet movedVertices, int fromPartition, int toPartition) {
        for (int movedVertex : movedVertices){
            m_partitionVertices.get(fromPartition).remove(movedVertex);
            m_partitionVertices.get(toPartition).add(movedVertex);
            m_vertexPartition.put(movedVertex, toPartition);

            // update plan too
            // format of vertices is <TABLE>,<VALUE>
            String movedVertexName = m_vertex_to_name.get(movedVertex);
            String [] fields = movedVertexName.split(",");
            
            if(Controller.ROOT_TABLE == null){
    //            System.out.println("table: " + fields[0] + " from partition: " + fromPartition + " to partition " + toPartition);
    //            System.out.println("remove ID: " + fields[1]);
                m_plan_handler.removeTupleId(fields[0], fromPartition, Long.parseLong(fields[1]));
    //            System.out.println("After removal");
    //            System.out.println(m_plan_handler.toString() + "\n");
    //            m_plan_handler.addPartition(fields[0], toPartition);
    //            System.out.println("After adding partition");
    //            System.out.println(m_plan_handler.toString() + "\n");
                m_plan_handler.addRange(fields[0], toPartition, Long.parseLong(fields[1]), Long.parseLong(fields[1]));
    //            System.out.println("After adding range");
    //            System.out.println(m_plan_handler.toString() + "\n");
    //            System.exit(0);
            }
            else{
                m_plan_handler.removeTupleIdAllTables(fromPartition, Long.parseLong(fields[1]));
                m_plan_handler.addRangeAllTables(toPartition, Long.parseLong(fields[1]), Long.parseLong(fields[1]));
            }
        }
        
//        System.out.println(m_plan_handler.toString() + "\n");
    }
    
    public String getTupleName(int hash){
        return m_vertex_to_name.get(hash);
    }
    
    public void moveColdRange(String table, Plan.Range movedRange, int fromPart, int toPart){     
        m_plan_handler.moveColdRange(table, movedRange, fromPart, toPart);
    }
    
    public void moveColdRangeAllTables(Plan.Range movedRange, int fromPart, int toPart){
        m_plan_handler.moveColdRangeAllTables(movedRange, fromPart, toPart);
    }
    
    public int getPartition(int vertex){
        String vertexName = m_vertex_to_name.get(vertex);
        return m_plan_handler.getPartition(vertexName);
    }
    
    public static boolean isActive(int partition){
        return m_plan_handler.isActive(partition);
    }
    
    public void planToJSON(String newPlanFile){
        m_plan_handler.toJSON(newPlanFile);
    }
    
    
    
    public void toMetisFile(Path file, Path mapOut){
        LOG.info("Writing graph for " + file.toString() +" . Number of vertices: " + m_vertices.size());
        BufferedWriter writer, mapWriter = null;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            String graphInfo = "011"; //ABC A:VertexSize B:VertexWeight C:EdgeWeight
            int edgeCount=0;
            for(int v_id : m_vertices.keySet()){                
                if (m_edges.get(v_id)!=null)
                    edgeCount+=m_edges.get(v_id).size();
            }
            String header = String.format("%s %s %s",m_vertices.size(), (int)(edgeCount/2), graphInfo);
            writer.write(header);
            writer.newLine();
            
            //Need an ordered list of vertexes 1 .. N
            Int2IntOpenHashMap vert_to_increment = new Int2IntOpenHashMap(m_vertices.size());
            int[] vert_ids = new int[m_vertices.size()+1];
            int count = 1;
            for (int vert_hash: m_vertices.keySet()){
                vert_ids[count] = vert_hash;
                vert_to_increment.put(vert_hash, count);
                count++;
            }
            if (mapOut != null){
                mapWriter = Files.newBufferedWriter(mapOut, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                for (int i = 1; i < vert_ids.length; i++ ){
                    mapWriter.write(String.format("%s", vert_ids[i]));
                    mapWriter.newLine();
                }
            }

            //Each line is [vert size] [edge to vertex id 1] [edge 1 weight] [edge to vertex id 2] [edge 2 weight] ...
            //vertex ID is implicitly line number (starting at 1)
            for(int incrID = 1; incrID < vert_ids.length; incrID++){
                //TODO check on format of weights
                int vert_hash = vert_ids[incrID];
                double vert_weight = m_vertices.get(vert_hash);
                //StringBuilder sb = new StringBuilder(String.format("%s %d", m_vertex_to_name.get(vert_hash), (int)(vert_weight*100)));//TODO casting to int
                StringBuilder sb = new StringBuilder(String.format("%d", (int)(vert_weight*100)));//TODO casting to int
                
                for (Entry<Integer, Double> edge: m_edges.get(vert_hash).entrySet()){
                    int weight = (int) (edge.getValue() * 100);
                    sb.append(String.format(" %d %d", vert_to_increment.get(edge.getKey()),weight ));//TODO casting to int
                }
                writer.write(sb.toString());
                writer.newLine();
            }
            writer.close();
            if (mapWriter!=null) mapWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            Controller.record("Error while opening file " + file.toString());
            System.out.println("Error while opening file " + file.toString());
            return;
       }        
        
    }

    
    public String planToString(){
        return m_plan_handler.toString();
    }
    
    public String[] getTableNames(){
        return m_plan_handler.table_names;
    }
    
    public List<Plan.Range> getAllRanges (String table, int partition){
        return m_plan_handler.getAllRanges(table, partition);
    }
    
    public double getColdTupleWeight (int partition){
        return 1.0 / m_intervalsInSecs[partition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;
    }
    
    public int getHotVertexCount(int partition){
        return m_partitionVertices.get(partition).size();
    }
    
    public PlanHandler clonePlan(){
        return m_plan_handler.clone();
    }
    
    public double getColdIncrement(int fromPartition){
        return 1.0 / m_intervalsInSecs[fromPartition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;
    }
    
    public List<Plan.Range> getRangeValues(String table, int partition, long from, long to){
        return m_plan_handler.getRangeValues(table, partition, from, to);
    }
    
    public void toFile(Path file){
        System.out.println("Writing graph. Number of vertices: " + m_edges.size());
        BufferedWriter writer;
        String s;
        double totalWeight = 0;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(int vertex : m_vertices.keySet()){
                s = "Vertex " + m_vertex_to_name.get(vertex) + " - weight " + m_vertices.get(vertex);
                totalWeight += m_vertices.get(vertex);
                writer.write(s, 0, s.length());
                writer.newLine();
                Int2DoubleMap adjacency = m_edges.get(vertex);
                if(adjacency == null){
                    writer.newLine();
                    continue;
                }
                for (Int2DoubleMap.Entry edge : adjacency.int2DoubleEntrySet()){
                    s = m_vertex_to_name.get(edge.getIntKey()) + " - weight " + edge.getDoubleValue();
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
            Controller.record("Error while opening file " + file.toString());
            System.out.println("Error while opening file " + file.toString());
            return;
       }
    }
    
    public void setPartitionMaps (Int2IntOpenHashMap newVertexPartition){
        
        // reset the mapping from partition to vertices
        int partitionsNo = m_partitionVertices.size();
        for (int i = 0; i < partitionsNo; i++){
            m_partitionVertices.set(i, new IntOpenHashSet());
        }
        
        for (Int2IntMap.Entry entry: newVertexPartition.int2IntEntrySet()){
            
            int vertex = entry.getIntKey();
            int toPartition = entry.getIntValue();
            
            // update plan
            String movedVertexName = m_vertex_to_name.get(vertex);

            String [] fields = movedVertexName.split(",");
            int fromPartition = m_vertexPartition.get(vertex);

            m_plan_handler.removeTupleId(fields[0], fromPartition, Long.parseLong(fields[1]));
            m_plan_handler.addRange(fields[0], toPartition, Long.parseLong(fields[1]), Long.parseLong(fields[1]));

            // update data structures
            IntOpenHashSet vertices = m_partitionVertices.get(toPartition);
            vertices.add(vertex);
        }
        
        // copy new partitioning
        m_vertexPartition = newVertexPartition;
        
    }

    public String verticesToString(IntSet set){
        StringBuilder res = new StringBuilder();
        for (int val : set){
            res.append(m_vertex_to_name.get(val) + "\n");
        }
        return res.toString();
    }
}
