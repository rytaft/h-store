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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.hstore.HStoreConstants;

public class AffinityGraph {
  
    private static final Logger LOG = Logger.getLogger(AffinityGraph.class);

    // determines granularity of edges, either vertex - vertex of vertex - partition 
    private final boolean m_tupleGranularity;
    
    // fromVertex -> adjacency list, where adjacency list is a toVertex -> edgeWeight map or toPartition -> edgeWeight map, depending on granularity
    protected Int2ObjectOpenHashMap<Int2DoubleOpenHashMap> m_edges = new Int2ObjectOpenHashMap <Int2DoubleOpenHashMap> ();
    
    // vertex -> full name
    protected Int2ObjectOpenHashMap<String> m_vertex_to_name = new Int2ObjectOpenHashMap <String> ();

    // vertex -> weight map
    final Int2DoubleOpenHashMap m_vertices = new Int2DoubleOpenHashMap (1000);
    // partition -> vertex and vertex -> partition mappings
    final List<IntOpenHashSet> m_partitionVertices = new ArrayList<IntOpenHashSet> ();
    final Int2IntOpenHashMap m_vertexPartition = new Int2IntOpenHashMap ();
    
    private PlanHandler m_plan_handler = null;
    
    public AffinityGraph(boolean tupleGranularity, CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles, int noPartitions) throws Exception {
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
        long[] intervalsInSecs = new long[intervalFiles.length];
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
                intervalsInSecs[currInterval] = Long.parseLong(line) / 1000;
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
        int currLogFile = 0;
        for (Path logFile : logFiles){
            
            System.out.println("Loading from file " + logFile);

            // SETUP
            double normalizedIncrement = 1.0/intervalsInSecs[currLogFile];
            currLogFile ++;
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
            IntOpenHashSet curr_transaction = new IntOpenHashSet ();
            // PROCESS LINE
            while(line != null){

                // if finished with one transaction, update graph and clear before moving on

                if (line.equals("END")){

                    for(int from : curr_transaction){
                        // update FROM vertex in graph
                        System.out.println("From -- " + from);
                        double currentVertexWeight = m_vertices.get(from);
                        if (currentVertexWeight == 0){
                            m_vertices.put(from, normalizedIncrement);
                        }
                        else{
                            m_vertices.put(from, currentVertexWeight + normalizedIncrement);                         
                        }
                        // store site mappings for FROM vertex
                        int partition = 0;
                        String fromName = m_vertex_to_name.get(from);
                        partition = m_plan_handler.getPartition(fromName);
                        
                        if (partition == HStoreConstants.NULL_PARTITION_ID){
                            LOG.info("Exiting graph loading. Could not find partition for key " + from);
                            System.out.println("Exiting graph loading. Could not find partition for key " + from);
                            throw new Exception();                            
                        }
                        m_partitionVertices.get(partition).add(from);
                        m_vertexPartition.put(from, partition);

                        Int2DoubleOpenHashMap adjacency = m_edges.get(from);
                        if(adjacency == null){
                            adjacency = new Int2DoubleOpenHashMap ();
                            m_edges.put(from, adjacency);
                        }

                        // update FROM -> TO edges
                        for(int to : curr_transaction){
                            if (from != to){
                                // if lower granularity, edges link vertices to partitions, not other vertices
                                if(!m_tupleGranularity){
                                    String toName = m_vertex_to_name.get(to);
                                    to = m_plan_handler.getPartition(toName);
                                }

                                double currentEdgeWeight = adjacency.get(to);
                                if (currentEdgeWeight == 0){
                                    adjacency.put(to, normalizedIncrement);
                                }
                                else{
                                    adjacency.put(to, currentEdgeWeight + normalizedIncrement);
                                }
                            }
                        } // END for(String to : curr_transaction)

                    } // END for(String from : curr_transaction)

                    curr_transaction.clear();

                    //                    System.out.println("Tran ID = " + currTransactionId);
                } // END if (line.equals("END"))

                else{
                    curr_transaction.add(line.hashCode());
                    if (!m_vertex_to_name.containsKey(line.hashCode())){
                        m_vertex_to_name.put(line.hashCode(), line);
                    }
                }

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
        } // END for (Path logFile : logFiles)
        
    }
    
    public void moveVertices(Set<Integer> movedVertices, int fromPartition, int toPartition) {
        for (int movedVertex : movedVertices){
            m_partitionVertices.get(fromPartition).remove(movedVertex);
            m_partitionVertices.get(toPartition).add(movedVertex);
            m_vertexPartition.put(movedVertex, toPartition);

            // update plan too
            // format of vertices is <TABLE>,<PART-KEY>,<VALUE>
            String movedVertexName = m_vertex_to_name.get(movedVertex);
            String [] fields = movedVertexName.split(",");
//            System.out.println("table: " + fields[0] + " from partition: " + fromPartition + " to partition " + toPartition);
//            System.out.println("remove ID: " + fields[2]);
            m_plan_handler.removeTupleId(fields[0], fromPartition, Long.parseLong(fields[2]));
//            System.out.println("After removal");
//            System.out.println(m_plan_handler.toString() + "\n");
            m_plan_handler.addRange(fields[0], toPartition, Long.parseLong(fields[2]), Long.parseLong(fields[2]));
//            System.out.println("After adding");
//            System.out.println(m_plan_handler.toString() + "\n");
        }
        
//        System.out.println(m_plan_handler.toString() + "\n");
    }
    
    public int getPartition(Integer vertex){
        String vertexName = m_vertex_to_name.get(vertex);
        return m_plan_handler.getPartition(vertexName);
    }
    
    public void planToJSON(String newPlanFile){
        m_plan_handler.toJSON(newPlanFile);
    }

    public void toFile(Path file){
        System.out.println("Writing graph. Number of vertices: " + m_edges.size());
        BufferedWriter writer;
        String s;
        double totalWeight = 0;
        try {
            writer = Files.newBufferedWriter(file, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(Integer vertex : m_vertices.keySet()){
                s = "Vertex " + m_vertex_to_name.get(vertex) + " - weight " + m_vertices.get(vertex);
                totalWeight += m_vertices.get(vertex);
                writer.write(s, 0, s.length());
                writer.newLine();
                Map<Integer,Double> adjacency = m_edges.get(vertex);
                if(adjacency == null){
                    writer.newLine();
                    continue;
                }
                for (Map.Entry<Integer, Double> edge : adjacency.entrySet()){
                    s = m_vertex_to_name.get(edge.getKey()) + " - weight " + edge.getValue();
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
}
