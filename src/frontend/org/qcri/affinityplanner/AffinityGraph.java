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
  
    private static final Logger LOG = Logger.getLogger(AffinityGraph.class);

    // determines granularity of edges, either vertex - vertex of vertex - partition 
    private final boolean m_tupleGranularity;
    
    // fromVertex -> adjacency list, where adjacency list is a toVertex -> edgeWeight map or toPartition -> edgeWeight map, depending on granularity
    protected Map<String,Map<String,Double>> m_edges = new HashMap<String,Map<String,Double>> ();

    // vertex -> weight map
    final Map<String,Double> m_vertices = new HashMap<String,Double> ();
    // partition -> vertex and vertex -> partition mappings
    final List<Set<String>> m_partitionVertices = new ArrayList<Set<String>> ();
    final Map<String,Integer> m_vertexPartition = new HashMap<String,Integer> ();
    
    private PlanHandler m_plan_handler = null;
    
    public AffinityGraph(boolean tupleGranularity, CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles, int noPartitions) throws Exception {
        m_tupleGranularity = tupleGranularity;

        BufferedReader reader;
        for (int i = 0; i < noPartitions; i++){
            m_partitionVertices.add(new HashSet<String>());
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
            HashMap<String, Set<String>> transactions = new HashMap<String,Set<String>>();
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
                            partition = m_plan_handler.getPartition(from);

                            if (partition == HStoreConstants.NULL_PARTITION_ID){
                                LOG.info("Exiting graph loading. Could not find partition for key " + from);
                                System.out.println("Exiting graph loading. Could not find partition for key " + from);
                                throw new Exception();                            
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
                                    
                                    // if lower granularity, edges link vertices to partitions, not other vertices
                                    if(!m_tupleGranularity){
                                        int toPartition = m_plan_handler.getPartition(to);
                                        to = Integer.toString(toPartition);
                                    }
                                    
                                    Double currentEdgeWeight = adjacency.get(to);
                                    if (currentEdgeWeight == null){
                                        adjacency.put(to, normalizedIncrement);
                                    }
                                    else{
                                        adjacency.put(to, currentEdgeWeight + normalizedIncrement);
                                    }
                                }
                            } // END for(String to : curr_transaction)
                            
                        } // END for(String from : curr_transaction)
                        
                        transactions.remove(transaction_id);
                        
    //                    System.out.println("Tran ID = " + currTransactionId);
                    } // END if(curr_transaction != null)
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
    
    public void moveVertices(Set<String> movedVertices, int fromPartition, int toPartition) {
        for (String movedVertex : movedVertices){
            m_partitionVertices.get(fromPartition).remove(movedVertex);
            m_partitionVertices.get(toPartition).add(movedVertex);
            m_vertexPartition.put(movedVertex, toPartition);

            // update plan too
            // format of vertices is <TABLE>,<PART-KEY>,<VALUE>
            String [] fields = movedVertex.split(",");
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
    
    public int getPartition(String vertex){
        return m_plan_handler.getPartition(vertex);
    }
    
    public void planToJSON(String newPlanFile){
        m_plan_handler.toJSON(newPlanFile);
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
            Controller.record("Error while opening file " + file.toString());
            System.out.println("Error while opening file " + file.toString());
            return;
       }
    }
}
