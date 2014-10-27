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
import java.util.HashMap;
import java.util.HashSet;
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
    
    // the internal map is the adjacency list of <toVertex, edgeWeight>
    private Map<String,Map<String,Integer>> edges = new HashMap<String,Map<String,Integer>> ();
    // weights of vertices
    private Map<String,Integer> vertices = new HashMap<String,Integer> ();
    // location of vertices
    private Map<String,Integer> vertexSites = new HashMap<String,Integer> ();
    
    public AffinityGraph (CatalogContext catalogContext, int partitions) throws Exception{
        BufferedReader reader;
        Path logFile;
        
        // get mapping of keys to partitions
        // TODO use parameters instead of having this hardcoded
        File planFile = new File ("plan.json");
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
        // TODO won't work with multiple servers having different sets of partitions
        for (int partition = 0; partition < partitions; partition++){
            System.out.println("Doing partition " + partition);
            logFile = FileSystems.getDefault().getPath(".", "transactions-partition-" + partition + ".log");
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
                                Map<String,Integer> adjacency = edges.get(from.getKey());
                                if(adjacency == null){
                                    adjacency = new HashMap<String,Integer>();
                                    edges.put(from.getKey(), adjacency);
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
                        Integer currentVertexWeight = vertices.get(from.getKey());
                        if (currentVertexWeight == null){
//                            vertices.put(from.getKey(), from.getValue());
                            vertices.put(from.getKey(), 1);
                        }
                        else{
//                            vertices.put(from.getKey(), currentVertexWeight+from.getValue());                            
                            vertices.put(from.getKey(), currentVertexWeight+1);                            
                        }
                        // update locations
                        String[] tupleData = from.getKey().split(",");
                        String table = tupleData[0];
                        String attribute = tupleData[1];
                        Long value = Long.parseLong(tupleData[2]);
                        // TODO how to handle multi-column partitioning attributes? need to talk to Aaron and Becca
                        int partitionId = p.getPartitionId(table, new Long[] {value});
//                        System.out.println("Tuple " + from.getKey() + " belongs to partition " + partitionId);
                        // TODO assuming that sites get partition IDs in order
                        vertexSites.put(from.getKey(), partitionId % Controller.PARTITIONS_PER_SITE);
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
    
    public void toFile(){
        System.out.println("Writing graph. Size of edges: " + edges.size());
        Path graphFile = FileSystems.getDefault().getPath(".", "graph.log");
        BufferedWriter writer;
        String s;
        try {
            writer = Files.newBufferedWriter(graphFile, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            for(String vertex : vertices.keySet()){
                s = "Vertex " + vertex + " - weight " + vertices.get(vertex);
                writer.write(s, 0, s.length());
                writer.newLine();
                Map<String,Integer> adjacency = edges.get(vertex);
                if(adjacency == null) continue;
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
            LOG.warn("Error while opening file " + graphFile.toString());
            System.out.println("Error while opening file " + graphFile.toString());
            return;
       }
    }
}
