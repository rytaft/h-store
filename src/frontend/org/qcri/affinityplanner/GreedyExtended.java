package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.voltdb.CatalogContext;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class GreedyExtended implements Partitioner {

    // tuple -> weight
    protected static final Int2DoubleOpenHashMap m_hotTuples = new Int2DoubleOpenHashMap ();

    // tuple -> partition
    protected static final IntList[] m_partitionToHotTuples = new IntList[Controller.MAX_PARTITIONS];

    // vertex -> full name
    protected static Int2ObjectOpenHashMap<String> m_tupleToName = new Int2ObjectOpenHashMap <String> ();

    private static long[] m_intervalsInSecs;
    private static PlanHandler m_plan_handler = null;

    public GreedyExtended (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles) {

        try {
            m_plan_handler = new PlanHandler(planFile, catalogContext);
        } catch (Exception e) {
            Controller.record("Could not create plan handler " + Controller.stackTraceToString(e));
            throw e;
        }

        try {
            loadLogFile(logFiles, intervalFiles);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void loadLogFile(Path[] logFiles, Path[] intervalFiles) throws Exception{

        IntSet[] partitionToHotTuplesSet = new IntSet[Controller.MAX_PARTITIONS];

        // read monitoring intervals for all sites - in seconds
        m_intervalsInSecs = new long[intervalFiles.length];
        int currInterval = 0;
        for (Path intervalFile : intervalFiles){
            // SETUP
            BufferedReader reader;

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

        int nextLogFile = 0;
        for(Path logFile : logFiles){
            System.out.println("Loading from file " + logFile);

            double increment = 1.0/m_intervalsInSecs[nextLogFile];
            ++nextLogFile;

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

            // PROCESS LINE
            while(line != null){
                if (!line.equals("END")){
                    int hash = line.hashCode();
                    double currWeight = m_hotTuples.get(hash);
                    if(currWeight == m_hotTuples.defaultReturnValue()){
                        m_hotTuples.put(hash, increment);
                    }
                    else{
                        m_hotTuples.put(hash, currWeight + increment);
                    }
                    m_tupleToName.put(line.hashCode(), line);
                    int partition = m_plan_handler.getPartition(line);
                    IntSet tuples = partitionToHotTuplesSet[partition];
                    if (tuples == null){
                        tuples = new IntOpenHashSet();
                        partitionToHotTuplesSet[partition] = tuples;
                    }
                    tuples.add(hash);
                }
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                    throw e;
                }
            }
        }

        for (int part = 0; part < Controller.MAX_PARTITIONS; part ++){
            m_partitionToHotTuples[part] = new IntArrayList(partitionToHotTuplesSet[part]);
        }
    }

    public boolean repartition(){

        int addedPartitions = 0;

        IntArrayList activePartitions = new IntArrayList();
        IntArrayList overloadedPartitions = new IntArrayList();
        
        int numMovedVertices = 0;

        System.out.println("Load per partition before reconfiguration");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(m_plan_handler.isActive(i)){
                activePartitions.add(i);
                double load =  getLoadPerPartition(i);
                System.out.println(load);
                if (load > Controller.MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        for (int overloadedPartition : overloadedPartitions){

            System.out.println("Offloading partition " + overloadedPartition);
            
            // loops over multiple added partitions
            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                IntList fromHotTuples = m_partitionToHotTuples[overloadedPartition];
    
                System.out.println("Got hot tuples ");
    
                // sort determines an _ascending_ order
                // then offload starting from end of the list in order to facilitate removal
                Collections.sort(fromHotTuples, new AbstractIntComparator (){
                    @Override
                    public int compare(int o1, int o2) {
                        if (m_hotTuples.get(o1) < m_hotTuples.get(o2)){
                            return -1;
                        }
                        else if (m_hotTuples.get(o1) > m_hotTuples.get(o2)){
                            return 1;
                        }
                        return 0;
                    }
                });
    
                System.out.println("Move hot tuples");
    
                // MOVE HOT TUPLES
    
                int topK = Controller.TOPK;
    
                // loops over multiple hot tuples
                while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART 
                        && !fromHotTuples.isEmpty() && topK > 0){
    
                    --topK;
    
                    int toPartition = getLeastLoadedPartition(activePartitions);                
                    IntList toHotTuples = m_partitionToHotTuples[toPartition];
    
                    int currHotTuple = fromHotTuples.getInt(fromHotTuples.size() - 1);
    
                    System.out.println("\nTuple name " + m_tupleToName.get(currHotTuple) + ", id " + currHotTuple + ", and weight " + m_hotTuples.get(currHotTuple));
    
                    toHotTuples.add(currHotTuple);
    
                    System.out.println("Load " + getLoadPerPartition(overloadedPartition));
    
                    if (getLoadPerPartition(toPartition) > Controller.MAX_LOAD_PER_PART){
    
                        toHotTuples.remove(toHotTuples.size()-1);
    
                    }
                    else{
                        // actually move tuple in plan
    
                        ++numMovedVertices;
                        
                        System.out.println("Tuple moved");
                        
                        String movedVertexName = m_tupleToName.get(currHotTuple);
    
                        fromHotTuples.removeInt(fromHotTuples.size() - 1);
    
                        String [] fields = movedVertexName.split(",");
                        //            System.out.println("table: " + fields[0] + " from partition: " + fromPartition + " to partition " + toPartition);
                        //            System.out.println("remove ID: " + fields[2]);
                        m_plan_handler.removeTupleId(fields[0], overloadedPartition, Long.parseLong(fields[2]));
                        //            System.out.println("After removal");
                        //            System.out.println(m_plan_handler.toString() + "\n");
                        m_plan_handler.addRange(fields[0], toPartition, Long.parseLong(fields[2]), Long.parseLong(fields[2]));
                    }
    
                } // while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART && !fromHotTuples.isEmpty() && topK > 0)
    
                System.out.println("Current load " + getLoadPerPartition(overloadedPartition));
                System.out.println("Current plan\n " + m_plan_handler);
    
                // MOVE COLD CHUNKS
                System.out.println("\nMove cold chunks\n");
    
                numMovedVertices = moveColdChunks(overloadedPartition, activePartitions, numMovedVertices); 
    
                // ADD NEW PARTITIONS if needed
                if (getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){
                    
                    if(activePartitions.size() < Controller.MAX_PARTITIONS 
                            && addedPartitions < Controller.MAX_PARTITIONS_ADDED){
    
                        // We fill up low-order partitions first to minimize the number of servers
                        addedPartitions++;
                        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                            if(!activePartitions.contains(i)){
                                activePartitions.add(i);
                                break;
                            }
                        }
                    }
                    else{
                        System.out.println("Cannot add new partition to offload " + overloadedPartitions);
                        return false;
                    }
    
                }
            } // while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART)
        } // for (int overloadedPartition : overloadedPartitions)

        return true;
    }

    public double getLoadPerPartition(int partition){
        double load = 0;

        for (int tuple : m_partitionToHotTuples[partition]){
            load += m_hotTuples.get(tuple);
        }

        long countColdTuples = 0;
        for(String table : m_plan_handler.table_names){

            List<Plan.Range> partitionRanges = m_plan_handler.getAllRanges(table, partition);
            if(partitionRanges.size() > 0) {

                for(Plan.Range r : partitionRanges) {
                    countColdTuples += r.to - r.from + 1;
                }
            }
        }

        double coldIncrement = 1.0 / m_intervalsInSecs[partition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;
        load += countColdTuples * coldIncrement;

        return load;
    }

    public int getLeastLoadedPartition(IntList activePartitions){
        double minLoad = Double.MAX_VALUE;
        int res = 0;
        for (int part : activePartitions){
            double newLoad = getLoadPerPartition(part);
            if (newLoad < minLoad){
                res = part;
                minLoad = newLoad; 
            }
        }
        return res;
    }

    @Override
    public void writePlan(String plan_out) {
        m_plan_handler.toJSON(plan_out);
    }

    // made a procedure so it is easier to stop when we are done
    private int moveColdChunks(int overloadedPartition, IntList activePartitions, int numMovedVertices){

        // clone plan to allow modifications while iterating on the clone
        PlanHandler oldPlan = m_plan_handler.clone();

        System.out.println("Cloned plan\n" + oldPlan);

        double coldIncrement = 1.0 / m_intervalsInSecs[overloadedPartition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;

        while (getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

            for(String table : m_plan_handler.table_names){

                System.out.println("Table " + table);

                List<List<Plan.Range>> partitionChunks = oldPlan.getRangeChunks(table, overloadedPartition,  (long) Controller.COLD_CHUNK_SIZE);
                if(partitionChunks.size() > 0) {

                    for(List<Plan.Range> chunk : partitionChunks) {  // a chunk can consist of multiple ranges if hot tuples are taken away

                        System.out.println("\nNew chunk");

                        for(Plan.Range r : chunk) { 

                            System.out.println("Range " + r.from + " " + r.to);

                            double rangeWeight = Plan.getRangeWidth(r) * coldIncrement;
                            int toPartition = getLeastLoadedPartition(activePartitions);     

                            System.out.println("Weight " + coldIncrement);
                            System.out.println("To partition " + toPartition);

                            if (rangeWeight + getLoadPerPartition(toPartition) < Controller.MAX_LOAD_PER_PART
                                   && numMovedVertices + Plan.getRangeWidth(r) < Controller.MAX_MOVED_TUPLES_PER_PART){

                                // do the move
                                
                                numMovedVertices += Plan.getRangeWidth(r);

                                System.out.println("Moving!");
                                System.out.println("Load before " + getLoadPerPartition(overloadedPartition));

                                m_plan_handler.moveColdRange(table, r, overloadedPartition, toPartition);

                                System.out.println("Load after " + getLoadPerPartition(overloadedPartition));
                                System.out.println("New plan\n" + m_plan_handler);

                                // after every move, see if I can stop
                                if(getLoadPerPartition(overloadedPartition) <= Controller.MAX_LOAD_PER_PART){
                                    return numMovedVertices;
                                }
                            }
                            else{
                                System.out.println("Cannot offload partition " + overloadedPartition);
                                return numMovedVertices;
                            }
                        }
                    }
                }
            }
        }
        return numMovedVertices;
    }
}
