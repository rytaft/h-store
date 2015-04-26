package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.qcri.affinityplanner.Plan;
import org.voltdb.CatalogContext;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class GreedyExtendedPartitioner {

    // tuple -> weight
    protected static final Int2DoubleOpenHashMap m_hotTuples = new Int2DoubleOpenHashMap ();

    // tuple -> partition
    protected static final Int2ObjectOpenHashMap<IntArrayList> m_partitionToHotTuples = new Int2ObjectOpenHashMap<IntArrayList> ();

    // vertex -> full name
    protected static Int2ObjectOpenHashMap<String> m_tupleToName = new Int2ObjectOpenHashMap <String> ();

    private static long[] m_intervalsInSecs;
    private static PlanHandler m_plan_handler = null;

    public GreedyExtendedPartitioner (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles, double normalizedIncrement) {

        try {
            m_plan_handler = new PlanHandler(planFile, catalogContext);
        } catch (Exception e) {
            Controller.record("Could not create plan handler " + Controller.stackTraceToString(e));
            throw e;
        }

        try {
            loadLogFile(logFiles, intervalFiles, normalizedIncrement);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void loadLogFile(Path[] logFiles, Path[] intervalFiles, double normalizedIncrement) throws Exception{

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
                    IntArrayList tuples = m_partitionToHotTuples.get(partition);
                    if (tuples == null){
                        tuples = new IntArrayList();
                        m_partitionToHotTuples.put(partition, tuples);
                    }
                    tuples.add(hash);
                }
            }
        }
    }

    public boolean repartition(){

        IntArrayList activePartitions = new IntArrayList();
        DoubleArrayList partitionsFromLeastLoaded = new DoubleArrayList();
        IntArrayList overloadedPartitions = new IntArrayList();

        System.out.println("Load per partition before reconfiguration");
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(m_plan_handler.isActive(i)){
                activePartitions.add(i);
                double load =  getLoadPerPartition(i);
                partitionsFromLeastLoaded.add(load);
                System.out.println(load);
                if (load > Controller.MAX_LOAD_PER_PART){
                    overloadedPartitions.add(i);
                }
            }
        }

        Collections.sort(partitionsFromLeastLoaded);
        
        System.out.println("Move hot tuples");

        for (int overloadedPartition : overloadedPartitions){

            IntArrayList fromHotTuples = m_partitionToHotTuples.get(overloadedPartition);

            // sort determines an _ascending_ order
            // Comparator should return "a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second"
            // We want a _descending_ order, so we need to invert the comparator result
            Collections.sort(fromHotTuples, new AbstractIntComparator (){
                @Override
                public int compare(int o1, int o2) {
                    if (m_hotTuples.get(o1) < m_hotTuples.get(o2)){
                        return 1;
                    }
                    else if (m_hotTuples.get(o1) > m_hotTuples.get(o2)){
                        return -1;
                    }
                    return 0;
                }
            });

            // MOVE HOT TUPLES

            int currHotTuplePos = 0;

            while(getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                int currHotTuple = fromHotTuples.getInt(currHotTuplePos);

                int toPartition = getLeastLoadedPartition(activePartitions);                
                IntList toHotTuples = m_partitionToHotTuples.get(toPartition);

                tryMoveVertices (overloadedPartition, toPartition, fromHotTuples, currHotTuple, toHotTuples);

                ++currHotTuplePos;
            }

            // MOVE COLD CHUNKS
            
            // clone to avoid modifications while iterating
            PlanHandler oldPlan = m_plan_handler.clone();
            
            double coldIncrement = 1.0 / m_intervalsInSecs[overloadedPartition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;

            while (getLoadPerPartition(overloadedPartition) > Controller.MAX_LOAD_PER_PART){

                for(String table : m_plan_handler.table_names){
                    
                    List<List<Plan.Range>> partitionSlices = oldPlan.getRangeSlices(table, overloadedPartition,  (long) Controller.COLD_CHUNK_SIZE);
                    if(partitionSlices.size() > 0) {
                        
                        for(List<Plan.Range> slice : partitionSlices) {  // for each slice - TODO understand what a slice is
                            for(Plan.Range r : slice) { 
                                
                                double chunkWeight = Plan.getRangeWidth(r) * coldIncrement;
                                int toPartition = getLeastLoadedPartition(activePartitions);     

                                if (chunkWeight + getLoadPerPartition(toPartition) < Controller.MAX_LOAD_PER_PART){
    
                                    // do the move
                                    
                                    List<Plan.Range> oldRanges = m_plan_handler.getRangeValues(table, overloadedPartition, r.from, r.to);
    
                                    for(Plan.Range oldRange : oldRanges) {
                                        
                                        // remove chunk from overloaded partition
                                        m_plan_handler.removeRange(table, overloadedPartition, oldRange.from);
    
                                        if(!m_plan_handler.hasPartition(toPartition)) {
                                            m_plan_handler.addPartition(table, toPartition);
                                        }
    
                                        // add chunk to destination partition 
                                        m_plan_handler.addRange(table, toPartition, Math.max(oldRange.from, r.from), Math.min(oldRange.to, r.to));
    
                                        // add back range minus chunk to overloaded partition
                                        if(oldRange.from < r.from) {
                                            m_plan_handler.addRange(table, overloadedPartition, oldRange.from, r.from - 1);
                                        }
                                        if(r.to < oldRange.to) {
                                            m_plan_handler.addRange(table, overloadedPartition, r.to + 1, oldRange.to);
                                        }
                                    }
                                }
                                else{
                                    System.out.println("Cannot offload partition " + overloadedPartition);
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return true;
    }

    public double getLoadPerPartition(int partition){
        double load = 0;

        for (int tuple : m_partitionToHotTuples.get(partition)){
            load += m_hotTuples.get(tuple);
        }

        double coldIncrement = 1.0 / m_intervalsInSecs[partition] / Controller.COLD_TUPLE_FRACTION_ACCESSES;
        load += (Controller.COLD_CHUNK_SIZE - m_partitionToHotTuples.get(partition).size()) * coldIncrement;

        return load;
    }

    public int getLeastLoadedPartition(IntList activePartitions){
        double minLoad = Double.MAX_VALUE;
        int res = 0;
        for (int part : activePartitions){
            if (getLoadPerPartition(part) < minLoad){
                res = part;
            }
        }
        return res;
    }

    private void tryMoveVertices (int fromPartition, int toPartition, IntList fromHotTuples, int currHotTuple, IntList toHotTuples){
        fromHotTuples.remove(currHotTuple);
        toHotTuples.add(currHotTuple);

        if (getLoadPerPartition(toPartition) > Controller.MAX_LOAD_PER_PART){
            fromHotTuples.add(currHotTuple);
            toHotTuples.remove(currHotTuple);
        }
        else{
            // actually move tuple in plan

            String movedVertexName = m_tupleToName.get(currHotTuple);
            String [] fields = movedVertexName.split(",");
            //            System.out.println("table: " + fields[0] + " from partition: " + fromPartition + " to partition " + toPartition);
            //            System.out.println("remove ID: " + fields[2]);
            m_plan_handler.removeTupleId(fields[0], fromPartition, Long.parseLong(fields[2]));
            //            System.out.println("After removal");
            //            System.out.println(m_plan_handler.toString() + "\n");
            m_plan_handler.addRange(fields[0], toPartition, Long.parseLong(fields[2]), Long.parseLong(fields[2]));
        }
    }
}
