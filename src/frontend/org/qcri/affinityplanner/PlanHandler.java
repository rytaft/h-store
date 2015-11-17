package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.CatalogContext;

import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.TwoTieredRangePartitions;
import edu.brown.utils.FileUtil;

/*
 * Keeps mappings of keys to partitions and of partitions to sites 
 * For the moment, it encapsulates all the hard-coded mappings we use
 */

public class PlanHandler extends Plan {
    
    private ExplicitPartitions m_partitioner;
    
    private File m_planFile;
    private CatalogContext m_catalogContext;

    public PlanHandler(File planFile, CatalogContext catalogContext) {
        super(planFile.toString());
        
        m_planFile = planFile;
        m_catalogContext = catalogContext;
        
        // get mapping of keys to partitions
        JSONObject json = null;
        try {
            json = new JSONObject(FileUtil.readFile(planFile.getAbsolutePath()));
        } catch (JSONException e) {
            System.out.println("Problem while reading JSON file with the plan");
            System.out.println("Stack trace:\n" + Controller.stackTraceToString(e));
           System.exit(1);
        }
        try {
            m_partitioner = new TwoTieredRangePartitions(catalogContext, json);
        } catch (Exception e) {
            System.out.println("Problem while creating the partitioner");
            System.out.println("Stack trace:\n" + Controller.stackTraceToString(e));
            System.exit(1);
        }
        try {
            m_partitioner.setPartitionPlan(planFile);
        } catch (Exception e) {
            System.out.println("Problem while setting the partition plan");
            System.out.println("Stack trace:\n" + Controller.stackTraceToString(e));
            System.exit(1);
        }
    }
    
    // used only for cloning
    public PlanHandler(){
        super ();
    }

    /*
     * Returns the partition of a vertex
     * vertex is specified as "TABLE_ID,ATTRIBUTE_NAME,VALUE"
     * 
     * *******ASSUMPTIONS (TODO)********
     * - Sites get partition IDs in order, ie., site 0 takes 0,..,N-1, site 1 takes N,...,2N-1 etc.
     * - Does not handle multi-column partitioning attributes. This will depend on the output of monitoring for that case
     */
    public int getPartition(String vertex) {
        try{
            String[] vertexData = vertex.split(",");
            String table = vertexData[0];
            Long value = Long.parseLong(vertexData[1]);
            int partitionId = m_partitioner.getPartitionId(table, new Long[] {value});
    //        System.out.println("Vertex " + from.getKey() + " belongs to partition " + partitionId);
            return partitionId;
        } catch (Exception e) {
            System.out.println("Could not get partition from plan handler " + Controller.stackTraceToString(e));
            System.exit(-1);                      
            return -1;
        }
    }
    
    public static int getSitePartition(int partitionId){
        return partitionId / Controller.PARTITIONS_PER_SITE;
    }

    /*
     * Returns the site of a vertex
     * vertex is specified as "TABLE_ID,ATTRIBUTE_NAME,VALUE"
     * 
     * *******ASSUMPTIONS (TODO)********
     * - Sites get partition IDs in order, ie., site 0 takes 0,..,N-1, site 1 takes N,...,2N-1 etc.
     * - Does not handle multi-column partitioning attributes. This will depend on the output of monitoring for that case
     */
    public int getSiteVertex(String vertex) throws Exception{
        return getSitePartition(getPartition(vertex));
    }
    
    public static IntList getPartitionsSite(int site){
        IntList res = new IntArrayList (Controller.PARTITIONS_PER_SITE);
        for (int i = site * Controller.PARTITIONS_PER_SITE; i < (site + 1) * Controller.PARTITIONS_PER_SITE; i++){
            res.add(i);
        }
        return res;
    }
    
    public boolean isNotEmpty(int partition){
        for(String table: table_names){
            List<Range> ranges = getAllRanges(table, partition);
            if (ranges != null && !ranges.isEmpty()){
                return true;
            }
        }
        return false;
    }
    
    // adapted from Becca's GreedyExtendedPlacement
    public PlanHandler clone(){
        
        PlanHandler cloned = new PlanHandler();
        
        cloned.m_partitioner = this.m_partitioner;
        cloned.m_planFile = this.m_planFile;
        cloned.m_catalogContext = this.m_catalogContext;
                
        cloned.table_names = this.table_names;
        cloned.m_defaultTable = this.m_defaultTable;
        
        cloned.tableToPartitionsToRanges = new HashMap<String, HashMap<Integer, TreeMap<Long,Long>>> ();
        
        for(String table : table_names){
            
            HashMap<Integer, TreeMap<Long,Long>> clonedPartitionToRanges = new HashMap<Integer, TreeMap<Long,Long>>();
            cloned.tableToPartitionsToRanges.put(table, clonedPartitionToRanges);

            Map<Integer, TreeMap<Long,Long>> partitionsToRanges = tableToPartitionsToRanges.get(table.toLowerCase());
            
            for (Map.Entry<Integer, TreeMap<Long,Long>> partititionToRanges :  partitionsToRanges.entrySet()){
                
                TreeMap<Long,Long> clonedRanges = new TreeMap<Long,Long>();
                clonedPartitionToRanges.put(partititionToRanges.getKey(), clonedRanges);
                
                TreeMap<Long,Long> ranges = partititionToRanges.getValue();
                for (Map.Entry<Long, Long> range : ranges.entrySet()){
                    clonedRanges.put(range.getKey(), range.getValue());
                }
            }
        }
        return cloned;
    }
    
    public boolean verifyPlan(){
        
        boolean res = true;
        
        for (String table : table_names){
            
            TreeMap<Long,Long> allRanges = new TreeMap<Long,Long> ();

            // merge ranges from all tuples
            for (TreeMap<Long,Long> ranges : tableToPartitionsToRanges.get(table).values()){
                for (Map.Entry<Long, Long> range : ranges.entrySet()){
                    allRanges.put(range.getKey(), range.getValue());
                }
            }
            
            Long currVal = 0L;
            for (Map.Entry<Long, Long> range : allRanges.entrySet()){
                if (range.getKey().equals(currVal)){
                    currVal = range.getValue() + 1;
                }
                else{
                    System.out.println("Table " + table + " misses id " + currVal + " next range " + range.getKey() + "-" + range.getValue());
                    currVal = range.getValue() + 1;
                    res = false;
                }
            }
        }
        return res;
    }
    
    // writes all partitions that have been moved
    public void printDataMovementsTo(PlanHandler other){
        
        Map<String,Long> moveCounts = new HashMap<String,Long> (this.table_names.length);
        
        for(String table : this.table_names){
            
//            System.out.println("Table " + table);
            
            Map<Integer, TreeMap<Long,Long>> thisPartitionToRanges = this.tableToPartitionsToRanges.get(table.toLowerCase());
            
            for (Integer partition : thisPartitionToRanges.keySet()){
                
//                System.out.println("Partition " + partition);
                
                List<Range> thisRanges = this.getAllRanges(table, partition);
                
                for(Range tr : thisRanges){
                    
//                    System.out.println("Changes to range " + tr.from + "," + tr.to);
                    
                    List<Range> otherIntersectingRanges = other.getRangesOverlappingValues(table, partition, tr.from, tr.to);
                    
                    if(otherIntersectingRanges == null || otherIntersectingRanges.size() == 0){

                        Long currCount = moveCounts.get(table);
                        if (currCount == null){
                            currCount = 0L;
                        }
                        moveCounts.put(table, currCount + tr.to - tr.from + 1);

//                        System.out.println("Removed the whole range");
                    }
                    else{
                        long size_tr = tr.to - tr.from + 1;
                        long size_overlaps_other = 0;
                        
                        for(Range or : otherIntersectingRanges){
                            long overlap_from = Math.max(or.from, tr.from);
                            long overlap_to = Math.min(or.to, tr.to);
                            size_overlaps_other += overlap_to - overlap_from + 1;
                            
//                            System.out.println("Overlap with other range " + or.from + "," + or.to + " is " + overlap_from  + "," + overlap_to);
                        }
                        
                        Long currCount = moveCounts.get(table);
                        if (currCount == null){
                            currCount = 0L;
                        }
                        moveCounts.put(table, currCount + size_tr - size_overlaps_other);                        
                    }
                }
            }
        }
        
        System.out.println("Data movements counts: ");
        for (Map.Entry<String,Long> entry : moveCounts.entrySet()){
            System.out.println("Table " + entry.getKey() + ": " + entry.getValue());
        }
    }
}
