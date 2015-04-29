package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.util.List;
import java.util.Map;

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
            @SuppressWarnings("unused")
            String attribute = vertexData[1];
            Long value = Long.parseLong(vertexData[2]);
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
    
    public boolean isActive(int partition){
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
        PlanHandler cloned = new PlanHandler(m_planFile, m_catalogContext);
        
        cloned.m_partitioner = m_partitioner;
        cloned.m_planFile = m_planFile;
        cloned.m_catalogContext = m_catalogContext;
        
        for(String table : table_names){
            
            for (int i = 0; i < Controller.MAX_PARTITIONS; i++){
                cloned.addPartition(table, i);
            }
            
            Map<Integer, List<Plan.Range>> ranges = getAllRanges(table);
            for(Integer i : ranges.keySet()) {
                List<Plan.Range> partitionRanges = ranges.get(i);
                for(Plan.Range range : partitionRanges) {
                    cloned.addRange(table, i, range.from, range.to);
                }
            }
        }
        return cloned;
    }
}
