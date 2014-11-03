package org.qcri.affinityplanner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

public class PlanHandler {
    
    ExplicitPartitions m_partitioner;

    public PlanHandler(File planFile, CatalogContext catalogContext) throws Exception{
        // get mapping of keys to partitions
        JSONObject json;
        try {
            json = new JSONObject(FileUtil.readFile(planFile.getAbsolutePath()));
        } catch (JSONException e1) {
            e1.printStackTrace();
            System.out.println("Problem while reading JSON file with the plan");
            throw e1;
        }
        try {
            m_partitioner = new TwoTieredRangePartitions(catalogContext, json);
        } catch (Exception e1) {
            e1.printStackTrace();
            System.out.println("Problem while creating the partitioner");
            throw e1;
        }
        m_partitioner.setPartitionPlan(planFile);
    }

    /*
     * Returns the partition of a vertex
     * vertex is specified as "TABLE_ID,ATTRIBUTE_NAME,VALUE"
     * 
     * *******ASSUMPTIONS (TODO)********
     * - Sites get partition IDs in order, ie., site 0 takes 0,..,N-1, site 1 takes N,...,2N-1 etc.
     * - Does not handle multi-column partitioning attributes. This will depend on the output of monitoring for that case
     */
    public int getPartition(String vertex) throws Exception{
        String[] vertexData = vertex.split(",");
        String table = vertexData[0];
        String attribute = vertexData[1];
        Long value = Long.parseLong(vertexData[2]);
        int partitionId = m_partitioner.getPartitionId(table, new Long[] {value});
//        System.out.println("Vertex " + from.getKey() + " belongs to partition " + partitionId);
        return partitionId;
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
    
    public static List<Integer> getPartitionsSite(int site){
        ArrayList<Integer> res = new ArrayList<Integer> (Controller.PARTITIONS_PER_SITE);
        for (int i = site * Controller.PARTITIONS_PER_SITE; i < (site + 1) * Controller.PARTITIONS_PER_SITE; i++){
            res.add(i);
        }
        return res;
    }
}
