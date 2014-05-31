package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.hashing.PlannedPartitions.PartitionPhase;

public interface ExplicitPartitions {

    /**
     * Get the explicit partitioned tables and ensure that the old plan and the new plan have the
     * same set of tables
     * 
     * @param partition_json
     * @return the set of tables in the partition plan
     */
    public abstract Set<String> getExplicitPartitionedTables(JSONObject partition_json);

    /**
     * Get the partition id for a given table and partition id/key
     * 
     * @param table_name
     * @param id
     * @return the partition id, or -1 / null partition if the id/key is not
     *         found in the plan
     * @throws Exception
     */
    public abstract int getPartitionId(String table_name, List<Object> ids) throws Exception;
    
    public abstract int getPartitionId(String table_name, Object[] ids) throws Exception;
    
    public abstract int getPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception;

    public abstract String getTableName(CatalogType catalog);
    
    public abstract String getTableName(List<CatalogType> catalog);
    
    public abstract Table getTable(List<CatalogType> catalog);

    /**
     * Get the previous partition id for a given table and partition id/key
     * 
     * @param table_name
     * @param id
     * @return the partition id, or -1 / null partition if the id/key is not
     *         found in the plan OR if there is no previous plan
     * @throws Exception
     */
    public abstract int getPreviousPartitionId(String table_name, List<Object> ids) throws Exception;

    public abstract int getPreviousPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception;

    public abstract ReconfigurationPlan setPartitionPlan(File partition_json_file) throws Exception;

    /**
     * Update the current partition plan
     * 
     * @param partition_json
     * @return The delta between the plans or null if there is no change
     */
    public abstract ReconfigurationPlan setPartitionPlan(JSONObject partition_json);

    /**
     * @return the current partition plan
     */
    public abstract PartitionPhase getCurrentPlan();

    /**
     * @return the previous partition plan
     */
    public abstract PartitionPhase getPreviousPlan();

    /*
     * (non-Javadoc)
     * @see org.json.JSONString#toJSONString()
     */
    public abstract String toJSONString();

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#save(java.io.File)
     */
    public abstract void save(File output_path) throws IOException;

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#load(java.io.File,
     * org.voltdb.catalog.Database)
     */
    public abstract void load(File input_path, Database catalog_db) throws IOException;

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
     */
    public abstract void toJSON(JSONStringer stringer) throws JSONException;

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#fromJSON(org.json.JSONObject,
     * org.voltdb.catalog.Database)
     */
    public abstract void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException;

    public abstract Map<String, List<String>> getRelatedTablesMap();
    
    public abstract Map<String, List<Table>> getRelatedCatalogTablesMap();

}