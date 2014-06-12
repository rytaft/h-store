/**
 * 
 */
package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.CatalogContext;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.DependencyUtil;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.StringUtil;

//       TODO This class likely needs to be relocated (ae)
/**
 * @author aelmore, rytaft A partition plan
 *         will contain a list of tables that dictate how the table is
 *         partitioned. <br>
 *         TwoTieredRangePartitions Hierarchy:
 *         <ul>
 *         <li>PartitionPhase partition_plan
 *         <ul>
 *         <li>Map[String, PartitionedTable] tables_map
 *         <ul>
 *         <li>List[PartitionRange] partitions
 *         <ul>
 *         <li>PartitionRange: min,max,partition_id
 *         </ul>
 *         </ul>
 *         </ul>
 *         </ul>
 */

public class TwoTieredRangePartitions extends ExplicitPartitions implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(TwoTieredRangePartitions.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String PARTITION_PLAN = "partition_plan";
    
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private PartitionPhase partition_plan;
    private PartitionPhase old_partition_plan;
    
    public TwoTieredRangePartitions(CatalogContext catalog_context, File partition_json_file) throws Exception {
        this(catalog_context, new JSONObject(FileUtil.readFile(partition_json_file)));
    }

    public TwoTieredRangePartitions(CatalogContext catalog_context, JSONObject partition_json) throws Exception {
    	super(catalog_context, partition_json);
    	this.old_partition_plan = null;
        this.partition_plan = null;
        
        if (partition_json.has(PARTITION_PLAN)) {
            JSONObject plan = partition_json.getJSONObject(PARTITION_PLAN);
            this.partition_plan = new PartitionPhase(catalog_context, plan, partitionedTablesByFK);
        } else {
            throw new JSONException(String.format("JSON file is missing key \"%s\". ", PARTITION_PLAN));
        }

        // TODO check to make sure partitions exist that are in the plan (ae)

    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getExplicitPartitionedTables(org.json.JSONObject)
     */
    @Override
    public Set<String> getExplicitPartitionedTables(JSONObject partition_json) {
    	try {
    		Set<String> tables = new HashSet<>();;
            if (partition_json.has(PARTITION_PLAN)) {
                JSONObject plan = partition_json.getJSONObject(PARTITION_PLAN);
                Iterator<String> table_names = plan.getJSONObject(TABLES).keys();
                while (table_names.hasNext()) {
                	tables.add(table_names.next());
                }
                
                synchronized (this) {
                	if(plan_tables != null) {
                		// check if equal
                		if (!tables.equals(plan_tables)) {
                			throw new RuntimeException(String.format("Partition plan has mistmatched tables (%s) != (%s)", tables, plan_tables));
                		}
                	}
                	plan_tables = tables;
                }
            }
            return tables;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getPartitionId(java.lang.String, java.lang.Object)
     */
    @Override
    public int getPartitionId(String table_name, List<Object> ids) throws Exception {
        PartitionPhase plan = this.getCurrentPlan();
        PartitionedTable table = plan.getTable(table_name);
        if (table == null) {
            if (debug.val)
                LOG.debug(String.format("Table not found: %s, using default:%s ", table_name, this.default_table));
            table = plan.getTable(this.default_table);
            if (table == null) {
                throw new RuntimeException(String.format("Default partition table is null. Lookup table:%s Default Table:%s", table_name, this.default_table));
            }
        }
        assert table != null : "Table not found " + table_name;
        return table.findPartition(ids);
    }

    
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getPreviousPartitionId(java.lang.String, java.lang.Object)
     */
    @Override
    public int getPreviousPartitionId(String table_name, List<Object> ids) throws Exception {
        PartitionPhase previousPlan = this.getPreviousPlan();
        if (previousPlan == null)
            return -1;
        PartitionedTable table = previousPlan.getTable(table_name);
        assert table != null : "Table not found " + table_name;
        return table.findPartition(ids);
    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#setPartitionPlan(java.io.File)
     */
    @Override
    public ReconfigurationPlan setPartitionPlan(File partition_json_file) throws Exception {
    	return setPartitionPlan(new JSONObject(FileUtil.readFile(partition_json_file)));
    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#setPartitionPlan(org.json.JSONObject)
     */
    @Override
    public ReconfigurationPlan setPartitionPlan(JSONObject partition_json) {
        try {
        	// check that the new tables match the old tables
        	getExplicitPartitionedTables(partition_json);
        	
        	PartitionPhase new_plan = null;
            PartitionPhase old_plan = null;
        	// update the partition plan
            if (partition_json.has(PARTITION_PLAN)) {
                JSONObject plan = partition_json.getJSONObject(PARTITION_PLAN);
                new_plan = new PartitionPhase(catalog_context, plan, partitionedTablesByFK);
                synchronized (this) {
            		this.old_partition_plan = this.partition_plan;
            		this.partition_plan = new_plan;
            		old_plan = this.old_partition_plan;
            	}
            } else {
                throw new JSONException(String.format("JSON file is missing key \"%s\". ", PARTITION_PLAN));
            }

            if (old_plan == null) {
                return null;
            }
            return new ReconfigurationPlan(this.catalog_context, old_plan, new_plan);
        } catch (Exception ex) {
            LOG.error("Exception on setting partition plan", ex);
            LOG.error(String.format("Old plan: %s  New plan: %s" , getPreviousPlan() ,getCurrentPlan()));
            throw new RuntimeException("Exception building Reconfiguration plan", ex);
        }

    }
    
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getCurrentPlan()
     */
    @Override
    public synchronized PartitionPhase getCurrentPlan() {
        return this.partition_plan;
    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getPreviousPlan()
     */
    @Override
    public synchronized PartitionPhase getPreviousPlan() {
        return this.old_partition_plan;
    }


    /*
     * (non-Javadoc)
     * @see org.json.JSONString#toJSONString()
     */
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#toJSONString()
     */
    @Override
    public String toJSONString() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#save(java.io.File)
     */
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#save(java.io.File)
     */
    @Override
    public void save(File output_path) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#load(java.io.File,
     * org.voltdb.catalog.Database)
     */
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#load(java.io.File, org.voltdb.catalog.Database)
     */
    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
     */
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#toJSON(org.json.JSONStringer)
     */
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#fromJSON(org.json.JSONObject,
     * org.voltdb.catalog.Database)
     */
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#fromJSON(org.json.JSONObject, org.voltdb.catalog.Database)
     */
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // TODO Auto-generated method stub

    }

    
}
