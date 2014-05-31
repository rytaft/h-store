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

public class TwoTieredRangePartitions implements JSONSerializable, ExplicitPartitions {
    private static final Logger LOG = Logger.getLogger(TwoTieredRangePartitions.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String PARTITION_PLAN = "partition_plan";
    public static final String TABLES = "tables";
    public static final String PARTITIONS = "partitions";
    private static final String DEFAULT_TABLE = "default_table";

    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private CatalogContext catalog_context;
    private PartitionPhase partition_plan;
    private PartitionPhase old_partition_plan;
    private Set<String> plan_tables;
    private Map<String, String> partitionedTablesByFK;
    private Map<CatalogType, Table> catalog_to_table_map;
    private Map<String, Column[]> table_partition_cols_map;
    private String default_table = null;
    private Map<String, List<String>> relatedTablesMap;
    private Map<String, List<Table>> relatedCatalogTablesMap;
    
    public TwoTieredRangePartitions(CatalogContext catalog_context, File partition_json_file) throws Exception {
        this(catalog_context, new JSONObject(FileUtil.readFile(partition_json_file)));
    }

    public TwoTieredRangePartitions(CatalogContext catalog_context, JSONObject partition_json) throws Exception {
        this.catalog_context = catalog_context;
        this.catalog_to_table_map = new HashMap<>();
        this.table_partition_cols_map = new HashMap<>();
        this.old_partition_plan = null;
        this.partition_plan = null;
        this.plan_tables = null;
        this.relatedTablesMap = new HashMap<>();
        this.relatedCatalogTablesMap = new HashMap<>();
        Set<String> partitionedTables = getExplicitPartitionedTables(partition_json);
        // TODO find catalogContext.getParameter mapping to find
        // statement_column
        // from project mapping (ae)
        assert partition_json.has(DEFAULT_TABLE) : "default_table missing from planned partition json";
        this.default_table = partition_json.getString(DEFAULT_TABLE);
        for (Table table : catalog_context.getDataTables()) {
            if (table.getIsreplicated()) continue;
        	
        	String tableName = table.getName().toLowerCase();
          
            Column[] cols = new Column[table.getPartitioncolumns().size()];
            for(ColumnRef colRef : table.getPartitioncolumns().values()) {
            	cols[colRef.getIndex()] = colRef.getColumn();
        	}
            
            // partition columns may not have been set
            Column partitionCol;
            if (cols.length == 0) {
            	partitionCol = table.getPartitioncolumn();
            	if (partitionCol != null) {
            		table_partition_cols_map.put(tableName, new Column[]{partitionCol});
            	}
            }
            else {
            	partitionCol = cols[0];
            	table_partition_cols_map.put(tableName, cols);
            }
            if (partitionCol == null) {
                LOG.info(String.format("Partition col for table %s is null. Skipping", tableName));
            } else {
                LOG.info(String.format("Adding table:%s partitionCol:%s %s", tableName, partitionCol, VoltType.get(partitionCol.getType())));
                this.catalog_to_table_map.put(partitionCol, table);
            }
        }

        for (Procedure proc : catalog_context.procedures) {
            if (!proc.getSystemproc()) {
            	Column[] cols = new Column[proc.getPartitioncolumns().size()];
                for(ColumnRef colRef : proc.getPartitioncolumns().values()) {
                	cols[colRef.getIndex()] = colRef.getColumn();
            	}
                
                // partition columns may not have been set
                Column partitionCol;
                if (cols.length == 0) {
                	partitionCol = proc.getPartitioncolumn();
                }
                else {
                	partitionCol = cols[0];
                }
            	
                String table_name = null;
                Table table = this.catalog_to_table_map.get(partitionCol);
                if(table != null) {
                	table_name = table.getName().toLowerCase();
                }
                if ((table_name == null) || (table_name.equals("null")) || (table_name.trim().length() == 0)) {
                    LOG.info(String.format("Using default table %s for procedure: %s ", this.default_table, proc.toString()));
                    table_name = this.default_table;
                } else {
                    LOG.info(table_name + " adding procedure: " + proc.toString());
                }
                this.catalog_to_table_map.put(proc, table);
                for (Statement statement : proc.getStatements()) {
                    LOG.debug(table_name + " adding statement: " + statement.toString());

                    this.catalog_to_table_map.put(statement, table);
                }

            }
        }
        // We need to track which tables are partitioned on another table in
        // order to generate the reconfiguration ranges for those tables,
        // because they are not explicitly partitioned in the plan.
        DependencyUtil dependUtil = DependencyUtil.singleton(catalog_context.database);
        partitionedTablesByFK = new HashMap<>();

        for (Table table : catalog_context.getDataTables()) {
            String tableName = table.getName().toLowerCase();
            // Making the assumption that the same tables are in all plans TODO
            // verify this
            if (partitionedTables.contains(tableName) == false) {

                Column[] partitionCols = this.table_partition_cols_map.get(tableName);
                if (partitionCols == null) {
                    LOG.info(tableName + " is not partitioned and has no partition column. skipping");
                    continue;
                } else {
                    LOG.info(tableName + " is not explicitly partitioned.");
                }

                Column partitionCol = partitionCols[0];
                List<Column> depCols = dependUtil.getAncestors(partitionCol);
                boolean partitionedParentFound = false;
                List<String> relatedTables = new ArrayList<>();
                List<Table> relatedCatalogTables = new ArrayList<>();
                for (Column c : depCols) {
                    CatalogType p = c.getParent();
                    if (p instanceof Table) {
                        // if in table then map to it
                        String relatedTblName = p.getName().toLowerCase();
                        Table relatedTbl = (Table) p;
                        LOG.info(String.format("Table %s is related to %s",tableName,relatedTblName));
                        relatedTables.add(relatedTblName);
                        relatedCatalogTables.add(relatedTbl);
                        if (partitionedTables.contains(relatedTblName)) {
                            LOG.info("parent partitioned table : " + p + " : " + relatedTblName);
                            partitionedTablesByFK.put(tableName, relatedTblName);
                            partitionedParentFound = true;
                            if (catalog_to_table_map.containsKey(table))
                                LOG.error("ctm has table already : " + table);
                            catalog_to_table_map.put(table, relatedTbl);
                            if (catalog_to_table_map.containsKey(partitionCol)) {
                                LOG.error("ctm has part col : " + partitionCol + " : " + catalog_to_table_map.get(partitionCol));
                            }
                            //TODO catalog_to_table_map.put(partitionCol, relatedTblName);
                            LOG.info("no relationships on look up " +partitionCol + " : " + tableName);
                            catalog_to_table_map.put(partitionCol, table);
                        }
                    }
                }
                if(!relatedTables.isEmpty()){
                    LOG.info("Associating the list of related tables for :"+ tableName);
                    relatedTables.add(tableName);
                    relatedCatalogTables.add(table);
                    relatedTablesMap.put(tableName, relatedTables);
                    relatedCatalogTablesMap.put(tableName, relatedCatalogTables);
                }
                if (!partitionedParentFound) {
                    throw new RuntimeException("No partitioned relationship found for table : " + tableName + " partitioned:" + partitionedTables.toString());
                }
            }
        }

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
     * @see edu.brown.hashing.ExplicitPartition#getPartitionId(java.lang.String, java.lang.Object)
     */
    @Override
    public int getPartitionId(String table_name, Object[] ids) throws Exception {
    	ArrayList<Object> idList = new ArrayList<>();
    	for (Object id : ids) {
    		idList.add(id);
    	}
    	return getPartitionId(table_name, idList);
    }
    
    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getPartitionId(org.voltdb.catalog.CatalogType, java.lang.Object)
     */
    @Override
    public int getPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception {
    	String table_name = this.catalog_to_table_map.get(catalogs.get(0)).getName().toLowerCase();
        return this.getPartitionId(table_name, ids);
    }

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getTableName(org.voltdb.catalog.CatalogType)
     */
    @Override
    public String getTableName(CatalogType catalog) {
        return this.catalog_to_table_map.get(catalog).getName().toLowerCase();
    }
    
    @Override
    public String getTableName(List<CatalogType> catalog) {
        return this.catalog_to_table_map.get(catalog.get(0)).getName().toLowerCase();
    }
    
    @Override
    public Table getTable(List<CatalogType> catalog) {
        return this.catalog_to_table_map.get(catalog.get(0));
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
     * @see edu.brown.hashing.ExplicitPartition#getPreviousPartitionId(org.voltdb.catalog.CatalogType, java.lang.Object)
     */
    @Override
    public int getPreviousPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception {
        String table_name = this.catalog_to_table_map.get(catalogs.get(0)).getName().toLowerCase();
        return this.getPreviousPartitionId(table_name, ids);
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
            return new ReconfigurationPlan(old_plan, new_plan);
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

    /* (non-Javadoc)
     * @see edu.brown.hashing.ExplicitPartition#getRelatedTablesMap()
     */
    @Override
    public Map<String, List<String>> getRelatedTablesMap() {
        return relatedTablesMap;
    }
    
    @Override
    public Map<String, List<Table>> getRelatedCatalogTablesMap() {
        return relatedCatalogTablesMap;
    }

    
}
