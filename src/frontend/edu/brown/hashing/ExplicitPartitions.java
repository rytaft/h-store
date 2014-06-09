package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import edu.brown.catalog.DependencyUtil;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;

public abstract class ExplicitPartitions {

	protected CatalogContext catalog_context;
	protected Set<String> plan_tables;
	protected Map<String, String> partitionedTablesByFK;
	protected Map<CatalogType, Table> catalog_to_table_map;
	protected Map<String, Column[]> table_partition_cols_map;
	protected String default_table = null;
	protected Map<String, List<String>> relatedTablesMap;
	protected Map<String, List<Table>> relatedCatalogTablesMap;
	
	private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String TABLES = "tables";
    public static final String PARTITIONS = "partitions";
    protected static final String DEFAULT_TABLE = "default_table";
    
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
	
	protected ExplicitPartitions(CatalogContext catalog_context, JSONObject partition_json) throws Exception {
		this.catalog_context = catalog_context;
        this.catalog_to_table_map = new HashMap<>();
        this.table_partition_cols_map = new HashMap<>();
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
                    table = this.catalog_context.getTableByName(this.default_table);
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
                    }
                }
                
                String relatedTblName = null;
                if(relatedTables.contains(this.default_table)) {
                	relatedTblName = this.default_table;
                } else {
                	for(String name : relatedTables) {
                		if (partitionedTables.contains(name)) {
                			relatedTblName = name;
                			break;
                		}
                	}
                }
                

                if (relatedTblName == null) {
                    throw new RuntimeException("No partitioned relationship found for table : " + tableName + " partitioned:" + partitionedTables.toString());
                }
                Table relatedTbl = this.catalog_context.getTableByName(relatedTblName);

                LOG.info("parent partitioned table : " + relatedTbl + " : " + relatedTblName);
                partitionedTablesByFK.put(tableName, relatedTblName);
                if (catalog_to_table_map.containsKey(table))
                	LOG.error("ctm has table already : " + table);
                catalog_to_table_map.put(table, relatedTbl);
                if (catalog_to_table_map.containsKey(partitionCol)) {
                	LOG.error("ctm has part col : " + partitionCol + " : " + catalog_to_table_map.get(partitionCol));
                }
                //TODO catalog_to_table_map.put(partitionCol, relatedTblName);
                LOG.info("no relationships on look up " +partitionCol + " : " + tableName);
                catalog_to_table_map.put(partitionCol, table);

                			
                if(!relatedTables.isEmpty()){
                    LOG.info("Associating the list of related tables for :"+ tableName);
                    relatedTables.add(tableName);
                    relatedCatalogTables.add(table);
                    relatedTablesMap.put(tableName, relatedTables);
                    relatedCatalogTablesMap.put(tableName, relatedCatalogTables);
                }
            }
        }
	}
	
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
    
    public int getPartitionId(String table_name, Object[] ids) throws Exception {
    	return getPartitionId(table_name, Arrays.asList(ids));
    }
    
    public int getPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception {
    	String table_name = this.catalog_to_table_map.get(catalogs.get(0)).getName().toLowerCase();
        return this.getPartitionId(table_name, ids);
    }

    public String getTableName(CatalogType catalog) {
        return this.catalog_to_table_map.get(catalog).getName().toLowerCase();
    }
    
    public String getTableName(List<CatalogType> catalog) {
        return this.catalog_to_table_map.get(catalog.get(0)).getName().toLowerCase();
    }

    public Table getTable(List<CatalogType> catalog) {
        return this.catalog_to_table_map.get(catalog.get(0));
    }

    /**
     * Get the previous partition id for a given table and partition id/key
     * 
     * @param table_name
     * @param id
     * @return the partition id, or -1 / null partition if the id/key is not
     *         found in the plan OR if there is no previous plan
     * @throws Exception
     */
    
    public int getPreviousPartitionId(List<CatalogType> catalogs, List<Object> ids) throws Exception {
        String table_name = this.catalog_to_table_map.get(catalogs.get(0)).getName().toLowerCase();
        return this.getPreviousPartitionId(table_name, ids);
    }
    
    public abstract int getPreviousPartitionId(String table_name, List<Object> ids) throws Exception;

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

    public Map<String, List<String>> getRelatedTablesMap() {
        return relatedTablesMap;
    }
    
    public Map<String, List<Table>> getRelatedCatalogTablesMap() {
        return relatedCatalogTablesMap;
    }

}