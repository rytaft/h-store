/**
 * 
 */
package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;
import org.voltdb.utils.VoltTableComparator;

import edu.brown.catalog.DependencyUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;

//       TODO This class likely needs to be relocated (ae)
/**
 * @author aelmore A container for statically defined partitions plans. Each
 *         plan will contain multiple partition phases. Each partition phase
 *         will contain a list of tables that dictate how the table is
 *         partitioned. <br>
 *         PlannedPartitions Hierarchy:
 *         <ul>
 *         <li>Map[String, PartitionPhase] partition_phase_map
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

public class PlannedPartitions extends ExplicitPartitions implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(PlannedPartitions.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String PLANNED_PARTITIONS = "partition_plans";
    
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private Map<String, PartitionPhase> partition_phase_map;
    private String current_phase;
    private String previous_phase;
    
    public PlannedPartitions(CatalogContext catalog_context, File planned_partition_json_file) throws Exception {
        this(catalog_context, new JSONObject(FileUtil.readFile(planned_partition_json_file)));
    }

    public PlannedPartitions(CatalogContext catalog_context, JSONObject planned_partition_json) throws Exception {
    	super(catalog_context, planned_partition_json);
        this.current_phase = null;
        this.previous_phase = null;
        this.partition_phase_map = new HashMap<>();
        
        if (planned_partition_json.has(PLANNED_PARTITIONS)) {
            JSONObject phases = planned_partition_json.getJSONObject(PLANNED_PARTITIONS);
            String first_key = null;
            Iterator<String> keys = phases.keys();
            while (keys.hasNext()) {
                String key = keys.next();

                JSONObject phase = phases.getJSONObject(key);
                this.partition_phase_map.put(key, new PartitionPhase(catalog_context, phase, partitionedTablesByFK));

                // Use the first phase by default
                if (first_key == null) {
                    first_key = key;
                    this.setPartitionPhase(first_key);
                }
            }

        } else {
            throw new JSONException(String.format("JSON file is missing key \"%s\". ", PLANNED_PARTITIONS));
        }

        // TODO check to make sure partitions exist that are in the plan (ae)

    }

    /**
     * Get the explicit partitioned tables and ensure that each phase has the
     * same set of tables
     * 
     * @param planned_partition_json
     * @return the set of tables in the partition plan
     */
    public Set<String> getExplicitPartitionedTables(JSONObject planned_partition_json) {
        try {
            Set<String> tables = null;
            if (planned_partition_json.has(PLANNED_PARTITIONS)) {
                JSONObject phases = planned_partition_json.getJSONObject(PLANNED_PARTITIONS);
                Iterator<String> keys = phases.keys();
                while (keys.hasNext()) {
                    JSONObject phase = phases.getJSONObject(keys.next());
                    Set<String> phase_tables = new HashSet<>();
                    Iterator<String> table_names = phase.getJSONObject(TABLES).keys();
                    while (table_names.hasNext()) {
                        phase_tables.add(table_names.next());
                    }
                    if (tables == null) {
                        // First set of tables
                        tables = phase_tables;
                    } else {
                        // check if equal
                        if (tables.equals(phase_tables) == false) {
                            throw new RuntimeException(String.format("Partition plan has mistmatched tables (%s) != (%s)", tables, phase_tables));
                        }
                    }
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
    	PartitionPhase phase = this.partition_phase_map.get(this.getCurrent_phase());
        PartitionedTable table = phase.getTable(table_name);
        if (table == null) {
            if (debug.val)
                LOG.debug(String.format("Table not found: %s, using default:%s ", table_name, this.default_table));
            table = phase.getTable(this.default_table);
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
    	String previousPhase = this.getPreviousPhase_phase();
        if (previousPhase == null)
            return -1;
        PartitionPhase phase = this.partition_phase_map.get(previousPhase);
        PartitionedTable table = phase.getTable(table_name);
        if (table == null){
            table = phase.getTable(table_name.toLowerCase());
            if (table == null) {
                throw new Exception("Unable to find table "+ table_name + " in phase  " + previousPhase);
            }
        }
        assert table != null : "Table not found " + table_name;
        return table.findPartition(ids);
    }
    

    // ******** Containers *****************************************/

    /**
     * @author aelmore Holds the phases/epochs/version of a partition plan
     */
    public static class PartitionPhase {
        protected Map<String, PartitionedTable> tables_map;

        @SuppressWarnings("unchecked")
        public List<PartitionRange> getPartitions(String table_name) {
            return (List<PartitionRange>) this.tables_map.get(table_name);
        }

        public PartitionedTable getTable(String table_name) {
            return this.tables_map.get(table_name);
        }

        /**
         * Create a new partition phase
         * 
         * @param catalog_db
         * @param table_vt_map
         *            mapping of table names to volt type of partition col
         * @param phase
         *            JSONObject
         * @param partitionedTablesByFK
         */
        public PartitionPhase(CatalogContext catalog_context, JSONObject phase, Map<String, String> partitionedTablesByFK) throws Exception {
            this.tables_map = new HashMap<String, PlannedPartitions.PartitionedTable>();
            assert (phase.has(TABLES));
            JSONObject json_tables = phase.getJSONObject(TABLES);
            Iterator<String> table_names = json_tables.keys();
            while (table_names.hasNext()) {
                String table_name = table_names.next();
                JSONObject table_json = json_tables.getJSONObject(table_name.toLowerCase());
                // Class<?> c = table_vt_map.get(table_name).classFromType();
                // TODO fix partitiontype
                this.tables_map.put(table_name, new PartitionedTable(table_name, table_json, catalog_context.getTableByName(table_name)));
            }

            //Add entries for tables that are partitioned on other columns
            for (Entry<String, String> partitionedFK : partitionedTablesByFK.entrySet()) {
                String table_name = partitionedFK.getKey();
                String fk_table_name = partitionedFK.getValue();
                if (json_tables.has(fk_table_name) == false) {
                    throw new RuntimeException(String.format("For table %s, the foreignkey partitioned table %s is not explicitly partitioned ", table_name,fk_table_name));
                }
                LOG.info(String.format("Adding FK partitioning %s->%s", table_name, fk_table_name));
                this.tables_map.put(partitionedFK.getKey(), this.tables_map.get(partitionedFK.getValue()).clone(table_name, catalog_context.getTableByName(table_name)));
            }
        }

        protected PartitionPhase(Map<String, PlannedPartitions.PartitionedTable> table_map) {
            this.tables_map = table_map;
        }
    }

    /**
     * @author aelmore Holds the partitioning for a table, during a given phase
     * @param <T>
     *            The type of the ID which is partitioned on. Comparable
     */
    public static class PartitionedTable {

        protected List<PartitionRange> partitions;
        protected String table_name;
        private Table catalog_table;
        private JSONObject table_json;

        public PartitionedTable(String table_name, JSONObject table_json, Table catalog_table) throws Exception {
            this.catalog_table = catalog_table;
            this.partitions = new ArrayList<>();
            this.table_name = table_name;
            this.table_json = table_json;
            assert (table_json.has(PARTITIONS));
            JSONObject partitions_json = table_json.getJSONObject(PARTITIONS);
            Iterator<String> partitions = partitions_json.keys();
            while (partitions.hasNext()) {
                String partition = partitions.next();
                // TODO do we need more than ints, what about specifying ranges
                // as
                // replicated tables (ae)
                int partition_id = Integer.parseInt(partition);
                this.addPartitionRanges(partition_id, partitions_json.getString(partition));
            }
            Collections.sort(this.partitions);
        }
        
        public PartitionedTable clone(String new_table_name, Table new_catalog_table) throws Exception{
            return new PartitionedTable(new_table_name, this.table_json,new_catalog_table);
        }

        public PartitionedTable(List<PartitionRange> partitions, String table_name) {
            this.partitions = partitions;
            this.table_name = table_name;
        }

        public PartitionedTable(List<PartitionRange> partitions, String table_name, Table catalog_table) {
            this.partitions = partitions;
            this.table_name = table_name;
            this.catalog_table = catalog_table;
        }

        /**
         * Find the partition for a key
         * 
         * @param id
         * @return the partition id or null partition id if no match could be
         *         found
         */
        @SuppressWarnings("unchecked")
        public int findPartition(List<Object> ids) throws Exception {
            if (trace.val) {
                LOG.trace(String.format("Looking up key %s on table %s during phase %s", ids.get(0), this.table_name));
            }

            try {
                for (PartitionRange p : this.partitions) {
                    // if this greater than or equal to the min inclusive val
                    // and
                    // less than
                    // max_exclusive or equal to both min and max (singleton)
                    // TODO fix partitiontype
                	if (p.inRange(ids)) {
                		return p.partition;
                	}
                }
            } catch (Exception e) {
                LOG.error("Error looking up partition", e);
            }

            LOG.error("Partition not found");
            return HStoreConstants.NULL_PARTITION_ID;
        }

        /**
         * Associate a partition with a set of values in the form of val or
         * val1,val2 or val1-val2 or val1,val2-val3 or some other combination
         * 
         * @param partition_id
         * @param partitionValues
         * @throws ParseException
         */
        public void addPartitionRanges(int partition_id, String partition_values) throws ParseException {
            for (String range : partition_values.split(",")) {
                this.partitions.add(new PartitionRange(this.catalog_table, partition_id, range));
            }
        }

        public Table getCatalog_table() {
            return catalog_table;
        }
    }

    /**
     * A defined range of keys and an associated partition id. Sorts by min id,
     * then max id only first, ie (1-4 < 2-3) and (1-4 < 1-5)
     * 
     * @author aelmore
     * @param <T>
     *            Comparable type of key
     */
    public static class PartitionRange implements Comparable<PartitionRange> {
        private int partition;
        private VoltTable keySchema;
        private VoltTable keySchemaCopy; // not exposed outside of the class
        private Object[] min_incl;
        private Object[] max_excl;
        private VoltTableComparator cmp;
        private Table catalog_table;
        
        public PartitionRange(Table table, int partition_id, String range_str) throws ParseException {
            this.partition = partition_id;
            this.catalog_table = table;
            
            this.keySchema = ReconfigurationUtil.getPartitionKeysVoltTable(table);
            this.keySchemaCopy = this.keySchema.clone(0);
            Object[] min_row;
            Object[] max_row;
            
            // x-y
            if (range_str.contains("-")) {
            	String vals[] = range_str.split("-", 2);

            	min_row = getRangeKeys(vals[0]);
                max_row = getRangeKeys(vals[1]);
                
            } else {
            	throw new ParseException("keys must be specified as min-max. range: " + range_str, -1);
            }
            
            this.cmp = ReconfigurationUtil.getComparator(keySchema);

            keySchemaCopy.addRow(min_row);
            keySchemaCopy.advanceToRow(0);
            this.min_incl = keySchemaCopy.getRowArray();
            keySchemaCopy.clearRowData();
        	
            keySchemaCopy.addRow(max_row);
            keySchemaCopy.advanceToRow(0);
            this.max_excl = keySchemaCopy.getRowArray();
            keySchemaCopy.clearRowData();
        	
            if (cmp.compare(this.min_incl, this.max_excl) > 0) {
            	throw new ParseException("Min cannot be greater than max", -1);
    	    }
        }
        
        private Object[] getRangeKeys(String key_str) throws ParseException {
        	String keys[];
        	// multi-key partitioning
        	if (key_str.contains(":")) {
        		keys = key_str.split(":");
        	} else {
        		keys = new String[]{ key_str };
        	}

        	Object[] row = new Object[keySchema.getColumnCount()];
            
        	int col = 0;
        	for(String key : keys) {
        		assert(col < keySchema.getColumnCount());
        		VoltType vt = keySchema.getColumnType(col);

        		row[col] = parseValue(vt, key);
        		col++;
        	}

        	for ( ; col < keySchema.getColumnCount(); col++) {
        		VoltType vt = keySchema.getColumnType(col);
        		Object obj = vt.getNullValue();
        		row[col] = obj;
        	}
        	
        	return row;
        }
        
        private Object parseValue(VoltType vt, String value) throws ParseException {
        	if (value.isEmpty()) {
        		return vt.getNullValue();
        	}
        	return VoltTypeUtil.getObjectFromString(vt, value);
        }
        
        private int getNonNullCols() {        	
        	int non_null_cols = 0;
            for(int i = 0; i < min_incl.length; i++) {
            	VoltType vt = keySchema.getColumnType(i);
            	if(vt.getNullValue().equals(min_incl[i]) && vt.getNullValue().equals(max_excl[i])) {
            		break;
            	}
            	non_null_cols++;
            }
            return non_null_cols;
        }
        
        @Override
        public String toString() {
        	String min_str = "";
            String max_str = "";
        	for(int i = 0; i < this.min_incl.length; i++) {
        		Object min = this.min_incl[i];
        		Object max = this.max_excl[i];
        		VoltType vt = this.keySchema.getColumnType(i);
        		if(!vt.getNullValue().equals(min)) {
        			if(i != 0) {
        				min_str += ":";
        			}
        			min_str += min.toString();
        		}
        		if(!vt.getNullValue().equals(max)) {
        			if(i != 0) {
        				max_str += ":";
        			}
        			max_str += max.toString();
        		}
        	}
        	return "PartitionRange [" + min_str + "-" + max_str + ") p_id=" + this.partition + "]";        	
        }

        @Override
        public int compareTo(PartitionRange o) {
        	if (cmp.compare(this.min_incl, o.min_incl) < 0) {
        		return -1;
        	} else if (cmp.compare(this.min_incl, o.min_incl) == 0) {
        		return cmp.compare(this.max_excl, o.max_excl);
        	} else {
        		return 1;
        	}
        }
        
        public synchronized boolean inRange(List<Object> ids) {
        	Object[] keys = new Object[this.min_incl.length];
        	int col = 0;
        	for(Object id : ids) {
        		if(col >= keys.length) {
        			break;
        		}
        		keys[col] = id;
        		col++;
        	}
        	for( ; col < keys.length; col++) {
        		VoltType vt = this.keySchema.getColumnType(col);
            	keys[col] = vt.getNullValue();
        	}
        	
        	keySchemaCopy.addRow(keys);
        	keySchemaCopy.advanceToRow(0);
        	Object[] rowArray = keySchemaCopy.getRowArray();
        	keySchemaCopy.clearRowData();
        	return inRange(rowArray, ids.size());
        }
        
        public boolean inRange(Object[] keys, int orig_size) {
        	if(cmp.compare(min_incl, keys) <= 0 && 
        			(cmp.compare(max_excl, keys) > 0 || 
        					(cmp.compare(min_incl, max_excl) == 0 && 
        					cmp.compare(min_incl, keys) == 0))){
        		if (orig_size >= getNonNullCols()) {
        			return true;
        		}
        	}

            return false;
        }
        
        public VoltTable getMinInclTable() {
        	VoltTable minInclTable = this.keySchema.clone(0);
        	minInclTable.addRow(this.min_incl);
        	return minInclTable;
        }
        
        public VoltTable getMaxExclTable() {
        	VoltTable maxExclTable = this.keySchema.clone(0);
        	maxExclTable.addRow(this.max_excl);
        	return maxExclTable;
        }
        
        public Object[] getMinIncl() {
        	return this.min_incl;
        }
        
        public Object[] getMaxExcl() {
        	return this.max_excl;
        }
        
        public VoltTable getKeySchema() {
        	return this.keySchema;
        }
        
        public Table getTable() {
        	return this.catalog_table;
        }
        
        public int getPartition() {
        	return this.partition;
        }

    }

    // ********End Containers **************************************/

    /**
     * Update the current partition phase (plan/epoch/etc)
     * 
     * @param new_phase
     * @return The delta between the plans or null if there is no change
     */
    public ReconfigurationPlan setPartitionPhase(String new_phase) {
        String old_phase = this.current_phase;
        if (old_phase != null && old_phase.equals(new_phase)) {
            return null;
        }
        if (this.partition_phase_map.containsKey(new_phase) == false) {
            throw new RuntimeException("Invalid Phase Name: " + new_phase + " phases: " + StringUtil.join(",", this.partition_phase_map.keySet()));
        }
        synchronized (this) {
            this.current_phase = new_phase;
            this.previous_phase = old_phase;
        }
        try {
            if (old_phase == null) {
                return null;
            }
            return new ReconfigurationPlan(this.partition_phase_map.get(old_phase), this.partition_phase_map.get(new_phase));
        } catch (Exception ex) {
            LOG.error("Exception on setting partition phase", ex);
            LOG.error(String.format("Old phase: %s  New Phase: %s" , old_phase,new_phase));
            throw new RuntimeException("Exception building Reconfiguration plan", ex);
        }

    }

    /**
     * @return the current partition phase/epoch
     */
    public synchronized String getCurrent_phase() {
        return this.current_phase;
    }

    /**
     * @return the current partition phase/epoch
     */
    public synchronized String getPreviousPhase_phase() {
        return this.previous_phase;
    }

    /*
     * (non-Javadoc)
     * @see org.json.JSONString#toJSONString()
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
    @Override
    public void save(File output_path) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#load(java.io.File,
     * org.voltdb.catalog.Database)
     */
    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
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
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // TODO Auto-generated method stub

    }

    @Override
    public ReconfigurationPlan setPartitionPlan(File partition_json_file) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ReconfigurationPlan setPartitionPlan(JSONObject partition_json) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized PartitionPhase getCurrentPlan() {
        
        return partition_phase_map.get(this.current_phase);
    }

    @Override
    public synchronized PartitionPhase getPreviousPlan() {
        return partition_phase_map.get(this.previous_phase);
    }

}
