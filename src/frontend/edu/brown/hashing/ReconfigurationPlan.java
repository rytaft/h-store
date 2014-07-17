/**
 * 
 */
package edu.brown.hashing;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Table;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableComparator;

import edu.brown.designer.MemoryEstimator;
import edu.brown.hashing.PlannedPartitions.PartitionKeyComparator;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;

/**
 * The delta between two partition plans
 * @author aelmore
 *
 */
public class ReconfigurationPlan {

	private static final Logger LOG = Logger.getLogger(ReconfigurationPlan.class);
    protected Map<String,ReconfigurationTable> tables_map;
    
    //Helper map of partition ID and outgoing/incoming ranges for this reconfiguration
    protected Map<Integer, List<ReconfigurationRange>> outgoing_ranges;
    protected Map<Integer, List<ReconfigurationRange>> incoming_ranges;
    protected Map<String, List<ReconfigurationRange>> range_map;
    public String planDebug = "";
    private CatalogContext catalogContext;
    protected Map<String, String> partitionedTablesByFK;
    
    public ReconfigurationPlan(CatalogContext catalogContext, Map<String, String> partitionedTablesByFK){
        this.catalogContext = catalogContext;
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        range_map = new HashMap<>();
        this.partitionedTablesByFK = partitionedTablesByFK;
    }
    
    public void addRange(ReconfigurationRange range){

    	
        if(!outgoing_ranges.containsKey(range.old_partition)) {
            outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange>());
        }
        if(!incoming_ranges.containsKey(range.new_partition)) {
            incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange>());
        }
        if(!range_map.containsKey(range.table_name)) {
        	range_map.put(range.table_name, new ArrayList<ReconfigurationRange>());
        }
        outgoing_ranges.get(range.old_partition).add(range);
        incoming_ranges.get(range.new_partition).add(range);
        range_map.get(range.table_name).add(range);
    }    
    
    /**
     * @throws Exception 
     * 
     */
    public ReconfigurationPlan(CatalogContext catalogContext, PartitionPhase old_phase,PartitionPhase new_phase) throws Exception {
        this.catalogContext = catalogContext;
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        range_map = new HashMap<>();
        partitionedTablesByFK = old_phase.partitionedTablesByFK;
        assert old_phase.tables_map.keySet().equals(new_phase.tables_map.keySet()) : "Partition plans have different tables";
        tables_map = new HashMap<String, ReconfigurationPlan.ReconfigurationTable>();
        for(String table_name : old_phase.tables_map.keySet()){
            tables_map.put(table_name, new ReconfigurationTable(catalogContext, old_phase.getTable(table_name), new_phase.getTable(table_name)));
        }
        registerReconfigurationRanges();
        planDebug = String.format("Reconfiguration plan generated \n Out: %s \n In: %s",outgoing_ranges.toString(),incoming_ranges.toString());
        LOG.info(planDebug);
    }
    
    protected void registerReconfigurationRanges(){
        for(String table_name : tables_map.keySet()){
            for(ReconfigurationRange range : tables_map.get(table_name).getReconfigurations()){
                addRange(range);
            }
        }
    }
    
    /**
     * Find the reconfiguration range for a key
     * 
     * @param id
     * @return the reconfiguration range or null if no match could be
     *         found
     */
    public ReconfigurationRange findReconfigurationRange(String table_name, List<Object> ids) throws Exception {
    	Pair<String, List<Object>> key = new Pair<>(table_name, ids);
    	try {

            
    		List<ReconfigurationRange> ranges = this.range_map.get(table_name);
    		if (ranges == null) {
    			return null;
    		}
    		
    		for (ReconfigurationRange r : ranges) {
    			// if this greater than or equal to the min inclusive val
    			// and
    			// less than
    			// max_exclusive or equal to both min and max (singleton)
    			if (r.inRange(ids)) {

    				return r;
    			}
    		}
          
        } catch (Exception e) {
            LOG.error("Error looking up reconfiguration range", e);
        }

        return null;
    }
    
    /**
     * Find all reconfiguration ranges that may contain a key
     * 
     * @param id
     * @return the matching reconfiguration ranges
     */
    public List<ReconfigurationRange> findAllReconfigurationRanges(String table_name, List<Object> ids) throws Exception {
    	List<ReconfigurationRange> matchingRanges = new ArrayList<ReconfigurationRange>();
		List<ReconfigurationRange> ranges = this.range_map.get(table_name);
		if (ranges == null) {
			return matchingRanges;
		}
		for (ReconfigurationRange r : ranges) {
			try {

				// if this greater than or equal to the min inclusive val
				// and
				// less than
				// max_exclusive or equal to both min and max (singleton)
				if (r.inRangeIgnoreNullCols(ids)) {
					matchingRanges.add(r);
				} 
			} catch (Exception e) {
				LOG.error("Error looking up reconfiguration range", e);
			}
		}

        return matchingRanges;
    }

    public static class ReconfigurationTable {
    	private List<ReconfigurationRange> reconfigurations;
        String table_name;
        HStoreConf conf = null;
        CatalogContext catalogContext;
        
        public ReconfigurationTable(CatalogContext catalogContext, PartitionedTable old_table, PartitionedTable new_table) throws Exception {
          this.catalogContext = catalogContext;
          table_name = old_table.table_name;
          this.conf = HStoreConf.singleton(false);
          setReconfigurations(new ArrayList<ReconfigurationRange>());
          Iterator<PartitionRange> old_ranges = old_table.partitions.iterator();
          Iterator<PartitionRange> new_ranges = new_table.partitions.iterator();

          PartitionRange new_range = new_ranges.next();
          PartitionKeyComparator cmp = new PartitionKeyComparator();
          
          Object[] max_old_accounted_for = null;
          
          PartitionRange old_range = null;
          // Iterate through the old partition ranges.
          // Only move to the next old rang
          while (old_ranges.hasNext() || (max_old_accounted_for != null && (cmp.compare(max_old_accounted_for, old_range.getMaxExcl())) != 0 )) {
            // only move to the next element if first time, or all of the previous
            // range has been accounted for
            if (old_range == null || cmp.compare(old_range.getMaxExcl(), max_old_accounted_for) <= 0) {
              old_range = old_ranges.next();
            }

            if (max_old_accounted_for == null) {
                // We have not accounted for any range yet
                max_old_accounted_for = old_range.getMinIncl();
            }
            if (old_range.compareTo(new_range) == 0) {
              if (old_range.getPartition() == new_range.getPartition()) {
                // No change do nothing
              } else {
                // Same range new partition
            	getReconfigurations().add(new ReconfigurationRange(this.table_name, old_range.getKeySchema(), old_range.getMinIncl(), old_range.getMaxExcl(),
                    old_range.getPartition(), new_range.getPartition()));
              }
              max_old_accounted_for = old_range.getMaxExcl();
              if(new_ranges.hasNext())
                  new_range = new_ranges.next();       
            } else {
              if (cmp.compare(old_range.getMaxExcl(), new_range.getMaxExcl()) <= 0) {
                // The old range is a subset of the new range
                if (old_range.getPartition() == new_range.getPartition()) {
                  // Same partitions no reconfiguration needed here
                  max_old_accounted_for = old_range.getMaxExcl();
                } else {
                  // Need to move the old range to new range
                  getReconfigurations().add(new ReconfigurationRange(this.table_name, old_range.getKeySchema(), max_old_accounted_for, old_range.getMaxExcl(),
                		  old_range.getPartition(), new_range.getPartition()));
                  max_old_accounted_for = old_range.getMaxExcl(); 
                }
                //Have we satisfied all of the new range and is there another new range to process
                if (cmp.compare(max_old_accounted_for, new_range.getMaxExcl())==0 && new_ranges.hasNext()){
                	new_range = new_ranges.next();
                }

              } else {
                // The old range is larger than this new range
                // keep getting new ranges until old range has been satisfied
                while (cmp.compare(old_range.getMaxExcl(), new_range.getMaxExcl()) > 0) {
                  if (old_range.getPartition() == new_range.getPartition()) {
                    // No need to move this range
                    max_old_accounted_for = new_range.getMaxExcl();
                  } else {
                    // move
                	getReconfigurations().add(new ReconfigurationRange(this.table_name, new_range.getKeySchema(), max_old_accounted_for, new_range.getMaxExcl(),
                    		old_range.getPartition(), new_range.getPartition()));
                    max_old_accounted_for = new_range.getMaxExcl();
                  }
                  if (new_ranges.hasNext() == false) {
                    throw new RuntimeException("Not all ranges accounted for");
                  }
                  new_range = new_ranges.next();
                }
              }

            }
          }
          if(!this.catalogContext.jarPath.getName().contains("tpcc")) { 
        	  setReconfigurations(
                  mergeReconfigurations(splitReconfigurations(getReconfigurations(),new_table.getCatalog_table()), new_table.getCatalog_table()));
          }
             
        
        }
        
        private List<ReconfigurationRange> mergeReconfigurations(List<ReconfigurationRange> reconfiguration_range, Table catalog_table) {
            if(catalog_table==null){
                LOG.debug("Catalog table is null. Not merging reconfigurations");
                return reconfiguration_range;
            }
            List<ReconfigurationRange> res = new ArrayList<>();
            
            //Check if we should merge
            if (conf == null)
                return reconfiguration_range;
            long currentMin = conf.site.reconfig_min_transfer_bytes;
            if (currentMin <= 1){
                LOG.debug(String.format("Not merging reconfiguration plan. Min transfer bytes: %s", conf.site.reconfig_min_transfer_bytes));
                return reconfiguration_range;   
            }
            
            try{
                
                long tupleBytes = MemoryEstimator.estimateTupleSize(catalog_table);
                long minRows = currentMin/tupleBytes;
                LOG.debug(String.format("Trying to merge on table:%s  TupleBytes:%s  CurrentMin:%s  MinRows:%s MinTransferBytes:%s", catalog_table.fullName(),tupleBytes,currentMin,minRows, currentMin));
                
                HashMap<String, ReconfigurationRange> rangeMap = new HashMap<>();
            	for(ReconfigurationRange range : reconfiguration_range){
                    
            		// only merge ranges that have the same old partition and same new partition
                	int old_partition = range.old_partition;
                	int new_partition = range.new_partition;
                	String key = new String(old_partition + "->" + new_partition);
                	ReconfigurationRange partialRange = rangeMap.get(key);
                	if(partialRange == null) {
                		VoltTable newMin = range.getKeySchema().clone(0);
                		VoltTable newMax = range.getKeySchema().clone(0);
                		partialRange = new ReconfigurationRange(this.table_name, range.getKeySchema(), newMin, newMax, old_partition, new_partition);
                		rangeMap.put(key, partialRange);
                	}
                	
                	Object[] max = range.getMaxExcl().get(0);
                	Object[] min = range.getMinIncl().get(0);
                    partialRange.getMaxExcl().add(max);
                    partialRange.getMinIncl().add(min);
                    long max_potential_keys = partialRange.getMaxPotentialKeys();
                    
                    // once we have reached the minimum number of rows, we can add this set of ranges to the output
                    if(max_potential_keys >= minRows) {
                    	int num_ranges = partialRange.getMaxExcl().size();
                    	if(num_ranges > 1) {
                    		LOG.debug(String.format("Merging %s ranges. Table:%s",num_ranges,table_name));
                    	}
                    	
                        res.add(partialRange);
                        partialRange = null;
                        rangeMap.remove(key);
                    }
                    
                }
                
            	// and don't forget to add the remaining sets of ranges that didn't reach the minimum number of rows
            	for(Map.Entry<String, ReconfigurationRange> rangeEntry : rangeMap.entrySet()) {
            		int num_ranges = rangeEntry.getValue().getMaxExcl().size();
                	if(num_ranges > 1) {
                		LOG.debug(String.format("Merging %s ranges. Table:%s",num_ranges,table_name));
                	}
                	
                    res.add(rangeEntry.getValue());
                }

            } catch(Exception ex){
              LOG.error("Exception merging reconfiguration ranges, returning original list",ex);  
              return reconfiguration_range;
            }

            return res;
        }
        
        private List<ReconfigurationRange> splitReconfigurations(List<ReconfigurationRange> reconfiguration_range, Table catalog_table) {
            if(catalog_table==null){
                LOG.info("Catalog table is null. Not splitting reconfigurations");
                return reconfiguration_range;
            }
            
            //Check if we should split
            if (conf == null)
                return reconfiguration_range;
            long currentMax = Math.min(conf.site.reconfig_max_transfer_bytes, conf.site.reconfig_chunk_size_kb*1024);
            if (currentMax <= 1)
                return reconfiguration_range;
            
            boolean modified = false;
            try{
                
                long tupleBytes = MemoryEstimator.estimateTupleSize(catalog_table);
                
                long maxRows = currentMax/tupleBytes;
                LOG.info(String.format("Trying to split on table:%s  TupleBytes:%s  CurrentMax:%s  MaxRows:%s MaxTransferBytes:%s", catalog_table.fullName(),tupleBytes,currentMax,maxRows, currentMax));
                
                List<ReconfigurationRange> res = new ArrayList<>();
                for(ReconfigurationRange range : reconfiguration_range){
                	long max_potential_keys = range.getMaxPotentialKeys();
                	if (max_potential_keys > maxRows){
                		long orig_max = ((Number) range.getMaxExcl().get(0)[0]).longValue();
                		long orig_min = ((Number) range.getMinIncl().get(0)[0]).longValue();
                		LOG.info(String.format("Splitting up a range %s-%s. Max row:%s. Table:%s",orig_min,orig_max,maxRows,table_name));
                		long new_max, new_min;
                		new_min = orig_min;
                		long keysRemaining = max_potential_keys;

                		//We need to split up this range
                		while(keysRemaining > 0) {
                			new_max = Math.min(new_min+maxRows,orig_max);
                			LOG.info(String.format("New range %s-%s",orig_min,new_max));
                			VoltTable max, min;
                			
                			if(new_max == orig_max) {
                				max = range.getMaxExclTable();
                			} else {
                				max = range.getKeySchema().clone(0);
                				Object[] values = new Object[max.getColumnCount()];
                				values[0] = new_max;
                				for (int i = 1; i < max.getColumnCount(); i++) {
                					VoltType vt = max.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				max.addRow(values);
                			}
                			
                			if(new_min == orig_min) {
                				min = range.getMinInclTable();
                			} else {
                				min = range.getKeySchema().clone(0);
                				Object[] values = new Object[min.getColumnCount()];
                				values[0] = new_min;
                				for (int i = 1; i < min.getColumnCount(); i++) {
                					VoltType vt = min.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				min.addRow(values);
                			}
                			
                			ReconfigurationRange newRange = new ReconfigurationRange(
                					this.table_name, range.getKeySchema(), min, max, range.old_partition, range.new_partition);
                			new_min = new_max;
                			keysRemaining-=maxRows;
                			modified = true;
                			res.add(newRange);
                		}

                	} else {
                		//This range is ok to keep
                		res.add(range);
                	}

                }

                if(modified){
                    return res;
                }
            } catch(Exception ex){
              LOG.error("Exception splitting reconfiguration ranges, returning original list",ex);  
            }

            return reconfiguration_range;
        }




        public List<ReconfigurationRange> getReconfigurations() {
            return reconfigurations;
        }




        public void setReconfigurations(List<ReconfigurationRange> reconfigurations) {
            this.reconfigurations = reconfigurations;
        }
        
        /**
         * Find the reconfiguration range for a key
         * 
         * @param id
         * @return the reconfiguration range or null if no match could be
         *         found
         */
        public ReconfigurationRange findReconfigurationRange(List<Object> ids) throws Exception {
        	try {
        		for (ReconfigurationRange r : this.reconfigurations) {
                    // if this greater than or equal to the min inclusive val
                    // and
                    // less than
                    // max_exclusive or equal to both min and max (singleton)
                    if (r.inRange(ids)) {
                    	return r;
                	}
                }
            } catch (Exception e) {
                LOG.error("Error looking up reconfiguration range", e);
            }

        	return null;
        }
      }
      
      /**
       * A partition range that holds old and new partition IDs.
       * As of 2.4.14 a range may not be non-contiguous, so a range
       * may actually hold a set of ranges. A range is the granual of 
       * migration / reconfiguration.
       * 
       * As of 5/20/14 a range may also contain a sub-range on a different key 
       * in the case of multi-column partitioning
       * 
       * @author aelmore, rytaft
       */
      public static class ReconfigurationRange implements Comparable<ReconfigurationRange> {
        private int old_partition;
        private int new_partition;
        private String table_name; 
        private VoltTable keySchema;
        private List<Object[]> min_incl;
        private List<Object[]> max_excl;
        private PartitionKeyComparator cmp;
        private Table catalog_table;
        
        public ReconfigurationRange(String table_name, VoltTable keySchema, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
        	this(table_name, keySchema, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);
        	
        	min_incl.resetRowPosition();
        	max_excl.resetRowPosition();
        	while(min_incl.advanceRow() && max_excl.advanceRow()) {
        		this.min_incl.add(min_incl.getRowArray());
        		this.max_excl.add(max_excl.getRowArray());
        	}
        }
        
        public ReconfigurationRange(Table table, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
        	this(table, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);
        	
        	min_incl.resetRowPosition();
        	max_excl.resetRowPosition();
        	while(min_incl.advanceRow() && max_excl.advanceRow()) {
        		this.min_incl.add(min_incl.getRowArray());
        		this.max_excl.add(max_excl.getRowArray());
        	}
        }
        
        public ReconfigurationRange(String table_name, VoltTable keySchema, Object[] min_incl, Object[] max_excl, int old_partition, int new_partition) {
        	this(table_name, keySchema, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);
        	
        	this.min_incl.add(min_incl);
        	this.max_excl.add(max_excl);
        }
        
        public ReconfigurationRange(Table table, Object[] min_incl, Object[] max_excl, int old_partition, int new_partition) {
        	this(table, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);
        
        	this.min_incl.add(min_incl);
        	this.max_excl.add(max_excl);
        }
        
        public ReconfigurationRange(String table_name, VoltTable keySchema, List<Object[]> min_incl, List<Object[]> max_excl, int old_partition, int new_partition) {
        	this.keySchema = keySchema;
            
            this.cmp = new PartitionKeyComparator();
            
            this.min_incl = min_incl;
            this.max_excl = max_excl;
            
            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table_name;
        }
        
        public ReconfigurationRange(Table table, List<Object[]> min_incl, List<Object[]> max_excl, int old_partition, int new_partition) {
        	this.catalog_table = table;
        	this.keySchema = ReconfigurationUtil.getPartitionKeysVoltTable(table);
        	
            this.cmp = new PartitionKeyComparator();
            
            this.min_incl = min_incl;
            this.max_excl = max_excl;
            
        	this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table.getName().toLowerCase();
        }
        
        public ReconfigurationRange clone(Table new_table) {
        	List<Object[]> minInclClone = new ArrayList<Object[]>();
        	List<Object[]> maxExclClone = new ArrayList<Object[]>();
        	minInclClone.addAll(this.min_incl);
        	maxExclClone.addAll(this.max_excl);
        	ReconfigurationRange clone = new ReconfigurationRange(this.table_name, this.keySchema.clone(0), minInclClone, 
        			maxExclClone, this.old_partition, this.new_partition);
        	clone.table_name = new_table.getName().toLowerCase();
        	clone.catalog_table = new_table;
        	return clone;
        }
        
        @Override
        public int compareTo(ReconfigurationRange o) {
        	if (cmp.compare(this.min_incl.get(0), o.min_incl.get(0)) < 0) {
        		return -1;
        	} else if (cmp.compare(this.min_incl.get(0), o.min_incl.get(0)) == 0) {
        		return cmp.compare(this.max_excl.get(0), o.max_excl.get(0));
        	} else {
        		return 1;
        	}
        }
        
        private int getNonNullCols(int row) {        	
        	int non_null_cols = 0;
            for(int i = 0; i < min_incl.get(row).length; i++) {
            	VoltType vt = keySchema.getColumnType(i);
            	if(vt.getNullValue().equals(min_incl.get(row)[i]) && vt.getNullValue().equals(max_excl.get(row)[i])) {
            		break;
            	}
            	non_null_cols++;
            }
            return non_null_cols;
        }
        
        @Override
        public String toString(){
        	String keys = "";
        	int row = 0;
        	
        	for(int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
        		Object[] min_incl_i = this.min_incl.get(i);
        		Object[] max_excl_i = this.max_excl.get(i);
        		
        		if(row != 0) {
        			keys += ", ";
        		}

        		String min_str = "";
                String max_str = "";
                for(int j = 0; j < min_incl_i.length; j++) {
            		Object min = min_incl_i[j];
            		Object max = max_excl_i[j];
            		VoltType vt = keySchema.getColumnType(j);
            		if(!vt.getNullValue().equals(min)) {
            			if(j != 0) {
            				min_str += ":";
            			}
            			min_str += min.toString();
            		}
            		if(!vt.getNullValue().equals(max)) {
            			if(j != 0) {
            				max_str += ":";
            			}
            			max_str += max.toString();
            		}
            	}
        		keys += "[" + min_str + "-" + max_str + ")";
        		row++;
        	}
        	return String.format("ReconfigRange (%s) keys:%s p_id:%s->%s ",table_name,keys,
        			old_partition,new_partition);

        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + new_partition;
            result = prime * result + old_partition;
            result = prime * result + min_incl.hashCode();
            result = prime * result + max_excl.hashCode();
            result = prime * result + ((table_name == null) ? 0 : table_name.hashCode());
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
	    }
	    if (obj == null) {
                return false;
            }
	    if (getClass() != obj.getClass()) {
                return false;
	    }

            ReconfigurationRange other = (ReconfigurationRange) obj;
            if (new_partition != other.new_partition) {
                return false;
	    }
            if (old_partition != other.old_partition) {
                return false;
	    }

            if (table_name == null) {
                if (other.table_name != null) {
                    return false;
		}
            } else if (!table_name.equals(other.table_name)) {
                return false;
	    }           

	    if (min_incl == null) {
                if (other.min_incl != null) {
                    return false;
		}
            } else {
		if(min_incl.size() != other.min_incl.size()) {
		    return false;
		}
		for(int i = 0; i < min_incl.size(); i++) {
		    if (!Arrays.asList(min_incl.get(i)).equals(Arrays.asList(other.min_incl.get(i)))) {
			return false;
		    }
		}
	    }

            if (max_excl == null) {
                if (other.max_excl != null) {
                    return false;
		}
            } else {
		if(max_excl.size() != other.max_excl.size()) {
		    return false;
		}
		for(int i = 0; i < max_excl.size(); i++) {
		    if (!Arrays.asList(max_excl.get(i)).equals(Arrays.asList(other.max_excl.get(i)))) {
			return false;
		    }
		}
	    }
	    return true;
        }
        
        public boolean inRange(List<Object> ids) {
        	Object[] keys = new Object[this.keySchema.getColumnCount()];
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
        	
        	return inRange(keys, ids.size());
        }
        
        public boolean inRange(Object[] keys, int orig_size) {
        	for(int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
        		Object[] min_incl_i = this.min_incl.get(i);
        		Object[] max_excl_i = this.max_excl.get(i);
            	if(cmp.compare(min_incl_i, keys) <= 0 && 
            			(cmp.compare(max_excl_i, keys) > 0 || 
                        (cmp.compare(min_incl_i, max_excl_i) == 0 && 
                        cmp.compare(min_incl_i, keys) == 0))){
            		if (orig_size >= getNonNullCols(i)) {
            			return true;
            		}
                }
            }
            return false;
        }
        
        public boolean inRangeIgnoreNullCols(List<Object> ids) {
        	Object[] keys = new Object[this.keySchema.getColumnCount()];
        	int col = 0;
        	for(Object id : ids) {
        		if(col >= keys.length) {
        			break;
        		}
        		keys[col] = id;
        		col++;
        	}
        	
        	return inRangeIgnoreNullCols(keys, ids.size());
        }
        
        public boolean inRangeIgnoreNullCols(Object[] keys, int orig_size) {
        	for(int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
        		Object[] min_incl_i = this.min_incl.get(i);
        		Object[] max_excl_i = this.max_excl.get(i);
        		for(int j = orig_size; j < keys.length; j++) {
        			keys[j] = min_incl_i[j];
        		}
            	if(cmp.compare(min_incl_i, keys) <= 0 && 
            			(cmp.compare(max_excl_i, keys) > 0 || 
                        (cmp.compare(min_incl_i, max_excl_i) == 0 && 
                        cmp.compare(min_incl_i, keys) == 0))){
            		return true;
                }
            }
            return false;
        }
        
        public static Object[] getKeys(List<Object> ids, Table table) {
        	VoltTable keySchema = ReconfigurationUtil.getPartitionKeysVoltTable(table);
        	Object[] keys = new Object[keySchema.getColumnCount()];
        	int col = 0;
        	for(Object id : ids) {
        		if(col >= keys.length) {
        			break;
        		}
        		keys[col] = id;
        		col++;
        	}
        	for( ; col < keySchema.getColumnCount(); col++) {
        		VoltType vt = keySchema.getColumnType(col);
            	keys[col] = vt.getNullValue();
        	}
        	
        	keySchema.addRow(keys);
        	keySchema.advanceToRow(0);
        	return keySchema.getRowArray();
        }
        
        public Long getMaxPotentialKeys() {
            ArrayList<Long> min_list = new ArrayList<>();
            ArrayList<Long> max_list = new ArrayList<>();
            for(int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
            	min_list.add(((Number) this.min_incl.get(i)[0]).longValue());
            	max_list.add(((Number) this.max_excl.get(i)[0]).longValue());
            }

        	Long max_potential_keys = 0L;
        	for(int i = 0; i < min_list.size() && i < max_list.size(); ++i) {
        		max_potential_keys += max_list.get(i) - min_list.get(i);
        	}
        	return max_potential_keys;
        }

        public int getOldPartition() {
        	return this.old_partition;
        }
        
        public int getNewPartition() {
        	return this.new_partition;
        }
        
        public String getTableName() {
        	return this.table_name;
        }
        
        public VoltTable getMinInclTable() {
        	VoltTable minInclTable = this.keySchema.clone(0);
        	for(Object[] row : this.min_incl) {
        		minInclTable.addRow(row);
        	}
        	return minInclTable;
        }
        
        public VoltTable getMaxExclTable() {
        	VoltTable maxExclTable = this.keySchema.clone(0);
        	for(Object[] row : this.max_excl) {
        		maxExclTable.addRow(row);
        	}
        	return maxExclTable;
        }
        
        public List<Object[]> getMinIncl() {
        	return this.min_incl;
        }
        
        public List<Object[]> getMaxExcl() {
        	return this.max_excl;
        }
        
        public VoltTable getKeySchema() {
        	return this.keySchema;
        }
        
        public Table getTable() {
        	return this.catalog_table;
        }
       
    }
      
      
      
      public Map<Integer, List<ReconfigurationRange>> getOutgoing_ranges() {
          return outgoing_ranges;
      }

      public Map<Integer, List<ReconfigurationRange>> getIncoming_ranges() {
    	  return incoming_ranges;
      }

      public CatalogContext getCatalogContext() {
    	  return catalogContext;
      }
      
      public Map<String, String> getPartitionedTablesByFK() {
    	  return this.partitionedTablesByFK;
      }
}
