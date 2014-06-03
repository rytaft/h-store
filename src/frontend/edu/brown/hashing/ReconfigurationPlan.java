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

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
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
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
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
    public String planDebug = "";
    
    public ReconfigurationPlan(){
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
    }
    
    public void addRange(ReconfigurationRange range){
        if(outgoing_ranges.containsKey(range.old_partition)==false){
            outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange>());
        }
        if(incoming_ranges.containsKey(range.new_partition)==false){
            incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange>());
        }
        outgoing_ranges.get(range.old_partition).add(range);
        incoming_ranges.get(range.new_partition).add(range);
    }    
    
    /**
     * @throws Exception 
     * 
     */
    public ReconfigurationPlan(PartitionPhase old_phase,PartitionPhase new_phase) throws Exception {
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        assert old_phase.tables_map.keySet().equals(new_phase.tables_map.keySet()) : "Partition plans have different tables";
        tables_map = new HashMap<String, ReconfigurationPlan.ReconfigurationTable>();
        for(String table_name : old_phase.tables_map.keySet()){
            tables_map.put(table_name, new ReconfigurationTable(old_phase.getTable(table_name), new_phase.getTable(table_name)));
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

    public static class ReconfigurationTable {
        private List<ReconfigurationRange> reconfigurations;
        String table_name;
        HStoreConf conf = null;
        
        public ReconfigurationTable(PartitionedTable old_table, PartitionedTable new_table) throws Exception {
          table_name = old_table.table_name;
          this.conf = HStoreConf.singleton(false);
          setReconfigurations(new ArrayList<ReconfigurationRange>());
          Iterator<PartitionRange> old_ranges = old_table.partitions.iterator();
          Iterator<PartitionRange> new_ranges = new_table.partitions.iterator();

          PartitionRange new_range = new_ranges.next();
          new_range.min_incl.advanceToRow(0);
          new_range.max_excl.advanceToRow(0);
          
          // get a volt table and volt table comparator
          Table table = old_table.getCatalog_table();
          VoltTable voltTable = ReconfigurationUtil.getPartitionKeysVoltTable(table);
          
          VoltTableComparator cmp = ReconfigurationUtil.getComparator(voltTable);
          
          Object[] max_old_accounted_for = null;
          
          PartitionRange old_range = null;
          // Iterate through the old partition ranges.
          // Only move to the next old rang
          while (old_ranges.hasNext() || (max_old_accounted_for != null && (cmp.compare(max_old_accounted_for, old_range.max_excl.getRowArray())) != 0 )) {
            // only move to the next element if first time, or all of the previous
            // range has been accounted for
            if (old_range == null || cmp.compare(old_range.max_excl.getRowArray(), max_old_accounted_for) <= 0) {
              old_range = old_ranges.next();
              old_range.min_incl.advanceToRow(0);
              old_range.max_excl.advanceToRow(0);
            }

            if (max_old_accounted_for == null) {
                // We have not accounted for any range yet
                max_old_accounted_for = old_range.min_incl.getRowArray();
            }
            if (old_range.compareTo(new_range) == 0) {
              if (old_range.partition == new_range.partition) {
                // No change do nothing
              } else {
                // Same range new partition
            	getReconfigurations().add(new ReconfigurationRange(this.table_name, old_range.getClone(), old_range.min_incl, old_range.max_excl,
                    old_range.partition, new_range.partition));
              }
              max_old_accounted_for = old_range.max_excl.getRowArray();
              if(new_ranges.hasNext())
                  new_range = new_ranges.next();
              	  new_range.min_incl.advanceToRow(0);
              	  new_range.max_excl.advanceToRow(0);         
            } else {
              if (cmp.compare(old_range.max_excl.getRowArray(), new_range.max_excl.getRowArray()) <= 0) {
                // The old range is a subset of the new range
                if (old_range.partition == new_range.partition) {
                  // Same partitions no reconfiguration needed here
                  max_old_accounted_for = old_range.max_excl.getRowArray();
                } else {
                  // Need to move the old range to new range
                  VoltTable new_min = voltTable.clone(0);
                  new_min.addRow(max_old_accounted_for);
                  getReconfigurations().add(new ReconfigurationRange(this.table_name, old_range.getClone(), new_min, old_range.max_excl,
                      old_range.partition, new_range.partition));
                  max_old_accounted_for = old_range.max_excl.getRowArray(); 
                }
                //Have we satisfied all of the new range and is there another new range to process
                if (cmp.compare(max_old_accounted_for, new_range.max_excl.getRowArray())==0 && new_ranges.hasNext()){
                	new_range = new_ranges.next();
                	new_range.min_incl.advanceToRow(0);
                  	new_range.max_excl.advanceToRow(0); 
                }

              } else {
                // The old range is larger than this new range
                // keep getting new ranges until old range has been satisfied
                while (cmp.compare(old_range.max_excl.getRowArray(), new_range.max_excl.getRowArray()) > 0) {
                  if (old_range.partition == new_range.partition) {
                    // No need to move this range
                    max_old_accounted_for = new_range.max_excl.getRowArray();
                  } else {
                    // move
                	VoltTable new_min = voltTable.clone(0);
                    new_min.addRow(max_old_accounted_for);
                    getReconfigurations().add(new ReconfigurationRange(this.table_name, new_range.getClone(), new_min, new_range.max_excl,
                        old_range.partition, new_range.partition));
                    max_old_accounted_for = new_range.max_excl.getRowArray();
                  }
                  if (new_ranges.hasNext() == false) {
                    throw new RuntimeException("Not all ranges accounted for");
                  }
                  new_range = new_ranges.next();
              	  new_range.min_incl.advanceToRow(0);
              	  new_range.max_excl.advanceToRow(0); 
                }
              }

            }
          }
          setReconfigurations(
                  mergeReconfigurations(splitReconfigurations(getReconfigurations(),new_table.getCatalog_table()), new_table.getCatalog_table()));
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
                		VoltTable newMin = range.clone.clone(0);
                		VoltTable newMax = range.clone.clone(0);
                		partialRange = new ReconfigurationRange(this.table_name, range.getClone(), newMin, newMax, old_partition, new_partition);
                		rangeMap.put(key, partialRange);
                	}
                	
                	VoltTable max = range.getMaxExcl();
                	VoltTable min = range.getMinIncl();
                    partialRange.getMaxExcl().add(max);
                    partialRange.getMinIncl().add(min);
                    long max_potential_keys = partialRange.getMaxPotentialKeys();
                    
                    // once we have reached the minimum number of rows, we can add this set of ranges to the output
                    if(max_potential_keys >= minRows) {
                    	int num_ranges = partialRange.getMaxExcl().getRowCount();
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
            		int num_ranges = rangeEntry.getValue().getMaxExcl().getRowCount();
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
                		range.getMaxExcl().advanceToRow(0);
                		range.getMinIncl().advanceToRow(0);
                		long orig_max = range.getMaxExcl().getLong(0);
                		long orig_min = range.getMinIncl().getLong(0);
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
                				max = range.getMaxExcl();
                			} else {
                				max = range.getClone().clone(0);
                				Object[] values = new Object[max.getColumnCount()];
                				values[0] = new_max;
                				for (int i = 1; i < max.getColumnCount(); i++) {
                					VoltType vt = max.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				max.addRow(values);
                			}
                			
                			if(new_min == orig_min) {
                				min = range.getMinIncl();
                			} else {
                				min = range.getClone().clone(0);
                				Object[] values = new Object[min.getColumnCount()];
                				values[0] = new_min;
                				for (int i = 1; i < min.getColumnCount(); i++) {
                					VoltType vt = min.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				min.addRow(values);
                			}
                			
                			ReconfigurationRange newRange = new ReconfigurationRange(
                					this.table_name, range.getClone(), min, max, range.old_partition, range.new_partition);
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
      public static class ReconfigurationRange extends PartitionRange {
        private int old_partition;
        private int new_partition;
        private String table_name; 
        
        public ReconfigurationRange(String table_name, VoltTable clone, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
            super(table_name, clone, min_incl, max_excl);
            
            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table_name;
        }
        
        public ReconfigurationRange(Table table, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
        	super(table, min_incl, max_excl);
        	
        	this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table.getName().toLowerCase();
        }
        
        public ReconfigurationRange(Table table, List<Object> min_incl, List<Object> max_excl, int old_partition, int new_partition) { 
        	super(table, min_incl, max_excl);
            
            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table.getName().toLowerCase();
        }
        
        @Override
        public String toString(){
        	this.min_incl.resetRowPosition();
        	this.max_excl.resetRowPosition();
        	String keys = "";
        	int row = 0;
        	
        	while(this.min_incl.advanceRow() && this.max_excl.advanceRow()) {
        		if(row != 0) {
        			keys += ", ";
        		}

        		String min_str = "";
                String max_str = "";
                for(int i = 0; i < this.min_incl.getColumnCount(); i++) {
            		Object min = this.min_incl.get(i);
            		Object max = this.max_excl.get(i);
            		VoltType vt = this.min_incl.getColumnType(i);
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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ReconfigurationRange other = (ReconfigurationRange) obj;
            if (new_partition != other.new_partition)
                return false;
            if (old_partition != other.old_partition)
                return false;
            if (table_name == null) {
                if (other.table_name != null)
                    return false;
            } else if (!table_name.equals(other.table_name))
                return false;
            if (min_incl == null) {
                if (other.min_incl != null)
                    return false;
            } else if (!min_incl.equals(other.min_incl))
                return false;
            if (max_excl == null) {
                if (other.max_excl != null)
                    return false;
            } else if (!max_excl.equals(other.max_excl))
                return false;
            return true;
        }

        public Long getMaxPotentialKeys() {
            this.min_incl.resetRowPosition();
            this.max_excl.resetRowPosition();
            ArrayList<Long> min_list = new ArrayList<>();
            ArrayList<Long> max_list = new ArrayList<>();
            while(this.min_incl.advanceRow() && this.max_excl.advanceRow()) {
            	min_list.add(this.min_incl.getLong(0));
            	max_list.add(this.max_excl.getLong(0));
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
        
    }
      
      
      
      public Map<Integer, List<ReconfigurationRange>> getOutgoing_ranges() {
          return outgoing_ranges;
      }

      public Map<Integer, List<ReconfigurationRange>> getIncoming_ranges() {
          return incoming_ranges;
      }
}
