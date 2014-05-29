/**
 * 
 */
package edu.brown.hashing;

import java.text.ParseException;
import java.util.ArrayList;
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
import edu.brown.utils.CompositeKey;

/**
 * The delta between two partition plans
 * @author aelmore
 *
 */
public class ReconfigurationPlan {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPlan.class);
    protected Map<String,ReconfigurationTable<? extends Comparable<?>>> tables_map;
    
    //Helper map of partition ID and outgoing/incoming ranges for this reconfiguration
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> outgoing_ranges;
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> incoming_ranges;
    public String planDebug = "";
    
    public ReconfigurationPlan(){
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
    }
    
    public void addRange(ReconfigurationRange<?> range){
        if(outgoing_ranges.containsKey(range.old_partition)==false){
            outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
        }
        if(incoming_ranges.containsKey(range.new_partition)==false){
            incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
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
        tables_map = new HashMap<String, ReconfigurationPlan.ReconfigurationTable<? extends Comparable<?>>>();
        for(String table_name : old_phase.tables_map.keySet()){
            tables_map.put(table_name, new ReconfigurationTable(old_phase.getTable(table_name), new_phase.getTable(table_name)));
        }
        registerReconfigurationRanges();
        planDebug = String.format("Reconfiguration plan generated \n Out: %s \n In: %s",outgoing_ranges.toString(),incoming_ranges.toString());
        LOG.info(planDebug);
    }
    
    protected void registerReconfigurationRanges(){
        for(String table_name : tables_map.keySet()){
            for(ReconfigurationRange<?> range : tables_map.get(table_name).getReconfigurations()){
                addRange(range);
            }
        }
    }

    public static class ReconfigurationTable<T extends Comparable<T>> {
        private List<ReconfigurationRange<T>> reconfigurations;
        String table_name;
        HStoreConf conf = null;
        
        public ReconfigurationTable(PartitionedTable<T> old_table, PartitionedTable<T> new_table) throws Exception {
          table_name = old_table.table_name;
          this.conf = HStoreConf.singleton(false);
          setReconfigurations(new ArrayList<ReconfigurationRange<T>>());
          Iterator<PartitionRange<T>> old_ranges = old_table.partitions.iterator();
          Iterator<PartitionRange<T>> new_ranges = new_table.partitions.iterator();

          PartitionRange<T> new_range = new_ranges.next();
          
          // get a volt table and volt table comparator
          Table table = old_table.getCatalog_table();
          ArrayList<Column> cols = new ArrayList<Column>(table.getPartitioncolumns().size());
          for(ColumnRef colRef : table.getPartitioncolumns()) {
          	cols.add(colRef.getIndex(), colRef.getColumn());
          }
          VoltTable voltTable = CatalogUtil.getVoltTable(cols);
          
          ArrayList<Pair<Integer, SortDirectionType>> sortCol = new ArrayList<Pair<Integer, SortDirectionType>>();
          for(int i = 0; i < voltTable.getColumnCount(); i++) {
          	sortCol.add(Pair.of(i, SortDirectionType.ASC));
          }
          VoltTableComparator cmp = new VoltTableComparator(voltTable, (Pair<Integer, SortDirectionType>[]) sortCol.toArray());
          
          // get a row representing the max accounted for
          // We have not accounted for any range yet
          Object[] max_old_accounted_for = new Object[voltTable.getColumnCount()];
          int non_null_cols = 0;
          for (int i = 0 ; i < voltTable.getColumnCount(); i++) {
          	VoltType vt = voltTable.getColumnType(i);
          	max_old_accounted_for[i] = vt.getNullValue();
          }
          
          PartitionRange<T> old_range = null;
          // Iterate through the old partition ranges.
          // Only move to the next old rang
          while (old_ranges.hasNext() || (old_range != null && (cmp.compare(max_old_accounted_for, old_range.max_excl.getRowArray())) != 0 )) {
            // only move to the next element if first time, or all of the previous
            // range has been accounted for
            if (old_range == null || cmp.compare(old_range.max_excl.getRowArray(), max_old_accounted_for) <= 0) {
              old_range = old_ranges.next();
            }

            if (old_range.compareTo(new_range) == 0) {
              if (old_range.partition == new_range.partition) {
                // No change do nothing
              } else {
                // Same range new partition
            	getReconfigurations().add(new ReconfigurationRange<T>(old_range.getTable(), old_range.min_incl, old_range.max_excl, old_range.non_null_cols,
                    old_range.partition, new_range.partition));
              }
              max_old_accounted_for = old_range.max_excl.getRowArray();
              non_null_cols = old_range.non_null_cols;
              if(new_ranges.hasNext())
                  new_range = new_ranges.next();
            } else {
              if (cmp.compare(old_range.max_excl.getRowArray(), new_range.max_excl.getRowArray()) <= 0) {
                // The old range is a subset of the new range
                if (old_range.partition == new_range.partition) {
                  // Same partitions no reconfiguration needed here
                  max_old_accounted_for = old_range.max_excl.getRowArray();
                  non_null_cols = old_range.non_null_cols;
                } else {
                  // Need to move the old range to new range
                  VoltTable new_min = voltTable.clone(0);
                  new_min.addRow(max_old_accounted_for);
                  getReconfigurations().add(new ReconfigurationRange<T>(old_range.getTable(), new_min, old_range.max_excl, Math.max(old_range.non_null_cols, non_null_cols),
                      old_range.partition, new_range.partition));
                  max_old_accounted_for = old_range.max_excl.getRowArray();    
                  non_null_cols = old_range.non_null_cols;
                }
                //Have we satisfied all of the new range and is there another new range to process
                if (cmp.compare(max_old_accounted_for, new_range.max_excl.getRowArray())==0 && new_ranges.hasNext()){
                	new_range = new_ranges.next();
                }

              } else {
                // The old range is larger than this new range
                // keep getting new ranges until old range has been satisfied
                while (cmp.compare(old_range.max_excl.getRowArray(), new_range.max_excl.getRowArray()) > 0) {
                  if (old_range.partition == new_range.partition) {
                    // No need to move this range
                    max_old_accounted_for = new_range.max_excl.getRowArray();
                    non_null_cols = new_range.non_null_cols;
                  } else {
                    // move
                	VoltTable new_min = voltTable.clone(0);
                    new_min.addRow(max_old_accounted_for);
                    getReconfigurations().add(new ReconfigurationRange<T>(new_range.getTable(), new_min, new_range.max_excl, Math.max(new_range.non_null_cols, non_null_cols),
                        old_range.partition, new_range.partition));
                    max_old_accounted_for = new_range.max_excl.getRowArray();
                    non_null_cols = new_range.non_null_cols;
                  }
                  if (new_ranges.hasNext() == false) {
                    throw new RuntimeException("Not all ranges accounted for");
                  }
                  new_range = new_ranges.next();
                }
              }

            }
          }
          setReconfigurations(
                  mergeReconfigurations(splitReconfigurations(getReconfigurations(),new_table.getCatalog_table()), new_table.getCatalog_table()));
        }
        
        private List<ReconfigurationRange<T>> mergeReconfigurations(List<ReconfigurationRange<T>> reconfiguration_range, Table catalog_table) {
            if(catalog_table==null){
                LOG.debug("Catalog table is null. Not merging reconfigurations");
                return reconfiguration_range;
            }
            List<ReconfigurationRange<T>> res = new ArrayList<>();
            
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
                
                HashMap<String, ReconfigurationRange<T>> rangeMap = new HashMap<>();
            	for(ReconfigurationRange<T> range : reconfiguration_range){
                    
            		// only merge ranges that have the same old partition and same new partition
                	int old_partition = range.old_partition;
                	int new_partition = range.new_partition;
                	String key = new String(old_partition + "->" + new_partition);
                	ReconfigurationRange<T> partialRange = rangeMap.get(key);
                	if(partialRange == null) {
                		VoltTable newMin = range.clone.clone(0);
                		VoltTable newMax = range.clone.clone(0);
                		partialRange = new ReconfigurationRange<T>(range.getTable(), newMin, newMax, 0, old_partition, new_partition);
                		rangeMap.put(key, partialRange);
                	}
                	
                	VoltTable max = range.getMaxExcl();
                	VoltTable min = range.getMinIncl();
                    partialRange.getMaxExcl().add(max);
                    partialRange.getMinIncl().add(min);
                    partialRange.non_null_cols = Math.max(partialRange.non_null_cols, range.non_null_cols);
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
            	for(Map.Entry<String, ReconfigurationRange<T>> rangeEntry : rangeMap.entrySet()) {
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
        
        private List<ReconfigurationRange<T>> splitReconfigurations(List<ReconfigurationRange<T>> reconfiguration_range, Table catalog_table) {
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
                
                List<ReconfigurationRange<T>> res = new ArrayList<>();
                for(ReconfigurationRange<T> range : reconfiguration_range){
                	long max_potential_keys = range.getMaxPotentialKeys();
                	if (max_potential_keys > maxRows){
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
                			int non_null_cols = 0;
                			
                			if(new_max == orig_max) {
                				max = range.getMaxExcl();
                				non_null_cols = Math.max(non_null_cols, range.non_null_cols);
                			} else {
                				max = range.getClone().clone(0);
                				Object[] values = new Object[max.getColumnCount()];
                				values[0] = new_max;
                				for (int i = 1; i < max.getColumnCount(); i++) {
                					VoltType vt = max.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				max.addRow(values);
                				non_null_cols = Math.max(non_null_cols, 1);
                			}
                			
                			if(new_min == orig_min) {
                				min = range.getMinIncl();
                				non_null_cols = Math.max(non_null_cols, range.non_null_cols);
                			} else {
                				min = range.getClone().clone(0);
                				Object[] values = new Object[min.getColumnCount()];
                				values[0] = new_min;
                				for (int i = 1; i < min.getColumnCount(); i++) {
                					VoltType vt = min.getColumnType(i);
                					values[i] = vt.getNullValue();
                				}
                				min.addRow(values);
                				non_null_cols = Math.max(non_null_cols, 1);
                			}
                			
                			ReconfigurationRange<T> newRange = new ReconfigurationRange<T>(
                					range.getTable(), min, max, non_null_cols, range.old_partition, range.new_partition);
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




        public List<ReconfigurationRange<T>> getReconfigurations() {
            return reconfigurations;
        }




        public void setReconfigurations(List<ReconfigurationRange<T>> reconfigurations) {
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
       * @author aelmore
       * 
       * @param <T>
       */
      public static class ReconfigurationRange<T extends Comparable<T>> extends PartitionRange<T> {
        public int old_partition;
        public int new_partition;
        private List<Pair<Long,Long>> ranges;
        private List<Long> min_list;
        private List<Long> max_list;
        
        //To reduce visibility from PartitionRange as this can have multiple ranges
        private T min_inclusive;
        private Long min_inclusive_long;
        private T max_exclusive;
        private Long max_exclusive_long;
        
        public String table_name;
        private boolean single_range = false; //single range or multi
        
        // new stuff!
        private VoltTable clone;
        private VoltTable min_incl;
        private VoltTable max_excl;
        private int non_null_cols;
        private VoltTableComparator cmp;
        private Table catalog_table;
        // end new stuff 
        
        public ReconfigurationRange(String table_name, VoltType vt, T min_inclusive, T max_exclusive, int old_partition, int new_partition)  {
          super(vt, min_inclusive, max_exclusive);
          //FIXME change to be type generic

          this.min_inclusive = min_inclusive;
          this.max_exclusive = max_exclusive;
          min_inclusive_long = ((Number)min_inclusive).longValue();
          max_exclusive_long = ((Number)max_exclusive).longValue();
          Pair<Long,Long> minMax = new Pair<Long, Long>(min_inclusive_long, max_exclusive_long);
          ranges = new ArrayList<>();
          ranges.add(minMax);
          
          min_list = new ArrayList<>();
          min_list.add(min_inclusive_long);
          max_list = new ArrayList<>();
          max_list.add(max_exclusive_long);
          
          this.old_partition = old_partition;
          this.new_partition = new_partition;
          this.table_name = table_name;
          this.single_range = true;
        }
        
        public ReconfigurationRange(String table_name, VoltType vt, List<Long> min_inclusive, List<Long> max_exclusive, int old_partition, int new_partition) {
            super(vt);
            //FIXME change to be type generic

            ranges = new ArrayList<>();
            for(int i = 0; i < min_inclusive.size() && i < max_exclusive.size(); ++i) {
            	Pair<Long,Long> minMax = new Pair<Long, Long>(min_inclusive.get(i), max_exclusive.get(i));
                ranges.add(minMax);
            }
            
            min_list = new ArrayList<>();
            min_list.addAll(min_inclusive);
            max_list = new ArrayList<>();
            max_list.addAll(max_exclusive);
            
            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table_name;
            this.single_range = false;
        }
        
//        public ReconfigurationRange(String table_name, String column_name, VoltType vt, T min_inclusive, T max_exclusive, 
//        		int old_partition, int new_partition, ReconfigurationRange<?> sub_range) {
//        	this(table_name, vt, min_inclusive, max_exclusive, old_partition, new_partition);
//        	if(sub_range != null) {
//        		assert(sub_range.old_partition == old_partition);
//        		assert(sub_range.new_partition == new_partition);
//        		assert(min_inclusive.equals(max_exclusive) || 
//        				min_inclusive.equals((Long)max_exclusive - 1));
//        	}
//        	this.sub_range = sub_range;
//        	this.column_name = column_name;
//        }
//        
//        public ReconfigurationRange(String table_name, String column_name, VoltType vt, List<Long> min_inclusive, List<Long> max_exclusive, 
//        		int old_partition, int new_partition, ReconfigurationRange<?> sub_range) {
//        	this(table_name, vt, min_inclusive, max_exclusive, old_partition, new_partition);
//        	if(sub_range != null) {
//        		assert(sub_range.old_partition == old_partition);
//        		assert(sub_range.new_partition == new_partition);
//        		assert(min_inclusive.size() == 1 && max_exclusive.size() == 1);
//        		assert(min_inclusive.get(0).equals(max_exclusive.get(0)) || 
//        				min_inclusive.get(0).equals(max_exclusive.get(0) - 1));
//        	}
//        	this.sub_range = sub_range;
//        	this.column_name = column_name;
//        }
        
        
        public ReconfigurationRange(Table table, VoltTable min_incl, VoltTable max_excl, int non_null_cols, int old_partition, int new_partition) {
            super(VoltType.BIGINT);
            
            this.catalog_table = table;
            ArrayList<Column> cols = new ArrayList<Column>(table.getPartitioncolumns().size());
            for(ColumnRef colRef : table.getPartitioncolumns()) {
            	cols.add(colRef.getIndex(), colRef.getColumn());
            }
            this.clone = CatalogUtil.getVoltTable(cols);
            
            ArrayList<Pair<Integer, SortDirectionType>> sortCol = new ArrayList<Pair<Integer, SortDirectionType>>();
            for(int i = 0; i < this.clone.getColumnCount(); i++) {
            	sortCol.add(Pair.of(i, SortDirectionType.ASC));
            }
            this.cmp = new VoltTableComparator(this.clone, (Pair<Integer, SortDirectionType>[]) sortCol.toArray());
            
            this.min_incl = min_incl;
            this.max_excl = max_excl;
            this.non_null_cols = non_null_cols;
            
            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.single_range = false;
        }

        @Override
        public String toString(){
        	if(min_incl != null && max_excl != null) {
        		this.min_incl.resetRowPosition();
                this.max_excl.resetRowPosition();
                String keys = "";
                int row = 0;
                while(this.min_incl.advanceRow() && this.max_excl.advanceRow()) {
                	if(row != 0) {
        				keys += ", ";
        			}
                	
                	String range_str = "";
            		for(int i = 0; i < this.non_null_cols; i++) {
            			if(i != 0) {
            				range_str += ":";
            			}
            			Object min = this.min_incl.get(i);
            			Object max = this.max_excl.get(i);
            			if(min.equals(max)) {
            				range_str += min.toString();
            			} else {
            				range_str += min.toString() + "-" + max.toString();
            			}
            		}
        			keys += "[" + range_str + ")";
        			row++;
                }
                return String.format("ReconfigRange (%s) keys:%s p_id:%s->%s ",table_name,keys,
        				old_partition,new_partition);
        	} else if(min_inclusive != null && max_exclusive != null) {
        		return String.format("ReconfigRange (%s) keys:[%s,%s) p_id:%s->%s ",table_name,
        				min_inclusive,max_exclusive,old_partition,new_partition);
        	}
        	else {
        		String keys = "";
        		for(int i = 0; i < min_list.size() && i < max_list.size(); ++i) {
        			if(i != 0) {
        				keys += ", ";
        			}
        			keys += "[" + min_list.get(i) + "," + max_list.get(i) + ")";
        		}
        		return String.format("ReconfigRange (%s) keys:%s p_id:%s->%s ",table_name,keys,
        				old_partition,new_partition);
        	}
        }
        
        public boolean inRange(Comparable<?> key){
        	try{
                long keyL = ((Number)key).longValue();
                for(Pair<Long,Long> range : ranges) {
                    long min_long = range.getFirst();
                    long max_long = range.getSecond();
                    if(min_long <= keyL && (max_long > keyL || 
                            (max_long == min_long && min_long == keyL))){
                        return true;
                    }
                }
            } catch(Exception e){
                LOG.error("TODO only number keys supported");
                LOG.error(e);
            }
            return false;
        }
        
//        // for multi-column partitioning
//        public boolean inRange(List<Comparable<?>> keys){
//            try{
//            	List<Pair<Long,Long>> r = this.ranges;
//            	boolean inRange = false;
//            	for(Comparable<?> key : keys) {
//	                long keyL = ((Number)key).longValue();
//	                for(Pair<Long,Long> range : r) {
//	                    long min_long = range.getFirst();
//	                    long max_long = range.getSecond();
//	                    if(min_long <= keyL && (max_long > keyL || 
//	                            (max_long == min_long && min_long == keyL))){
//	                        inRange = true;
//	                        break;
//	                    } else {
//	                    	return false;
//	                    }
//	                }
//	                if(this.sub_range == null) {
//	                	break;
//	                } else {
//	                	r = this.sub_range.ranges;
//	                }
//            	}
//            	return inRange;
//            } catch(Exception e){
//                LOG.error("TODO only number keys supported");
//                LOG.error(e);
//            }
//            return false;
//        }
        
        public boolean inRange(Object[] keys) {
        	this.min_incl.resetRowPosition();
            this.max_excl.resetRowPosition();
            while(this.min_incl.advanceRow() && this.max_excl.advanceRow()) {
            	if(cmp.compare(min_incl.getRowArray(), keys) <= 0 && 
            			(cmp.compare(max_excl.getRowArray(), keys) > 0 || 
                        (cmp.compare(min_incl.getRowArray(), max_excl.getRowArray()) == 0 && 
                        cmp.compare(min_incl.getRowArray(), keys) == 0))){
                    return true;
                }
            }
            return false;
        }
        
        @SuppressWarnings("unchecked")
        public <T> T castKey(Comparable<?> key){
            return (T)key;
        }
        
       

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + new_partition;
            result = prime * result + old_partition;
            result = prime * result + ((ranges == null) ? 0 : ranges.hashCode());
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
            if (ranges == null) {
                if (other.ranges != null)
                    return false;
            } else if (!ranges.equals(other.ranges))
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
            if (non_null_cols != other.non_null_cols)
                return false;
            return true;
        }

        public List<Long> getMinList() {
            return min_list;
        }

        public List<Long> getMaxList() {
            return max_list;
        }
        
        public Long getMaxPotentialKeys() {
            if(min_incl != null && max_excl != null) {
            	this.min_incl.resetRowPosition();
            	this.max_excl.resetRowPosition();
            	min_list = new ArrayList<>();
            	max_list = new ArrayList<>();
            	while(this.min_incl.advanceRow() && this.max_excl.advanceRow()) {
            		min_list.add(this.min_incl.getLong(0));
            		max_list.add(this.max_excl.getLong(0));
            	}
        	}
        	
        	Long max_potential_keys = 0L;
        	for(int i = 0; i < min_list.size() && i < max_list.size(); ++i) {
        		max_potential_keys += max_list.get(i) - min_list.get(i);
        	}
        	return max_potential_keys;
        }

        public T getMin_inclusive() {
            if(!single_range) 
                throw new RuntimeException("Trying to get min_inclusive when multiple ranges exists");
            return min_inclusive;
        }

        public T getMax_exclusive() {
            if(!single_range) 
                throw new RuntimeException("Trying to get max_exclusive when multiple ranges exists");
            return max_exclusive;
        } 
        
        public boolean isSingleRange() {
        	return single_range;
        }
        
        public VoltTable getMinIncl() {
        	return this.min_incl;
        }
        
        public VoltTable getMaxExcl() {
        	return this.max_excl;
        }
        
        public VoltTable getClone() {
        	return this.clone;
        }
        
        public int getNonNullCols() {
        	return this.non_null_cols;
        }
        
        public Table getTable() {
        	return this.catalog_table;
        }
        
    }
      

      
      
    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getOutgoing_ranges() {
        return outgoing_ranges;
    }

    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getIncoming_ranges() {
        return incoming_ranges;
    }
}
