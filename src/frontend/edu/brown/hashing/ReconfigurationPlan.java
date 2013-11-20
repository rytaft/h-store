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
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;

import edu.brown.designer.MemoryEstimator;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;

/**
 * The delta between two partition plans
 * @author aelmore
 *
 */
public class ReconfigurationPlan {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPlan.class);
    Map<String,ReconfigurationTable<? extends Comparable<?>>> tables_map;
    
    //Helper map of partition ID and outgoing/incoming ranges for this reconfiguration
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> outgoing_ranges;
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> incoming_ranges;
    
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
        LOG.info(String.format("Reconfiguration plan generated \n Out: %s \n In: %s",outgoing_ranges.toString(),incoming_ranges.toString()));
    }
    
    protected void registerReconfigurationRanges(){
        for(String table_name : tables_map.keySet()){
            for(ReconfigurationRange<?> range : tables_map.get(table_name).getReconfigurations()){
                if(outgoing_ranges.containsKey(range.old_partition)==false){
                    outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
                }
                if(incoming_ranges.containsKey(range.new_partition)==false){
                    incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
                }
                outgoing_ranges.get(range.old_partition).add(range);
                incoming_ranges.get(range.new_partition).add(range);
            }
        }
    }

    public static class ReconfigurationTable<T extends Comparable<T>> {
        private List<ReconfigurationRange<T>> reconfigurations;
        String table_name;
        public ReconfigurationTable(PartitionedTable<T> old_table, PartitionedTable<T> new_table) throws Exception {
          table_name = old_table.table_name;
          setReconfigurations(new ArrayList<ReconfigurationRange<T>>());
          Iterator<PartitionRange<T>> old_ranges = old_table.partitions.iterator();
          Iterator<PartitionRange<T>> new_ranges = new_table.partitions.iterator();

          PartitionRange<T> new_range = new_ranges.next();
          T max_old_accounted_for = null;
          PartitionRange<T> old_range = null;
          // Iterate through the old partition ranges.
          // Only move to the next old rang
          while (old_ranges.hasNext() || (max_old_accounted_for != null && max_old_accounted_for.compareTo(old_range.max_exclusive)!=0) ) {
            // only move to the next element if first time, or all of the previous
            // range has been accounted for
            if (old_range == null || old_range.max_exclusive.compareTo(max_old_accounted_for) <= 0) {
              old_range = old_ranges.next();
            }

            if (max_old_accounted_for == null) {
              // We have not accounted for any range yet
              max_old_accounted_for = old_range.min_inclusive;
            }
            if (old_range.compareTo(new_range) == 0) {
              if (old_range.partition == new_range.partition) {
                // No change do nothing
              } else {
                // Same range new partition
                getReconfigurations().add(new ReconfigurationRange<T>(table_name, old_range.vt, old_range.min_inclusive, old_range.max_exclusive,
                    old_range.partition, new_range.partition));
              }
              max_old_accounted_for = old_range.max_exclusive;
              if(new_ranges.hasNext())
                  new_range = new_ranges.next();
            } else {
              if (old_range.max_exclusive.compareTo(new_range.max_exclusive) <= 0) {
                // The old range is a subset of the new range
                if (old_range.partition == new_range.partition) {
                  // Same partitions no reconfiguration needed here
                  max_old_accounted_for = old_range.max_exclusive;
                } else {
                  // Need to move the old range to new range
                  getReconfigurations().add(new ReconfigurationRange<T>(table_name, old_range.vt, max_old_accounted_for, old_range.max_exclusive,
                      old_range.partition, new_range.partition));
                  max_old_accounted_for = old_range.max_exclusive;
                  
                  //Have we satisfied all of the new range and is there another new range to process
                  if (max_old_accounted_for.compareTo(new_range.max_exclusive)==0 && new_ranges.hasNext()){
                    new_range = new_ranges.next();
                  }
                }
              } else {
                // The old range is larger than this new range
                // keep getting new ranges until old range has been satisfied
                while (old_range.max_exclusive.compareTo(new_range.max_exclusive) > 0) {
                  if (old_range.partition == new_range.partition) {
                    // No need to move this range
                    max_old_accounted_for = new_range.max_exclusive;
                  } else {
                    // move
                    getReconfigurations().add(new ReconfigurationRange<T>(table_name, old_range.vt, max_old_accounted_for, new_range.max_exclusive,
                        old_range.partition, new_range.partition));
                    max_old_accounted_for = new_range.max_exclusive;
                  }
                  if (new_ranges.hasNext() == false) {
                    throw new RuntimeException("Not all ranges accounted for");
                  }
                  new_range = new_ranges.next();
                }
              }

            }
          }
          setReconfigurations(splitReconfigurations(getReconfigurations(),new_table.getCatalog_table()));
        }
        
        
        
        
        private List<ReconfigurationRange<T>> splitReconfigurations(List<ReconfigurationRange<T>> reconfiguration_range, Table catalog_table) {
            if(catalog_table==null){
                LOG.info("Catalog table is null. Not splitting reconfigurations");
                return reconfiguration_range;
            }
            boolean modified = false;
            try{
                
                long tupleBytes = MemoryEstimator.estimateTupleSize(catalog_table);
                long currentMax = ReconfigurationConstants.MAX_TRANSFER_BYTES;
                long maxRows = currentMax/tupleBytes;
                LOG.info(String.format("Trying to split on table:%s  TupleBytes:%s  CurrentMax:%s  MaxRows:%s", catalog_table.fullName(),tupleBytes,currentMax,maxRows));
                
                List<ReconfigurationRange<T>> res = new ArrayList<>();
                Comparable<?> sampleKey = reconfiguration_range.get(0).getMin_inclusive() ;
                if (sampleKey instanceof Short || sampleKey instanceof Integer || sampleKey instanceof Long  ){
                    for(ReconfigurationRange<T> range : reconfiguration_range){
                        long max = ((Number)range.max_exclusive).longValue();
                        long min = ((Number)range.min_inclusive).longValue();
                        long max_potential_keys = max - min;
                        if (max_potential_keys > maxRows){
                            LOG.info(String.format("Splitting up a range %s-%s. Max row:%s. Table:%s",min,max,maxRows,table_name));
                            long orig_max = max;
                            Class<?> keyclass =  sampleKey.getClass();
                            long keysRemaining = max_potential_keys;

                            //We need to split up this range
                            while(keysRemaining > 0) {
                                max = Math.min(min+maxRows,orig_max);
                                LOG.info(String.format("New range %s-%s",min,max));
                                ReconfigurationRange<T> newRange = new ReconfigurationRange<T>(
                                        range.table_name, range.vt, (T)keyclass.cast(min), (T)keyclass.cast(max), range.old_partition, range.new_partition);
                                min = max;
                                keysRemaining-=maxRows;
                                modified = true;
                                res.add(newRange);
                            }
                            
                        } else {
                            //This range is ok to keep
                            res.add(range);
                        }
                        
                    }
                } else{
                
                    throw new NotImplementedException("Can only handle types of small, long, int. Class: " +sampleKey.getClass().getName());
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
       * A partition range that holds old and new partition IDs
       * 
       * @author aelmore
       * 
       * @param <T>
       */
      public static class ReconfigurationRange<T extends Comparable<T>> extends PartitionRange<T> {
        public int old_partition;
        public int new_partition;
        public Long min_long;
        public Long max_long; 
        public String table_name;

        public ReconfigurationRange(String table_name, VoltType vt, T min_inclusive, T max_exclusive, int old_partition, int new_partition)  {
          super(vt, min_inclusive, max_exclusive);
          //FIXME change to be type generic
          min_long = ((Number)min_inclusive).longValue();
          max_long = ((Number)max_exclusive).longValue();
          this.old_partition = old_partition;
          this.new_partition = new_partition;
          this.table_name = table_name;
        }
        
        @Override
        public String toString(){
          return String.format("ReconfigRange (%s)  [%s,%s) id:%s->%s ",table_name,min_inclusive,max_exclusive,old_partition,new_partition);
        }
        
        //FIXME Ugh this needs to be fixed the generic comparable in these classes are a mess
        public boolean inRange(Comparable<?> key){
            @SuppressWarnings("unchecked")
            T castKey = (T)key;
            if(this.min_inclusive.compareTo(castKey)<=0 && this.max_exclusive.compareTo(castKey)>0){
                return true;
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
            result = prime * result + ((max_long == null) ? 0 : max_long.hashCode());
            result = prime * result + ((min_long == null) ? 0 : min_long.hashCode());
            result = prime * result + new_partition;
            result = prime * result + old_partition;
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
            if (max_long == null) {
                if (other.max_long != null)
                    return false;
            } else if (!max_long.equals(other.max_long))
                return false;
            if (min_long == null) {
                if (other.min_long != null)
                    return false;
            } else if (!min_long.equals(other.min_long))
                return false;
            if (new_partition != other.new_partition)
                return false;
            if (old_partition != other.old_partition)
                return false;
            if (table_name == null) {
                if (other.table_name != null)
                    return false;
            } else if (!table_name.equals(other.table_name))
                return false;
            return true;
        }  
        
        
      }
      

      
      
    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getOutgoing_ranges() {
        return outgoing_ranges;
    }

    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getIncoming_ranges() {
        return incoming_ranges;
    }
}
