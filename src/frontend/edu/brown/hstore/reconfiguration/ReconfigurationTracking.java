/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.voltdb.exceptions.ReconfigurationException;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

/**
 * Class to track the reconfiguration state and progress for a single partition
 * Not thread safe
 * @author aelmore
 *
 */
public class ReconfigurationTracking implements ReconfigurationTrackingInterface {
    private List<ReconfigurationRange<? extends Comparable<?>>> outgoing_ranges;
    private List<ReconfigurationRange<? extends Comparable<?>>> incoming_ranges;
    public List<ReconfigurationRange<? extends Comparable<?>>> dataMigratedOut;
    public List<ReconfigurationRange<? extends Comparable<?>>> dataMigratedIn;
    private int partition_id;
    
    
    public ReconfigurationTracking(List<ReconfigurationRange<? extends Comparable<?>>> outgoing_ranges,
            List<ReconfigurationRange<? extends Comparable<?>>> incoming_ranges, int partition_id) {
        super();
        this.outgoing_ranges = outgoing_ranges;
        this.incoming_ranges = incoming_ranges;
        this.partition_id = partition_id;
    }
    
    @Override 
    public boolean markRangeAsMigratedOut(List<ReconfigurationRange<? extends Comparable<?>>> range ){
        throw new NotImplementedException();
    }
    
    @Override
    public boolean markRangeAsReceived(List<ReconfigurationRange<? extends Comparable<?>>> range ){

        throw new NotImplementedException();
    }

    @Override
    public boolean markKeyAsMigratedOut(String table_name, Object key) {
        throw new NotImplementedException();
    }

    @Override
    public boolean markKeyAsReceived(String table_name, Object key) {
        throw new NotImplementedException();
    }

    @Override
    public boolean checkKeyOwned(String table_name, Object key) throws ReconfigurationException {
        throw new NotImplementedException();
    }
    
    
}
