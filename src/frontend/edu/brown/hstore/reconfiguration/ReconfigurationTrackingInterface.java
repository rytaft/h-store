/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.List;

import org.voltdb.exceptions.ReconfigurationException;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

/**
 * @author aelmore
 *
 */
public interface ReconfigurationTrackingInterface {
    
    /**
     * Mark an individual key as migrated away from this partition
     * @param table_name
     * @param key
     * @return
     */
    public boolean markKeyAsMigratedOut(String table_name, Object key ); 
    
    /**
     * Mark a key range migrated away from this partition
     * @param range
     * @return
     */
    public boolean markRangeAsMigratedOut(List<ReconfigurationRange<? extends Comparable<?>>> range );
    
    /**
     * Mark a key received by this partition
     * @param table_name
     * @param key
     * @return
     */
    public boolean markKeyAsReceived(String table_name, Object key);
    
    
    /**
     * Mark a range as received by this partition
     * @param range
     * @return
     */
    public boolean markRangeAsReceived(List<ReconfigurationRange<? extends Comparable<?>>> range );
    
    /**
     * Check if a key is owned and currently present
     * @param table_name
     * @param key
     * @return if the key is owned or not.
     * @throws ReconfigurationException to indicate a set of keys must be migrated out, in or both
     */
    public boolean checkKeyOwned(String table_name, Object key) throws ReconfigurationException;
}
