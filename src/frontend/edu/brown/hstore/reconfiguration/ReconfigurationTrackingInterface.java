/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.List;

import org.voltdb.catalog.CatalogType;
import org.voltdb.exceptions.ReconfigurationException;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

/**
 * @author aelmore
 *
 */
public interface ReconfigurationTrackingInterface {
    
    /**
     * Mark an individual key as migrated away from this partition
     * Does not verify that this key was expected
     * @param table_name
     * @param key
     * @return
     */
    public boolean markKeyAsMigratedOut(String table_name, Comparable<?> key ); 
    
    /**
     * Mark a key range migrated away from this partition      
     * Does not verify that this range was expected
     * @param range
     * @return
     */
    public boolean markRangeAsMigratedOut(List<ReconfigurationRange<? extends Comparable<?>>> range );
    
    /**
     * Mark a key range migrated away from this partition
     * Does not verify that this range was expected
     * @param range
     * @return
     */
    public boolean markRangeAsMigratedOut(ReconfigurationRange<? extends Comparable<?>> range );
    
    /**
     * Mark a key received by this partition
     * Does not verify that this key was expected
     * @param table_name
     * @param key
     * @return
     */
    public boolean markKeyAsReceived(String table_name, Comparable<?> key);
    
    
    /**
     * Mark a range as received by this partition.
     * Does not verify that this range was expected
     * @param range
     * @return
     */
    public boolean markRangeAsReceived(List<ReconfigurationRange<? extends Comparable<?>>> range );
    
    /**
     * Mark a range as received by this partition
     * Does not verify that this range was expected
     * @param range
     * @return
     */
    public boolean markRangeAsReceived(ReconfigurationRange<? extends Comparable<?>> range );
    
    /**
     * Check if a key is owned and currently present
     * @param table_name
     * @param key
     * @return if the key is owned or not.
     * @throws ReconfigurationException to indicate a set of keys must be migrated out, in or both
     */
    public boolean checkKeyOwned(String table_name, Comparable<?> key) throws ReconfigurationException;
    
    /**
     * Check if a key is owned and currently present
     * @param catalog
     * @param key
     * @return if the key is owned or not.
     * @throws ReconfigurationException to indicate a set of keys must be migrated out, in or both
     */
    public boolean checkKeyOwned(CatalogType catalog, Object key) throws ReconfigurationException;
}
