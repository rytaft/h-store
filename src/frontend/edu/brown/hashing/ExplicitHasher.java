package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

public interface ExplicitHasher {

    /**
     * Combine multiple values into a single key and get the hash of that
     * Should be uniformly distributed (or at least good enough for what we need)
     * @param values
     * @return
     */
    public abstract int multiValueHash(Object values[]);

    public abstract int multiValueHash(Object val0, Object val1);

    public abstract int multiValueHash(int... values);

    /**
     * Return the number of partitions that this hasher can map values to
     * @return
     */
    public abstract int getNumPartitions();

    public abstract void init(CatalogContext catalogContext);

    /**
     * Hash the given value based on the partition count 
     * @param value
     * @return
     */
    public abstract int hash(Object value);

    /**
     * Hash the given value that is derived from a particular catalog object
     * @param value
     * @param catalog_item
     * @return
     */
    public abstract int hash(Object value, CatalogType catalog_item);
    
    /**
     * Hash the given values that are derived from particular catalog objects
     * @param values
     * @param catalog_items
     * @return
     */
    public abstract int hash(List<Object> values, List<CatalogType> catalog_items);

    /**
     * Hash the given value using a specific partition count
     * @param value
     * @param num_partitions
     * @return
     */
    public abstract int hash(Object value, int num_partitions);

    public abstract void load(File input_path, Database catalog_db) throws IOException;

    public abstract void save(File output_path) throws IOException;

    public abstract String toJSONString();
    
    public ReconfigurationPlan changePartitionPhase(String partition_plan) throws Exception;
    
    public ReconfigurationPlan changePartitionPlan(String partition_json_file) throws Exception;
    
    public ExplicitPartitions getPartitions();

}