package edu.mit.benchmark.affinity;

public class AffinityConstants {

	public static final long NUM_SUPPLIERS = 100;
    public static final String TABLENAME_SUPPLIERS = "SUPPLIERS";
    protected static final int SUPPLIERS_NUM_COLUMNS = 11;
    public static final int SUPPLIERS_COLUMN_LENGTH = 100;
    public static final String SUP_PRE = "suppliers.";
    
    public static final long NUM_PRODUCTS = 100;
    public static final String TABLENAME_PRODUCTS = "PRODUCTS";
    protected static final int PRODUCTS_NUM_COLUMNS = 11;
    public static final int PRODUCTS_COLUMN_LENGTH = 100;
    public static final String PROD_PRE = "products.";

    public static final long NUM_PARTS = 100;
    public static final String TABLENAME_PARTS = "PARTS";
    protected static final int PARTS_NUM_COLUMNS = 11;
    public static final int PARTS_COLUMN_LENGTH = 100;
    public static final String PARTS_PRE = "parts.";

    public static final int MAX_PARTS_PER_SUPPLIER = 10;
    public static final String TABLENAME_SUPPLIES = "SUPPLIES";
    protected static final int SUPPLIES_NUM_COLUMNS = 2;
    public static final String SUPPLIES_PRE = "supplies.";

    public static final int MAX_PARTS_PER_PRODUCT = 10;
    public static final String TABLENAME_USES = "USES";
    protected static final int USES_NUM_COLUMNS = 2;
    public static final String USES_PRE = "uses.";
    
    public static final int BATCH_SIZE = 10000;
    public static final int FREQ_READ_SUPPLIER = 20; 
    public static final int FREQ_READ_PRODUCT = 20;
    public static final int FREQ_READ_PART = 20;
    public static final int FREQ_READ_PARTS_BY_SUPPLIER = 20;
    public static final int FREQ_READ_PARTS_BY_PRODUCT = 20;
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestDistribution";
    
    public static final int HOT_DATA_WORKLOAD_SKEW = 95;
    public static final int HOT_DATA_SIZE = 1;

    public static final int WARM_DATA_SIZE = 0;
    public static final int WARM_DATA_WORKLOAD_SKEW = 0;


    public static final double ZIPFIAN_CONSTANT = .5;

    /**
     * Hotspot distribution.
     */
    public static final String HOTSPOT_DISTRIBUTION = "hotspot";

    /**
     * Hotspot distribution.
     */
    public static final String CUSTOM_DISTRIBUTION = "custom";    
    /**
     * Uniform distribution.
     */
    public static final String UNIFORM_DISTRIBUTION = "uniform";
    
    /**
     * Zipfian distribution
     */
    public static final String ZIPFIAN_DISTRIBUTION = "zipfian";
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = UNIFORM_DISTRIBUTION;
}
