package edu.mit.benchmark.affinity;

public class AffinityConstants {

	public static final int NUM_SUPPLIERS = 100;
    public static final String TABLENAME_SUPPLIERS = "SUPPLIERS";
    protected static final int SUPPLIERS_NUM_COLUMNS = 11;
    public static final int SUPPLIERS_COLUMN_LENGTH = 100;

    public static final int NUM_PRODUCTS = 100;
    public static final String TABLENAME_PRODUCTS = "PRODUCTS";
    protected static final int PRODUCTS_NUM_COLUMNS = 11;
    public static final int PRODUCTS_COLUMN_LENGTH = 100;

    public static final int NUM_PARTS = 100;
    public static final String TABLENAME_PARTS = "PARTS";
    protected static final int PARTS_NUM_COLUMNS = 11;
    public static final int PARTS_COLUMN_LENGTH = 100;

    public static final double SUPPLIES_PROBABILITY = 0.1;
    public static final String TABLENAME_SUPPLIES = "SUPPLIES";
    protected static final int SUPPLIES_NUM_COLUMNS = 2;

    public static final double USES_PROBABILITY = 0.1;
    public static final String TABLENAME_USES = "USES";
    protected static final int USES_NUM_COLUMNS = 2;
    
    public static final int BATCH_SIZE = 10000;
    public static final int FREQ_READ_SUPPLIER = 90; 
    public static final int FREQ_READ_PRODUCT = 0;
    public static final int FREQ_READ_PART = 0;
    public static final int FREQ_READ_PARTS_BY_SUPPLIER = 10;
    public static final int FREQ_READ_PARTS_BY_PRODUCT = 0;

}
