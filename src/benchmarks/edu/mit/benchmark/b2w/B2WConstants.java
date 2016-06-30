package edu.mit.benchmark.b2w;

public class B2WConstants {

    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_RESERVED = "RESERVED";
    public static final String STATUS_PURCHASED = "PURCHASED";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_INCOMPLETE = "INCOMPLETE";

    public static final String TABLENAME_INVENTORY_STOCK                = "STK_INVENTORY_STOCK";
    public static final String TABLENAME_INVENTORY_STOCK_QUANTITY       = "STK_INVENTORY_STOCK_QUANTITY";
    public static final String TABLENAME_STOCK_TRANSACTION              = "STK_STOCK_TRANSACTION";
    public static final String TABLENAME_CART                           = "CART";
    public static final String TABLENAME_CART_CUSTOMER                  = "CART_CUSTOMER";
    public static final String TABLENAME_CART_LINES                     = "CART_LINES";
    public static final String TABLENAME_CART_LINE_PRODUCTS             = "CART_LINE_PRODUCTS";
    public static final String TABLENAME_CART_LINE_PROMOTIONS           = "CART_LINE_PROMOTIONS";
    public static final String TABLENAME_CART_LINE_PRODUCT_WARRANTIES   = "CART_LINE_PRODUCT_WARRANTIES";
    public static final String TABLENAME_CART_LINE_PRODUCT_STORES       = "CART_LINE_PRODUCT_STORES";
    public static final String TABLENAME_CHECKOUT                       = "CHECKOUT";
    public static final String TABLENAME_CHECKOUT_PAYMENTS              = "CHECKOUT_PAYMENTS";
    public static final String TABLENAME_CHECKOUT_FREIGHT_DELIVERY_TIME = "CHECKOUT_FREIGHT_DELIVERY_TIME";
    public static final String TABLENAME_CHECKOUT_STOCK_TRANSACTIONS    = "CHECKOUT_STOCK_TRANSACTIONS";

    public static final int STOCK_TABLE_COUNT = 3;
    public static final int CART_TABLE_COUNT = 7;
    public static final int CHECKOUT_TABLE_COUNT = 4;
    
    public static final int TOKEN_LENGTH = 150;
    
    public static final int NULL_DELIVERY_TIME = -1;
}
