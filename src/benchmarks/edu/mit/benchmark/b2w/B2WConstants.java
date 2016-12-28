package edu.mit.benchmark.b2w;

public class B2WConstants {

    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_RESERVED = "RESERVED";
    public static final String STATUS_PURCHASED = "PURCHASED";
    public static final String STATUS_FINISHED = "FINISHED";
    public static final String STATUS_CANCELLED = "CANCELLED";
    public static final String STATUS_EXPIRED = "EXPIRED";
    public static final String STATUS_COMPLETE = "COMPLETE";
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
    
    public static final int BATCH_SIZE = 10000;
    public static final int TOKEN_LENGTH = 150;
    public static final int PRIME_MULTIPLIER = 32869; // a big prime number
    public static final int DISTINCT_HASH_VALUES = 15485863; // a bigger prime number
    
    public static final String OPERATION = "operation";
    public static final String OPERATION_OFFSET = "offset";
    public static final String OPERATION_PARAMS = "params";
    
    public static final String PARAMS_CART_ID = "cartId";
    public static final String PARAMS_CHECKOUT_ID = "checkoutId";
    public static final String PARAMS_CUSTOMER_ID = "customerId";
    public static final String PARAMS_TOKEN = "token";
    public static final String PARAMS_GUEST = "guest";
    public static final String PARAMS_IS_GUEST = "isGuest";
    public static final String PARAMS_DELIVERY_ADDRESS_ID = "deliveryAddressId"; 
    public static final String PARAMS_BILLING_ADDRESS_ID = "billingAddressId"; 
    public static final String PARAMS_AMOUNT_DUE = "amountDue"; 
    public static final String PARAMS_TOTAL = "total"; 
    public static final String PARAMS_FREIGHT_CONTRACT = "freightContract"; 
    public static final String PARAMS_FREIGHT_PRICE = "freightPrice"; 
    public static final String PARAMS_FREIGHT_STATUS = "freightStatus"; 
    public static final String PARAMS_LINES = "lines";
    public static final String PARAMS_LINE_ID = "lineId";
    public static final String PARAMS_TRANSACTION_ID = "transactionId"; 
    public static final String PARAMS_RESERVES = "reserves";
    public static final String PARAMS_STOCK_ID = "stockId"; 
    public static final String PARAMS_RESERVE_ID = "reserveId"; 
    public static final String PARAMS_BRAND = "brand"; 
    public static final String PARAMS_CREATION_DATE = "stockTxnCreationTimestamp"; 
    public static final String PARAMS_IS_KIT = "isKit"; 
    public static final String PARAMS_REQUESTED_QUANTITY = "requestedQuantity"; 
    public static final String PARAMS_RESERVE_LINES = "reserveLines"; 
    public static final String PARAMS_RESERVED_QUANTITY = "reservedQuantity"; 
    public static final String PARAMS_SKU = "sku"; 
    public static final String PARAMS_SOLR_QUERY = "solrQuery"; 
    public static final String PARAMS_STORE_ID = "storeId"; 
    public static final String PARAMS_SUBINVENTORY = "subinventory"; 
    public static final String PARAMS_WAREHOUSE = "warehouse";
    public static final String PARAMS_STOCK_TYPE = "stockType";
    public static final String PARAMS_DELIVERY_TIME = "deliveryTime";
    public static final String PARAMS_TIMESTAMP = "timestamp";
    public static final String PARAMS_PAYMENT_OPTION_ID = "paymentOptionId";
    public static final String PARAMS_PAYMENT_OPTION_TYPE = "paymentOptionType";
    public static final String PARAMS_DUE_DAYS = "dueDays";
    public static final String PARAMS_AMOUNT = "amount";
    public static final String PARAMS_INSTALLMENT_QUANTITY = "installmentQuantity";
    public static final String PARAMS_INTEREST_AMOUNT = "interestAmount";
    public static final String PARAMS_INTEREST_RATE = "interestRate";
    public static final String PARAMS_ANNUAL_CET = "annualCET";
    public static final String PARAMS_NUMBER = "number";
    public static final String PARAMS_CRIPTO_NUMBER = "criptoNumber";
    public static final String PARAMS_HOLDERS_NAME = "holdersName";
    public static final String PARAMS_SECURITY_CODE = "securityCode";
    public static final String PARAMS_EXPIRATION_DATE = "expirationDate";
    public static final String PARAMS_SALES_CHANNEL = "salesChannel";
    public static final String PARAMS_OPN = "opn";
    public static final String PARAMS_EPAR = "epar";
    public static final String PARAMS_AUTO_MERGE = "autoMerge";    
    public static final String PARAMS_UNIT_SALES_PRICE = "unitSalesPrice";
    public static final String PARAMS_SALES_PRICE = "salesPrice";
    public static final String PARAMS_MAX_QUANTITY = "maxQuantity";
    public static final String PARAMS_MAXIMUM_QUANTITY_REASON = "maximumQuantityReason";
    public static final String PARAMS_TYPE = "type";
    public static final String PARAMS_STOCK_TRANSACTION_ID = "stockTransactionId";
    public static final String PARAMS_LINE_STATUS = "lineStatus";
    public static final String PARAMS_IMAGE = "image";
    public static final String PARAMS_NAME = "name";
    public static final String PARAMS_PRICE = "price";
    public static final String PARAMS_ORIGINAL_PRICE = "originalPrice";
    public static final String PARAMS_IS_LARGE = "isLarge";
    public static final String PARAMS_DEPARTMENT = "department";
    public static final String PARAMS_LINE = "line";
    public static final String PARAMS_SUB_CLASS = "subClass";
    public static final String PARAMS_WEIGHT = "weight";
    public static final String PARAMS_PRODUCT_CLASS = "productClass";
    public static final String PARAMS_PRODUCT_SKU = "productSku"; 
    public static final String PARAMS_PRODUCT_ID = "productId";
    public static final String PARAMS_QUANTITY = "quantity";

}
