
CREATE TABLE STK_INVENTORY_STOCK (
        sku BIGINT,
        id VARCHAR(128) NOT NULL,
        warehouse INT,
        sub_inventory INT,
        stock_type INT,
        store_id BIGINT,
        lead_time INT,
        PRIMARY KEY (id)
);
CREATE INDEX IDX_STK_INVENTORY_STOCK ON STK_INVENTORY_STOCK(sku);

CREATE TABLE STK_INVENTORY_STOCK_QUANTITY (
        id VARCHAR(128) NOT NULL, -- REFERENCES STK_INVENTORY_STOCK(id)
        available INT,
        purchase INT,
        session INT,
        PRIMARY KEY (id)
);

CREATE TABLE STK_STOCK_TRANSACTION (
        transaction_id VARCHAR(128) NOT NULL,
        reserve_id VARCHAR(128),
        brand VARCHAR(32),
        creation_date DATE,
        current_status VARCHAR(32),
        expiration_date DATE,
        is_kit BOOLEAN,
        requested_quantity INT,
        reserve_lines VARCHAR(256),
        reserved_quantity INT,
        sku BIGINT,
        solr_query VARCHAR(128),
        status VARCHAR(256),
        store_id BIGINT,
        subinventory INT,
        warehouse INT,
        PRIMARY KEY (transaction_id)
);

CREATE TABLE CART (
{
        id VARCHAR(128) NOT NULL,
        total FLOAT,
        salesChannel VARCHAR(32),
        opn VARCHAR(32),
        epar VARCHAR(128),
        lastModified DATE,
        status VARCHAR(32),
        autoMerge BOOLEAN,
        PRIMARY KEY (id)
);

CREATE TABLE CART_CUSTOMER (
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        id VARCHAR(128),
        token VARCHAR(256),
        guest BOOLEAN,
        isGuest BOOLEAN
);
CREATE INDEX IDX_CART_CUSTOMER ON CART_CUSTOMER(cartId);

CREATE TABLE CART_LINES (
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        id VARCHAR(128), -- <productSku>-<storeId> 
        productSku BIGINT, -- REFERENCES CART_LINE_PRODUCTS(sku)
        productId BIGINT, -- REFERENCES CART_LINE_PRODUCTS(id)
        storeId BIGINT, -- REFERENCES CART_LINE_PRODUCT_STORES(id)
        unitSalesPrice FLOAT,
        salesPrice FLOAT,
        quantity INT,
        maxQuantity INT,
        maximumQuantityReason VARCHAR(32),
        type VARCHAR(32),
        stockTransactionId VARCHAR(128), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        requestedQuantity INT,
        status VARCHAR(32),
        stockType VARCHAR(32),
        insertDate DATE
);
CREATE INDEX IDX_CART_LINES ON CART_LINES(cartId);

CREATE TABLE CART_LINE_PRODUCTS (
{
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        id BIGINT,
        sku BIGINT,
        image VARCHAR(256),
        name VARCHAR(128),
        isKit BOOLEAN,
        price FLOAT,
        originalPrice FLOAT,
        isLarge BOOLEAN,
        department BIGINT,
        line BIGINT,
        subClass BIGINT,
        weight FLOAT,
        class BIGINT
);
CREATE INDEX IDX_CART_LINE_PRODUCTS ON CART_LINE_PRODUCTS(cartId);

CREATE TABLE CART_LINE_PROMOTIONS (
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        name VARCHAR(32),
        category VARCHAR(32),
        sourceValue FLOAT,
        type VARCHAR(32),
        conditional BOOLEAN,
        discountValue FLOAT
);
CREATE INDEX IDX_CART_LINE_PROMOTIONS ON CART_LINE_PROMOTIONS(cartId);

CREATE TABLE CART_LINE_PRODUCT_WARRANTIES (
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        sku BIGINT,
        productSku VARCHAR(128), -- REFERENCES CART_LINES(id)
        description VARCHAR(256)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_WARRANTIES ON CART_LINE_PRODUCT_WARRANTIES(cartId);

CREATE TABLE CART_LINE_PRODUCT_STORES (
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        id BIGINT,
        name VARCHAR(32),
        image VARCHAR(256),
        deliveryType VARCHAR(32)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_STORES ON CART_LINE_PRODUCT_STORES(cartId);

CREATE TABLE CHECKOUT (
        id VARCHAR(128) NOT NULL,
        cartId VARCHAR(128), --  REFERENCES CART(id)
        deliveryAddressId VARCHAR(32),
        billingAddressId VARCHAR(32),
        amountDue FLOAT,
        total FLOAT,
        freightContract VARCHAR(32),
        freightPrice FLOAT,
        freightStatus VARCHAR(32)
        PRIMARY KEY (id)
);

CREATE TABLE CHECKOUT_PAYMENTS (    
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        paymentOptionId VARCHAR(32),
        paymentOptionType VARCHAR(32),
        dueDays INT,
        amount FLOAT,
        installmentQuantity INT,
        interestAmount FLOAT,
        interestRate INT,
        annualCET INT,
        number VARCHAR(32),
        criptoNumber BIGINT,
        holdersName VARCHAR(32),
        securityCode BIGINT,
        expirationDate VARCHAR(16)
);
CREATE INDEX IDX_CHECKOUT_PAYMENTS ON CHECKOUT_PAYMENTS(checkoutId);

CREATE TABLE CHECKOUT_FREIGHT_DELIVERY_TIME (
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        lineId BIGINT, -- REFERENCES CART_LINES(id)
        deliveryTime INT
);
CREATE INDEX IDX_CHECKOUT_FREIGHT_DELIVERY_TIME ON CHECKOUT_FREIGHT_DELIVERY_TIME(checkoutId);

CREATE TABLE CHECKOUT_STOCK_TRANSACTIONS (
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        id VARCHAR(128), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        lineId BIGINT -- REFERENCES CART_LINES(id)
);
CREATE INDEX IDX_CHECKOUT_STOCK_TRANSACTIONS ON CHECKOUT_STOCK_TRANSACTIONS(checkoutId);
