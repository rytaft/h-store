
CREATE TABLE STK_INVENTORY_STOCK (
        sku BIGINT,
        id VARCHAR(100) NOT NULL,
        warehouse INT,
        sub_inventory INT,
        stock_type INT,
        store_id BIGINT,
        lead_time INT,
        PRIMARY KEY (id)
);
CREATE INDEX IDX_STK_INVENTORY_STOCK ON STK_INVENTORY_STOCK(sku);

CREATE TABLE STK_INVENTORY_STOCK_QUANTITY (
        id VARCHAR(100) NOT NULL, -- REFERENCES STK_INVENTORY_STOCK(id)
        available INT,
        purchase INT,
        session INT,
        PRIMARY KEY (id)
);

CREATE TABLE STK_STOCK_TRANSACTION (
        transaction_id VARCHAR(100) NOT NULL,
        reserve_id VARCHAR(100),
        brand VARCHAR(100),
        creation_date DATE,
        current_status VARCHAR(100),
        expiration_date DATE,
        is_kit BOOLEAN,
        requested_quantity INT,
        reserve_lines VARCHAR(100),
        reserved_quantity INT,
        sku BIGINT,
        solr_query VARCHAR(100),
        status VARCHAR(100),
        store_id BIGINT,
        subinventory INT,
        warehouse INT,
        PRIMARY KEY (transaction_id)
);

CREATE TABLE CART (
{
        id VARCHAR(100) NOT NULL,
        total FLOAT,
        salesChannel VARCHAR(100),
        opn VARCHAR(100),
        epar VARCHAR(100),
        lastModified DATE,
        status VARCHAR(100),
        autoMerge BOOLEAN,
        PRIMARY KEY (id)
);

CREATE TABLE CART_CUSTOMER (
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        id VARCHAR(100),
        token VARCHAR(100),
        guest BOOLEAN,
        isGuest BOOLEAN
);
CREATE INDEX IDX_CART_CUSTOMER ON CART_CUSTOMER(cartId);

CREATE TABLE CART_LINES (
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        id VARCHAR(100), -- <productSku>-<storeId> 
        productSku BIGINT, -- REFERENCES CART_LINE_PRODUCTS(sku)
        productId BIGINT, -- REFERENCES CART_LINE_PRODUCTS(id)
        storeId BIGINT, -- REFERENCES CART_LINE_PRODUCT_STORES(id)
        unitSalesPrice FLOAT,
        salesPrice FLOAT,
        quantity INT,
        maxQuantity INT,
        maximumQuantityReason VARCHAR(100),
        type VARCHAR(100),
        stockTransactionId VARCHAR(100), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        requestedQuantity INT,
        status VARCHAR(100),
        stockType VARCHAR(100),
        insertDate DATE
);
CREATE INDEX IDX_CART_LINES ON CART_LINES(cartId);

CREATE TABLE CART_LINE_PRODUCTS (
{
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(100), -- REFERENCES CART_LINES(id)
        id BIGINT,
        sku BIGINT,
        image VARCHAR(100),
        name VARCHAR(100),
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
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(100), -- REFERENCES CART_LINES(id)
        name VARCHAR(100),
        category VARCHAR(100),
        sourceValue FLOAT,
        type VARCHAR(100),
        conditional BOOLEAN,
        discountValue FLOAT
);
CREATE INDEX IDX_CART_LINE_PROMOTIONS ON CART_LINE_PROMOTIONS(cartId);

CREATE TABLE CART_LINE_PRODUCT_WARRANTIES (
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(100), -- REFERENCES CART_LINES(id)
        sku BIGINT,
        productSku VARCHAR(100), -- REFERENCES CART_LINES(id)
        description VARCHAR(100)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_WARRANTIES ON CART_LINE_PRODUCT_WARRANTIES(cartId);

CREATE TABLE CART_LINE_PRODUCT_STORES (
        cartId VARCHAR(100) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(100), -- REFERENCES CART_LINES(id)
        id BIGINT,
        name VARCHAR(100),
        image VARCHAR(100),
        deliveryType VARCHAR(100)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_STORES ON CART_LINE_PRODUCT_STORES(cartId);

CREATE TABLE CHECKOUT (
        id VARCHAR(100) NOT NULL,
        cartId VARCHAR(100),
        deliveryAddressId VARCHAR(100),
        billingAddressId VARCHAR(100),
        amountDue FLOAT,
        total FLOAT,
        freightContract VARCHAR(100),
        freightPrice FLOAT,
        freightStatus VARCHAR(100)
        PRIMARY KEY (id)
);

CREATE TABLE CHECKOUT_PAYMENTS (    
        checkoutId VARCHAR(100) NOT NULL REFERENCES CHECKOUT(id),
        paymentOptionId VARCHAR(100),
        paymentOptionType VARCHAR(100),
        dueDays INT,
        amount FLOAT,
        installmentQuantity INT,
        interestAmount FLOAT,
        interestRate INT,
        annualCET INT,
        number VARCHAR(100),
        criptoNumber BIGINT,
        holdersName VARCHAR(100),
        securityCode BIGINT,
        expirationDate VARCHAR(10)
);
CREATE INDEX IDX_CHECKOUT_PAYMENTS ON CHECKOUT_PAYMENTS(checkoutId);

CREATE TABLE CHECKOUT_FREIGHT_DELIVERY_TIME (
        checkoutId VARCHAR(100) NOT NULL REFERENCES CHECKOUT(id),
        lineId BIGINT, -- REFERENCES CART_LINES(id)
        deliveryTime INT
);
CREATE INDEX IDX_CHECKOUT_FREIGHT_DELIVERY_TIME ON CHECKOUT_FREIGHT_DELIVERY_TIME(checkoutId);

CREATE TABLE CHECKOUT_STOCK_TRANSACTIONS (
        checkoutId VARCHAR(100) NOT NULL REFERENCES CHECKOUT(id),
        id VARCHAR(100), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        lineId BIGINT -- REFERENCES CART_LINES(id)
);
CREATE INDEX IDX_CHECKOUT_STOCK_TRANSACTIONS ON CHECKOUT_STOCK_TRANSACTIONS(checkoutId);
