
CREATE TABLE STK_INVENTORY_STOCK (
        partition_key INTEGER NOT NULL,
        sku VARCHAR(32) NOT NULL,
        id VARCHAR(128) NOT NULL,
        warehouse INTEGER,
        sub_inventory INTEGER,
        stock_type INTEGER,
        store_id VARCHAR(32),
        lead_time INTEGER,
        PRIMARY KEY (id)
);
CREATE INDEX IDX_STK_INVENTORY_STOCK ON STK_INVENTORY_STOCK(sku);

CREATE TABLE STK_INVENTORY_STOCK_QUANTITY (
        partition_key INTEGER NOT NULL,
        id VARCHAR(128) NOT NULL, -- REFERENCES STK_INVENTORY_STOCK(id)
        available INTEGER,
        purchase INTEGER,
        session INTEGER,
        PRIMARY KEY (id)
);

CREATE TABLE STK_STOCK_TRANSACTION (
        partition_key INTEGER NOT NULL,
        transaction_id VARCHAR(128) NOT NULL,
        reserve_id VARCHAR(128) NOT NULL,
        brand VARCHAR(32),
        creation_date TIMESTAMP,
        current_status VARCHAR(32),
        expiration_date TIMESTAMP,
        is_kit TINYINT,
        requested_quantity INTEGER,
        reserve_lines VARCHAR(4096),
        reserved_quantity INTEGER,
        sku VARCHAR(32), -- REFERENCES STK_INVENTORY_STOCK(sku)
        status VARCHAR(2048),
        store_id VARCHAR(32),
        subinventory INTEGER,
        warehouse INTEGER,
        PRIMARY KEY (reserve_id)
);

CREATE TABLE CART (
        partition_key INTEGER NOT NULL,
        id VARCHAR(128) NOT NULL,
        total FLOAT,
        salesChannel VARCHAR(32),
        opn VARCHAR(32),
        epar VARCHAR(128),
        lastModified TIMESTAMP,
        status VARCHAR(32),
        autoMerge TINYINT,
        PRIMARY KEY (id)
);

CREATE TABLE CART_CUSTOMER (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        id VARCHAR(128),
        token VARCHAR(256),
        guest TINYINT,
        isGuest TINYINT
);
CREATE INDEX IDX_CART_CUSTOMER ON CART_CUSTOMER(cartId);

CREATE TABLE CART_LINES (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        id VARCHAR(128), -- <productSku>-<storeId> 
        productSku VARCHAR(32), -- REFERENCES CART_LINE_PRODUCTS(sku)
        productId BIGINT, -- REFERENCES CART_LINE_PRODUCTS(id)
        storeId VARCHAR(32), -- REFERENCES CART_LINE_PRODUCT_STORES(id)
        unitSalesPrice FLOAT,
        salesPrice FLOAT,
        quantity INTEGER,
        maxQuantity INTEGER,
        maximumQuantityReason VARCHAR(32),
        type VARCHAR(32),
        stockTransactionId VARCHAR(128), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        requestedQuantity INTEGER,
        status VARCHAR(32),
        stockType VARCHAR(32),
        insertDate TIMESTAMP
);
CREATE INDEX IDX_CART_LINES ON CART_LINES(cartId);

CREATE TABLE CART_LINE_PRODUCTS (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        id BIGINT,
        sku VARCHAR(32),
        image VARCHAR(256),
        name VARCHAR(128),
        isKit TINYINT,
        price FLOAT,
        originalPrice FLOAT,
        isLarge TINYINT,
        department BIGINT,
        line BIGINT,
        subClass BIGINT,
        weight FLOAT,
        class BIGINT
);
CREATE INDEX IDX_CART_LINE_PRODUCTS ON CART_LINE_PRODUCTS(cartId);

CREATE TABLE CART_LINE_PROMOTIONS (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        name VARCHAR(32),
        category VARCHAR(32),
        sourceValue FLOAT,
        type VARCHAR(32),
        conditional TINYINT,
        discountValue FLOAT
);
CREATE INDEX IDX_CART_LINE_PROMOTIONS ON CART_LINE_PROMOTIONS(cartId);

CREATE TABLE CART_LINE_PRODUCT_WARRANTIES (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        sku VARCHAR(32),
        productSku VARCHAR(128), -- REFERENCES CART_LINES(id)
        description VARCHAR(256)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_WARRANTIES ON CART_LINE_PRODUCT_WARRANTIES(cartId);

CREATE TABLE CART_LINE_PRODUCT_STORES (
        partition_key INTEGER NOT NULL,
        cartId VARCHAR(128) NOT NULL REFERENCES CART(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        id VARCHAR(32),
        name VARCHAR(32),
        image VARCHAR(256),
        deliveryType VARCHAR(32)
);
CREATE INDEX IDX_CART_LINE_PRODUCT_STORES ON CART_LINE_PRODUCT_STORES(cartId);

CREATE TABLE CHECKOUT (
        partition_key INTEGER NOT NULL,
        id VARCHAR(128) NOT NULL,
        cartId VARCHAR(128), --  REFERENCES CART(id)
        deliveryAddressId VARCHAR(32),
        billingAddressId VARCHAR(32),
        amountDue FLOAT,
        total FLOAT,
        freightContract VARCHAR(32),
        freightPrice FLOAT,
        freightStatus VARCHAR(32),
        PRIMARY KEY (id)
);

CREATE TABLE CHECKOUT_PAYMENTS (
        partition_key INTEGER NOT NULL,
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        paymentOptionId VARCHAR(32),
        paymentOptionType VARCHAR(32),
        dueDays INTEGER,
        amount FLOAT,
        installmentQuantity INTEGER,
        interestAmount FLOAT,
        interestRate INTEGER,
        annualCET INTEGER,
        number VARCHAR(32),
        criptoNumber VARCHAR(128),
        holdersName VARCHAR(32),
        securityCode VARCHAR(64),
        expirationDate VARCHAR(16)
);
CREATE INDEX IDX_CHECKOUT_PAYMENTS ON CHECKOUT_PAYMENTS(checkoutId);

CREATE TABLE CHECKOUT_FREIGHT_DELIVERY_TIME (
        partition_key INTEGER NOT NULL,
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        lineId VARCHAR(128), -- REFERENCES CART_LINES(id)
        deliveryTime INTEGER
);
CREATE INDEX IDX_CHECKOUT_FREIGHT_DELIVERY_TIME ON CHECKOUT_FREIGHT_DELIVERY_TIME(checkoutId);

CREATE TABLE CHECKOUT_STOCK_TRANSACTIONS (
        partition_key INTEGER NOT NULL,
        checkoutId VARCHAR(128) NOT NULL REFERENCES CHECKOUT(id),
        id VARCHAR(128), -- REFERENCES STK_STOCK_TRANSACTION(transaction_id),
        lineId VARCHAR(128) -- REFERENCES CART_LINES(id)
);
CREATE INDEX IDX_CHECKOUT_STOCK_TRANSACTIONS ON CHECKOUT_STOCK_TRANSACTIONS(checkoutId);
