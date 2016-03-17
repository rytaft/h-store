
CREATE TABLE STK_INVENTORY_STOCK (
        sku BIGINT,
        id VARCHAR(100),
        warehouse INT,
        sub_inventory INT,
        stock_type INT,
        store_id BIGINT,
        lead_time INT,
        PRIMARY KEY (id)
);


CREATE TABLE STK_INVENTORY_STOCK_QUANTITY (
        id VARCHAR(100),
        available INT,
        purchase INT,
        session INT,
        PRIMARY KEY (id)
);

CREATE INDEX IDX_STK_INVENTORY_STOCK ON STK_INVENTORY_STOCK(sku);
CREATE INDEX IDX_STK_INVENTORY_STOCK_QUANTITY ON STK_INVENTORY_STOCK_QUANTITY(id);

CREATE TABLE CART (
{
        id VARCHAR(100) NOT NULL,
        total FLOAT,
        salesChannel VARCHAR(100),
        opn VARCHAR(100),
        epar VARCHAR(100),
        lastModified DATE,
        PRIMARY KEY (id)
);

CREATE TABLE CART_LINES (
        cart_id VARCHAR(100) NOT NULL REFERENCES CART(id),
        id VARCHAR(100) NOT NULL, -- <productSku>-<storeId> 
        productSku BIGINT, -- redundant?
        productId BIGINT NOT NULL REFERENCES CART_LINE_PRODUCTS(id),
        storeId BIGINT NOT NULL REFERENCES STORES(id),
        unitSalesPrice FLOAT,
        salesPrice FLOAT,
        quantity INT,
        maxQuantity INT,
        maximumQuantityReason VARCHAR(100),
        type VARCHAR(100),
        insertDate DATE
);

CREATE TABLE CART_LINE_PRODUCTS (
{
        id BIGINT NOT NULL,
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
        class BIGINT,
        PRIMARY KEY (id)
);

CREATE TABLE PRODUCT_WARRANTIES (
        sku BIGINT,
        productSku VARCHAR(100), -- NOT NULL REFERENCES CART_LINES(id)
        description VARCHAR(100)
);

CREATE TABLE STORES (
        id BIGINT NOT NULL,
        name VARCHAR(100),
        image VARCHAR(100),
        deliveryType VARCHAR(100),
        PRIMARY KEY (id)
);
