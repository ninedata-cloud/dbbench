CREATE TABLE warehouse (
    w_id INTEGER NOT NULL,
    w_name NVARCHAR(10),
    w_street_1 NVARCHAR(20),
    w_street_2 NVARCHAR(20),
    w_city NVARCHAR(20),
    w_state NVARCHAR(2),
    w_zip NVARCHAR(9),
    w_tax DECIMAL(4,4),
    w_ytd DECIMAL(12,2),
    PRIMARY KEY (w_id)
);

CREATE TABLE district (
    d_id INTEGER NOT NULL,
    d_w_id INTEGER NOT NULL,
    d_name NVARCHAR(10),
    d_street_1 NVARCHAR(20),
    d_street_2 NVARCHAR(20),
    d_city NVARCHAR(20),
    d_state NVARCHAR(2),
    d_zip NVARCHAR(9),
    d_tax DECIMAL(4,4),
    d_ytd DECIMAL(12,2),
    d_next_o_id INTEGER,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    c_id INTEGER NOT NULL,
    c_d_id INTEGER NOT NULL,
    c_w_id INTEGER NOT NULL,
    c_first NVARCHAR(16),
    c_middle NVARCHAR(2),
    c_last NVARCHAR(16),
    c_street_1 NVARCHAR(20),
    c_street_2 NVARCHAR(20),
    c_city NVARCHAR(20),
    c_state NVARCHAR(2),
    c_zip NVARCHAR(9),
    c_phone NVARCHAR(16),
    c_since TIMESTAMP,
    c_credit NVARCHAR(2),
    c_credit_lim DECIMAL(12,2),
    c_discount DECIMAL(4,4),
    c_balance DECIMAL(12,2),
    c_ytd_payment DECIMAL(12,2),
    c_payment_cnt INTEGER,
    c_delivery_cnt INTEGER,
    c_data NVARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE item (
    i_id INTEGER NOT NULL,
    i_im_id INTEGER,
    i_name NVARCHAR(24),
    i_price DECIMAL(5,2),
    i_data NVARCHAR(50),
    PRIMARY KEY (i_id)
);

CREATE TABLE stock (
    s_i_id INTEGER NOT NULL,
    s_w_id INTEGER NOT NULL,
    s_quantity INTEGER,
    s_dist_01 NVARCHAR(24),
    s_dist_02 NVARCHAR(24),
    s_dist_03 NVARCHAR(24),
    s_dist_04 NVARCHAR(24),
    s_dist_05 NVARCHAR(24),
    s_dist_06 NVARCHAR(24),
    s_dist_07 NVARCHAR(24),
    s_dist_08 NVARCHAR(24),
    s_dist_09 NVARCHAR(24),
    s_dist_10 NVARCHAR(24),
    s_ytd INTEGER,
    s_order_cnt INTEGER,
    s_remote_cnt INTEGER,
    s_data NVARCHAR(50),
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE history (
    h_c_id INTEGER,
    h_c_d_id INTEGER,
    h_c_w_id INTEGER,
    h_d_id INTEGER,
    h_w_id INTEGER,
    h_date TIMESTAMP,
    h_amount DECIMAL(6,2),
    h_data NVARCHAR(24)
);

CREATE TABLE oorder (
    o_id INTEGER NOT NULL,
    o_d_id INTEGER NOT NULL,
    o_w_id INTEGER NOT NULL,
    o_c_id INTEGER,
    o_entry_d TIMESTAMP,
    o_carrier_id INTEGER,
    o_ol_cnt INTEGER,
    o_all_local INTEGER,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE new_order (
    no_o_id INTEGER NOT NULL,
    no_d_id INTEGER NOT NULL,
    no_w_id INTEGER NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line (
    ol_o_id INTEGER NOT NULL,
    ol_d_id INTEGER NOT NULL,
    ol_w_id INTEGER NOT NULL,
    ol_number INTEGER NOT NULL,
    ol_i_id INTEGER,
    ol_supply_w_id INTEGER,
    ol_delivery_d TIMESTAMP,
    ol_quantity INTEGER,
    ol_amount DECIMAL(6,2),
    ol_dist_info NVARCHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);
