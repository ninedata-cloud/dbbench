CREATE INDEX IF NOT EXISTS idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
CREATE INDEX IF NOT EXISTS idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id);
