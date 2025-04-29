SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = 1 AND c_w_id = 1 AND c_d_id = 5 AND c_id = 544;
UPDATE warehouse SET w_ytd = w_ytd + 2479.74 WHERE w_id = 2;
UPDATE order_line SET ol_delivery_d = '2014-10-12 20:52:15' WHERE ol_o_id = 2511 AND ol_d_id = 1 AND ol_w_id = 2;
SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = 2;
UPDATE district SET d_ytd = d_ytd + 2479.74 WHERE d_w_id = 2 AND d_id = 9;
