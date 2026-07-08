-- dataset_path parameter value can be set in the notebook's configuration
-- volume path for Unity Catalog can be found in Catalog > Volumnes > bookstore_data : copy volume path
CREATE OR REFRESH STREAMING TABLE orders_raw 
COMMENT "The rew books orders, ingested from orders-raw"
AS SELECT * 
  FROM STREAM read_files("${dataset_path}/orders-json-raw",
  format => 'json',
  inferColumnTypes => true)
;

CREATE OR REFRESH MATERIALIZED VIEW customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * 
  FROM read_files("${dataset_path}/customers-json",
  format => 'json',
  inferColumnTypes => true)
;

