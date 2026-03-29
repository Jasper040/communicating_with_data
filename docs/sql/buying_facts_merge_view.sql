-- Optional DBA view: mirrors the merge performed in `data_loader._merged_facts_sql`.
-- Replace `your_schema` and physical column names if they differ from defaults
-- (see env vars PG_SALES_QTY_COL, PG_INV_STOCK_COL, etc. in data_loader.py).

CREATE OR REPLACE VIEW your_schema.buying_facts_merged AS
WITH key_rows AS (
  SELECT item_no, colour_no, size, barcode FROM your_schema.total_sales_b2c
  UNION
  SELECT item_no, colour_no, size, barcode FROM your_schema.inventory
  UNION
  SELECT item_no, colour_no, size, barcode FROM your_schema.purchased
  UNION
  SELECT item_no, colour_no, size, barcode FROM your_schema.products
),
sales_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE(quantity::double precision, 0)) AS sales_quantity,
    -- Ex-VAT sales turnover (match data_loader; list price VAT handled in app via NL_VAT_DIVISOR)
    SUM(COALESCE(amount_lcy::double precision, 0)) AS sales_revenue_agg,
    MIN(order_date) AS sales_first_order_date,
    MAX(order_date) AS sales_order_date,
    MAX(NULLIF(trim(both from season::text), '')) AS sales_season,
    MAX(NULLIF(trim(both from brand::text), '')) AS sales_brand
  FROM your_schema.total_sales_b2c
  GROUP BY 1, 2, 3, 4
),
inv_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE(stock::double precision, 0)) AS inv_stock
  FROM your_schema.inventory
  GROUP BY 1, 2, 3, 4
),
pur_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE(quantity::double precision, 0)) AS pur_quantity,
    MAX(purchase_price::double precision) AS pur_purchase_price,
    MAX(NULLIF(trim(both from season::text), '')) AS pur_season
  FROM your_schema.purchased
  GROUP BY 1, 2, 3, 4
),
prod_dedup AS (
  SELECT DISTINCT ON (item_no, colour_no, size, barcode)
    item_no, colour_no, size, barcode,
    COALESCE(sales_listprice::double precision, 0) AS prod_sales_listprice,
    season AS prod_season,
    brand AS prod_brand,
    item_group AS prod_item_group,
    fit AS prod_fit,
    size_group AS prod_size_group
  FROM your_schema.products
  ORDER BY item_no, colour_no, size, barcode, COALESCE(sales_listprice::double precision, 0) DESC NULLS LAST
)
SELECT
  k.item_no,
  k.colour_no,
  k.size,
  k.barcode,
  s.sales_quantity,
  s.sales_revenue_agg,
  s.sales_first_order_date,
  s.sales_order_date,
  s.sales_season,
  s.sales_brand,
  i.inv_stock,
  p.pur_quantity,
  p.pur_purchase_price,
  p.pur_season,
  pr.prod_sales_listprice,
  pr.prod_season,
  pr.prod_brand,
  pr.prod_item_group,
  pr.prod_fit,
  pr.prod_size_group
FROM key_rows k
LEFT JOIN sales_agg s
  ON s.item_no = k.item_no AND s.colour_no = k.colour_no AND s.size = k.size AND s.barcode = k.barcode
LEFT JOIN inv_agg i
  ON i.item_no = k.item_no AND i.colour_no = k.colour_no AND i.size = k.size AND i.barcode = k.barcode
LEFT JOIN pur_agg p
  ON p.item_no = k.item_no AND p.colour_no = k.colour_no AND p.size = k.size AND p.barcode = k.barcode
LEFT JOIN prod_dedup pr
  ON pr.item_no = k.item_no AND pr.colour_no = k.colour_no AND pr.size = k.size AND pr.barcode = k.barcode;
