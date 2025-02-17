WITH
  feedback AS (
    SELECT aa.* FROM
  (SELECT * , ROW_NUMBER() OVER (PARTITION BY order_id,customer_id ORDER BY part_dt DESC) AS rn FROM  `original-future-449709-n8.NeoCommerceOms.feedback` ) aa where aa.rn=1 ),
  orders AS (
    SELECT bb.* FROM
  (SELECT * , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn FROM  `original-future-449709-n8.NeoCommerceOms.orders` ) bb where bb.rn=1 ),
  product AS (
    SELECT bb.* FROM
  (SELECT * , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY part_dt DESC) AS rn FROM  `original-future-449709-n8.NeoCommerceOms.product` ) bb where bb.rn=1 )
SELECT
  os.order_id,
  os.customer_id,
  os.order_date,
  os.total_amount,
  os.status,
  pt.name as product_name,
  pt.category,
  pt.price,
  pt.in_stock,
  fk.rating,
  fk.feedback_text
FROM orders os 
LEFT JOIN product pt ON os.product_id=pt.product_id
LEFT JOIN feedback fk ON os.order_id=fk.order_id AND os.customer_id=fk.customer_id
