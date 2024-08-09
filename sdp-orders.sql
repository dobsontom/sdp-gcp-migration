WITH
   main AS (
      WITH
         orders_base AS (
            SELECT
               product_instance_id,
               sigma_order_number,
               order_type,
               order_subtype AS modify_order_type,
               order_date AS commmercial_order_creation_datetime,
               order_type,
               order_subtype AS modify_order_type
            FROM
               `inm-iar-data-warehouse-dev.sdp_orders.fact_sdp_orders`
         )
      SELECT
         *
      FROM
         orders_base
   )
SELECT
   *
FROM
   main
WHERE
   product_instance_id = 'ID_gs374yej7r';