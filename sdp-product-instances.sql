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
         ),
         product_offering_instances AS (
            SELECT
               product_instance_id,
               product_offering_id AS product_code,
               secondary_flag
            FROM
               `inm-iar-data-warehouse-dev.sdp_product_instances.fact_product_offering_instances`
         ),
         orders_product_instances AS (
            SELECT
               o.*,
               p.* EXCEPT (product_instance_id)
            FROM
               orders_base o
               LEFT JOIN product_offering_instances p ON o.product_instance_id = p.product_instance_id
         ),
         add_customer AS (
            SELECT
               opi.*,
               c.customer_division,
               c.customer_folder
            FROM
               orders_product_instances opi
               LEFT JOIN `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_customer` c ON opi.product_instance_id = c.product_instance_id
         ),
         add_site AS (
            SELECT
               c.*,
               site.product_specification_id,
               site.remote_site_id
            FROM
               add_customer c
               LEFT JOIN `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_site` site ON c.product_instance_id = site.product_instance_id
         )
         add_sims AS (
            SELECT
               site.*,
               sim.product_specification_id
            FROM
               add_site site
               LEFT JOIN `sdp_product_instances.dim_package_simcards` sim ON site.product_instance_id = sim.product_instance_id
         )
         add_product AS (
            SELECT
               s.*,
               p.minimum_subscription_period
            FROM
               add_site s
               LEFT JOIN `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_product` p ON s.product_instance_id = p.product_instance_id
               AND s.product_specification_id = p.product_specification_id
         )
      SELECT
         *
      FROM
         add_site
   )
SELECT
   *
FROM
   main
WHERE
   product_instance_id = 'ID_gs374yej7r';


-- SELECT
--   *
--   -- product_offering_id AS product_code,
--   -- secondary_flag AS secondary_service,
--   -- customer_division AS customer_division_name,
--   -- customer_folder AS customer_division_folder,
--   -- remote_site_id,
--   -- minimum_subscription_period,
--   -- order_date AS commmercial_order_creation_datetime,
--   -- order_type,
--   -- order_subtype AS modify_order_type
-- FROM inm-iar-data-warehouse-dev.sdp_orders.fact_sdp_orders fso
-- LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.fact_product_offering_instances fpoi ON fso.product_instance_id = fpoi.product_instance_id
-- LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_customer pc ON fso.product_instance_id = pc.product_instance_id
-- LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_site ps ON fso.product_instance_id = ps.product_instance_id
-- -- LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_product pp ON fso.product_instance_id = pp.product_instance_id 
-- -- ALSO JOIN ON product_specification_id
-- WHERE fso.product_instance_id = 'ID_gs374yej7r')
