WITH main AS (
   WITH orders_base AS (
      SELECT 
   )
SELECT
  *
  -- product_offering_id AS product_code,
  -- secondary_flag AS secondary_service,
  -- customer_division AS customer_division_name,
  -- customer_folder AS customer_division_folder,
  -- remote_site_id,
  -- minimum_subscription_period,
  -- order_date AS commmercial_order_creation_datetime,
  -- order_type,
  -- order_subtype AS modify_order_type
FROM inm-iar-data-warehouse-dev.sdp_orders.fact_sdp_orders fso
LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.fact_product_offering_instances fpoi ON fso.product_instance_id = fpoi.product_instance_id
LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_customer pc ON fso.product_instance_id = pc.product_instance_id
LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_site ps ON fso.product_instance_id = ps.product_instance_id
-- LEFT JOIN inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_product pp ON fso.product_instance_id = pp.product_instance_id 
-- ALSO JOIN ON product_specification_id
WHERE fso.product_instance_id = 'ID_gs374yej7r')
