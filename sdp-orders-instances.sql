CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.sdp_report.orders` AS (

    WITH
    orders_base AS (
        SELECT
            product_instance_id,
            sigma_order_number,
            order_type,
            order_subtype AS modify_order_type,
            order_date
        FROM
            `inm-iar-data-warehouse-dev.sdp_orders.fact_sdp_orders`
    ),

    product_offering_instances AS (
        SELECT
            product_instance_id,
            product_offering_id,
            secondary_flag
        FROM
            `inm-iar-data-warehouse-dev.sdp_product_instances.fact_product_offering_instances`
    ),

    orders_product_instances AS (
        SELECT
            o.*,
            p.* EXCEPT (product_instance_id)
        FROM
            orders_base AS o
        LEFT JOIN product_offering_instances AS p ON o.product_instance_id = p.product_instance_id
    ),

    add_customer AS (
        SELECT
            opi.*,
            c.customer_division,
            c.customer_folder
        FROM
            orders_product_instances AS opi
        LEFT JOIN
            `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_customer` AS c
            ON opi.product_instance_id = c.product_instance_id
    ),

    add_site AS (
        SELECT
            c.*,
            site.product_specification_id,
            site.remote_site_id
        FROM
            add_customer AS c
        LEFT JOIN
            `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_site` AS site
            ON c.product_instance_id = site.product_instance_id
    )

    -- ,
    -- add_sims AS (
    --    SELECT
    --       site.* EXCEPT (product_specification_id),
    --       sim.product_specification_id
    --    FROM
    --       add_site site
    --       LEFT JOIN `sdp_product_instances.dim_package_simcards` sim ON site.product_instance_id = sim.product_instance_id
    -- ),
    -- add_product AS (
    --    SELECT
    --       s.*,
    --       p.minimum_subscription_period
    --    FROM
    --       add_sims s
    --       LEFT JOIN `inm-iar-data-warehouse-dev.sdp_product_instances.dim_package_product` p ON s.product_instance_id = p.product_instance_id
    --       AND s.product_specification_id = p.product_specification_id
    -- )

    SELECT
        * EXCEPT (product_specification_id),
        ROW_NUMBER() OVER (
            PARTITION BY
                product_instance_id
            ORDER BY
                order_date ASC
        ) AS change_order
    FROM
        add_site
-- WHERE
--    product_instance_id = 'ID_gs374yej7r'
);
