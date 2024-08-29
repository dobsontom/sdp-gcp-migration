CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.sdp_report.subscriptions` AS (
    WITH
    subscriber_base AS (
        SELECT
            unified_instance_id,
            provisioning_account_id,
            instance_name,
            sdp_instance_id,
            peoplesoft_instance_id,
            activation_dttm,
            disconnect_dttm
        FROM
            `inm-iar-data-warehouse-dev.unified_subscribers.fact_unified_subscriber_instance`
    ),

    site AS (
        SELECT
            unified_instance_id,
            site_folder_name,
            site_folder_id,
            site_id,
            site_name,
            site_type,
            aircraft_tail_number
        FROM
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_site`
    ),

    customer AS (
        SELECT
            provisioning_account_id,
            customer_cle_id,
            customer_cle_name,
            customer_account_id
        FROM
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_customer`
    ),

    base_site_customer AS (
        SELECT
            base.*,
            site.* EXCEPT (unified_instance_id),
            customer.* EXCEPT (provisioning_account_id)
        FROM
            subscriber_base AS base
        LEFT JOIN site ON base.unified_instance_id = site.unified_instance_id
        LEFT JOIN customer ON base.provisioning_account_id = customer.provisioning_account_id
    ),

    add_cmd_site AS (
        SELECT
            bsc.*,
            cmd.account_number AS sap_site_id
        FROM
            base_site_customer AS bsc
        LEFT JOIN `inm-iar-data-warehouse-dev.cmd.sites` AS cmd ON bsc.site_id = cmd.cmd_site_id
    ),

    add_product AS (
        SELECT
            cmd.*,
            p.component_id,
            p.component_name,
            p.subscription_plan_name,
            p.subscription_end_date,
            p.recommitment_start_date,
            p.recommitment_end_date,
            p.related_from_dttm AS product_related_from_dttm,
            p.related_to_dttm AS product_related_to_dttm
        FROM
            add_cmd_site AS cmd
        LEFT JOIN
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_product` AS p
            ON cmd.unified_instance_id = p.unified_instance_id
    ),

    add_identity AS (
        SELECT
            p.*,
            i.network_identity,
            i.msisdn_usage,
            i.related_from_dttm AS identity_related_from_dttm,
            i.related_to_dttm AS identity_related_to_dttm
        FROM
            add_product AS p
        LEFT JOIN
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_identity` AS i
            ON p.unified_instance_id = i.unified_instance_id
            AND p.component_id = i.component_id
            AND NOT (
                p.product_related_from_dttm > i.related_from_dttm
                AND p.product_related_from_dttm > i.related_to_dttm
                OR p.product_related_to_dttm < i.related_from_dttm
                AND p.product_related_to_dttm < i.related_to_dttm
            )
    ),

    add_networkdevice AS (
        SELECT
            i.*,
            n.nsd_type
        FROM
            add_identity AS i
        LEFT JOIN
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_networkdevice` AS n
            ON i.unified_instance_id = n.unified_instance_id
            AND i.component_id = n.component_id
            AND NOT (
                i.product_related_from_dttm > n.related_from_dttm
                AND i.product_related_from_dttm > n.related_to_dttm
                OR i.product_related_to_dttm < n.related_from_dttm
                AND i.product_related_to_dttm < n.related_to_dttm
            )
    ),

    add_terminal AS (
        SELECT
            n.*,
            t.terminal_provisioning_key,
            t.gx_terminal_did,
            t.related_from_dttm AS actual_activation_date_time,
            t.is_current_record AS is_latest,
            t.terminal_network
        FROM
            add_networkdevice AS n
        LEFT JOIN
            `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_terminal` AS t
            ON n.unified_instance_id = t.unified_instance_id
            AND NOT (
                n.product_related_from_dttm > t.related_from_dttm
                AND n.product_related_from_dttm > t.related_to_dttm
                OR n.product_related_to_dttm < t.related_from_dttm
                AND n.product_related_to_dttm < t.related_to_dttm
            )
    )

    SELECT
        unified_instance_id,
        instance_name,
        sdp_instance_id,
        peoplesoft_instance_id,
        activation_dttm,
        disconnect_dttm,
        site_folder_name,
        site_folder_id,
        site_id,
        site_name,
        site_type,
        aircraft_tail_number,
        customer_cle_id,
        customer_cle_name,
        customer_account_id,
        sap_site_id,
        subscription_plan_name,
        subscription_end_date,
        recommitment_start_date,
        recommitment_end_date,
        product_related_from_dttm,
        network_identity,
        nsd_type,
        terminal_provisioning_key,
        gx_terminal_did,
        actual_activation_date_time,
        is_latest,
        COALESCE(
            component_name = 'GX'
            OR msisdn_usage = 'GX'
            OR terminal_network = 'GX',
            FALSE
        ) AS gx_flag,
        ROW_NUMBER() OVER (
            PARTITION BY
                unified_instance_id
            ORDER BY
                actual_activation_date_time ASC
        ) AS change_order
    FROM
        add_terminal
        -- WHERE
        -- sdp_instance_id = 'ID_gs374yej7r';
);
