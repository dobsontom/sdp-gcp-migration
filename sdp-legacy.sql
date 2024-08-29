WITH
main AS (
    SELECT
        usi.instance_name AS product_offering_name,
        usc.customer_cle_id AS cle_id,
        usc.customer_cle_name AS cle_name,
        usc.customer_account_id AS billing_account_id,
        uss.site_folder_name,
        uss.site_folder_id AS site_folder,
        uss.site_id,
        uss.site_name,
        cmd.account_number AS sap_site_id,
        uss.site_type,
        usid.network_identity AS imsi,
        uss.aircraft_tail_number AS tail_number,
        usp.subscription_plan_name,
        usnd.nsd_type,
        ust.terminal_provisioning_key,
        usi.activation_dttm AS activation_start_date,
        usp.subscription_end_date AS subscription_period_end_date,
        usp.recommitment_start_date,
        usp.recommitment_end_date,
        ust.gx_terminal_did,
        usp.related_from_dttm AS created_date_time,
        ust.related_from_dttm AS actual_activation_date_time,
        ust.is_current_record AS is_latest,
        usi.activation_dttm AS effective_start_date,
        usi.disconnect_dttm AS effective_end_date,
        COALESCE(usi.sdp_instance_id, usi.peoplesoft_instance_id) AS product_offering_instance_id,
        COALESCE(
            usp.is_current_record = TRUE
            AND usid.is_current_record = TRUE
            AND usnd.is_current_record = TRUE
            AND ust.is_current_record = TRUE, FALSE
        ) AS is_current_record__c,
        COALESCE(
            usp.component_name = 'GX'
            OR usid.msisdn_usage = 'GX'
            OR ust.terminal_network = 'GX', FALSE
        ) AS gx_flag
    FROM
        `inm-iar-data-warehouse-dev.unified_subscribers.fact_unified_subscriber_instance` AS usi
    -- NO DUPLICATION
    INNER JOIN
        `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_site` AS uss
        ON usi.unified_instance_id = uss.unified_instance_id
    -- *8
    INNER JOIN
        `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_product` AS usp
        ON uss.unified_instance_id = usp.unified_instance_id
    -- NO DUPLICATION
    INNER JOIN
        `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_customer` AS usc
        ON usi.provisioning_account_id = usc.provisioning_account_id
    INNER JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_identity` AS usid
        ON usi.unified_instance_id = usid.unified_instance_id
        AND NOT (
            usp.related_to_dttm <= usid.related_from_dttm
            AND usp.related_to_dttm <= usid.related_to_dttm
            OR usp.related_from_dttm >= usid.related_to_dttm
            AND usp.related_from_dttm >= usid.related_from_dttm
        ) -- *6
    INNER JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_networkdevice` AS usnd
        ON usi.unified_instance_id = usnd.unified_instance_id
        AND NOT (
            usp.related_to_dttm <= usnd.related_from_dttm
            AND usp.related_to_dttm <= usnd.related_to_dttm
            OR usp.related_from_dttm >= usnd.related_to_dttm
            AND usp.related_from_dttm >= usnd.related_from_dttm
        ) -- NO DUPLICATION
    INNER JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_terminal` AS ust
        ON usi.unified_instance_id = ust.unified_instance_id
        AND NOT (
            usp.related_to_dttm <= ust.related_from_dttm
            AND usp.related_to_dttm <= ust.related_to_dttm
            OR usp.related_from_dttm >= ust.related_to_dttm
            AND usp.related_from_dttm >= ust.related_from_dttm
        ) -- *2
    INNER JOIN `inm-iar-data-warehouse-dev.cmd.sites` AS cmd ON uss.site_id = cmd.cmd_site_id -- NO DUPLICATION
    WHERE
        1 = 1
        AND COALESCE(usi.sdp_instance_id, usi.peoplesoft_instance_id) = 'ID_gs374yej7r'
)

SELECT
    *
FROM
    main
WHERE
    gx_flag = TRUE;
