WITH
   main AS (
      SELECT
         COALESCE(usi.sdp_instance_id, usi.peoplesoft_instance_id) AS product_offering_instance_id,
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
         CASE
            WHEN usp.is_current_record = TRUE
            AND usid.is_current_record = TRUE
            AND usnd.is_current_record = TRUE
            AND ust.is_current_record = TRUE THEN TRUE
            ELSE FALSE
         END AS is_current_record__c,
         CASE
            WHEN usp.component_name = 'GX'
            OR usid.msisdn_usage = 'GX'
            OR ust.terminal_network = 'GX' THEN TRUE
            ELSE FALSE
         END AS gx_flag
      FROM
         `inm-iar-data-warehouse-dev.unified_subscribers.fact_unified_subscriber_instance` usi
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_site` uss ON usi.unified_instance_id = uss.unified_instance_id -- NO DUPLICATION
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_product` usp ON uss.unified_instance_id = usp.unified_instance_id -- *8
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_customer` usc ON usi.provisioning_account_id = usc.provisioning_account_id -- NO DUPLICATION
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_identity` usid ON usi.unified_instance_id = usid.unified_instance_id
         AND NOT (
            usid.related_from_dttm >= usp.related_to_dttm
            AND usid.related_to_dttm >= usp.related_to_dttm
            OR usid.related_to_dttm <= usp.related_from_dttm
            AND usid.related_from_dttm <= usp.related_from_dttm
         ) -- *6
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_networkdevice` usnd ON usi.unified_instance_id = usnd.unified_instance_id
         AND NOT (
            usnd.related_from_dttm >= usp.related_to_dttm
            AND usnd.related_to_dttm >= usp.related_to_dttm
            OR usnd.related_to_dttm <= usp.related_from_dttm
            AND usnd.related_from_dttm <= usp.related_from_dttm
         ) -- NO DUPLICATION
         JOIN `inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_terminal` ust ON usi.unified_instance_id = ust.unified_instance_id
         AND NOT (
            ust.related_from_dttm >= usp.related_to_dttm
            AND ust.related_to_dttm >= usp.related_to_dttm
            OR ust.related_to_dttm <= usp.related_from_dttm
            AND ust.related_from_dttm <= usp.related_from_dttm
         ) -- *2
         JOIN `inm-iar-data-warehouse-dev.cmd.sites` cmd ON uss.site_id = cmd.cmd_site_id -- NO DUPLICATION
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