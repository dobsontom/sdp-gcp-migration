SELECT
    sdp_instance_id,
    peoplesoft_instadence_id
FROM
    `inm-iar-data-warehouse-dev.unified_subscribers.fact_unified_subscriber_instance` a
    LEFT JOIN inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_site b ON a.unified_instance_id = b.unified_instance_id
    LEFT JOIN inm-iar-data-warehouse-dev.unified_subscribers.dim_unified_subscriber_product c ON a.unified_instance_id = c.unified_instance_id;