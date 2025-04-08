{{ config(
    materialized='incremental',
    unique_key='sales_lead_row_id',
    on_schema_change='append_new_columns'
) }}

WITH lead_map AS (
    SELECT * FROM {{ ref('sales_lead_map') }}
     {% if is_incremental() %}
        WHERE landing_updated_date_time > (
            SELECT MAX(landing_updated_date_time) FROM {{ this }}
        )
    {% endif %}
),

final AS (
SELECT
    lead_id::VARCHAR AS lead_id,
    source_system_created_by_id::VARCHAR AS source_system_created_by_id,
    source_system_created_date::TIMESTAMP_NTZ AS source_system_created_date,
    source_system_updated_by_id::VARCHAR AS source_system_updated_by_id,
    source_system_updated_date::TIMESTAMP_NTZ AS source_system_updated_date,
    lead_source_id::VARCHAR AS lead_source_id,
    region::VARCHAR AS region,
    converted_account_source_id::VARCHAR AS converted_account_source_id,
    sales_person_id::VARCHAR AS sales_person_id,
    converted_account_id::VARCHAR AS converted_account_id,
    owner_source_id::VARCHAR AS owner_source_id,
    OWNER_ID ::VARCHAR AS owner_id,
    lead_status::VARCHAR AS lead_status,
    isdeleted::BOOLEAN AS isdeleted,
    country::VARCHAR AS country,
    isconverted::BOOLEAN AS isconverted,
    mal_date::TIMESTAMP_NTZ AS mal_date,
    new_mql_date::TIMESTAMP_NTZ AS new_mql_date,
    sal_date::TIMESTAMP_NTZ AS sal_date,
    converted_date::TIMESTAMP_NTZ AS converted_date,
    lead_type::VARCHAR AS lead_type,
    lead_last_modified_date::TIMESTAMP_NTZ AS lead_last_modified_date,
    lead_last_source_update_date::TIMESTAMP_NTZ AS lead_last_source_update_date,
    annual_sales::NUMBER AS annual_sales,
    annual_revenue::NUMBER AS annual_revenue,
    duns_number::VARCHAR AS duns_number,
    last_activity_date::TIMESTAMP_NTZ AS last_activity_date,
    lead_source::VARCHAR AS lead_source,
    lead_source_name::VARCHAR AS lead_source_name,
    created_by_source_id::VARCHAR AS created_by_source_id,
    channel_partner::VARCHAR AS channel_partner,
    channel_partner_type ::VARCHAR AS channel_partner_type, 
    LEADER_SEGMENT,
    SUB_SEGMENT,
    STRATEGIC_SEGMENT,
    landing_updated_date_time::TIMESTAMP_NTZ AS landing_updated_date_time,    
    Owner_L4_Manager,
    sales_lead_row_id
FROM lead_map
),

{{
    tag_inserts_and_updates(
        source_data = 'SELECT * FROM final',
        unique_id = ['sales_lead_row_id']
    )
}}
