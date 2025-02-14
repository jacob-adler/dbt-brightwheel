with
stg_salesforce_leads as (select * from {{ ref('stg_salesforce_leads') }}),
stg_source1_leads as (select * from {{ ref('stg_source1_leads') }}),
stg_source2_leads as (select * from {{ ref('stg_source2_leads') }}),
stg_source3_leads as (select * from {{ ref('stg_source3_leads') }}),

combined as (
    select
        stg_salesforce_leads.salesforce_lead_id,
        stg_salesforce_leads.is_deleted,
        stg_salesforce_leads.last_name,
        stg_salesforce_leads.first_name,
        stg_salesforce_leads.title,
        coalesce(stg_salesforce_leads.company, stg_source2_leads.company_name, stg_source3_leads.company_name, stg_source1_leads.company_name) as company_name,
        coalesce(stg_salesforce_leads.street, stg_source2_leads.street_address, stg_source3_leads.address, stg_source1_leads.address) as street,
        coalesce(stg_salesforce_leads.city, stg_source2_leads.city, stg_source3_leads.city) as city,
        coalesce(stg_salesforce_leads.state, stg_source2_leads.state, stg_source3_leads.state, stg_source1_leads.state) as state,
        coalesce(stg_salesforce_leads.postal_code, stg_source2_leads.postal_code, stg_source3_leads.postal_code) as postal_code,
        coalesce(stg_source3_leads.county, stg_source1_leads.county) as county,
        stg_salesforce_leads.country,
        coalesce(stg_salesforce_leads.phone, stg_source2_leads.phone, stg_source3_leads.phone, stg_source1_leads.phone) as phone,
        stg_salesforce_leads.mobile_phone,
        coalesce(stg_salesforce_leads.email, stg_source2_leads.email, stg_source3_leads.email) as email,
        stg_salesforce_leads.website,
        stg_salesforce_leads.lead_source,
        stg_salesforce_leads.status,
        stg_salesforce_leads.is_converted,
        stg_salesforce_leads.created_date,
        stg_salesforce_leads.last_modified_date,
        stg_salesforce_leads.last_activity_date,
        stg_salesforce_leads.last_viewed_date,
        stg_salesforce_leads.last_referenced_date,
        stg_salesforce_leads.email_bounced_reason,
        stg_salesforce_leads.email_bounced_date,
        stg_salesforce_leads.outreach_state,
        stg_salesforce_leads.current_enrollment,
        stg_salesforce_leads.capacity,
        stg_salesforce_leads.lead_source_last_updated_date,
        stg_salesforce_leads.brightwheel_school_uuid,
        stg_source1_leads.credential_type,
        stg_source2_leads.license_type,
        {# Trying to classify where the data came from, but I don't think this achieves idempotency. Would spend more time here if more time #}
        case
            when stg_salesforce_leads.phone is not null then 'Salesforce'
            when stg_source2_leads.phone is not null then 'Source 2'
            when stg_source3_leads.phone is not null then 'Source 3'
            when stg_source1_leads.phone is not null then 'Source 1'
            else 'Unexpected value, something went wrong'
        end as data_source

        {# Did not have time to add a timestamp for when the data is first loaded in.
        I think this is something that would be good to do during the initial load into the warehouse #}
    from stg_salesforce_leads
    full join stg_source1_leads on stg_source1_leads.phone = stg_salesforce_leads.phone
    full join stg_source2_leads on stg_source2_leads.phone = stg_salesforce_leads.phone 
    full join stg_source3_leads on stg_source3_leads.phone = stg_salesforce_leads.phone
    {# TODO: add a qualify statement to deduplicate.
    This could be better off in each staging model since it would be caused by dupes from the same source
    You might tiebreak on which record is more complete (ex. count of non-null fields) #}
)

select * from combined