select
    id as salesforce_lead_id,
    is_deleted,
    last_name,
    first_name,
    title,
    company,
    street,
    city,
    state,
    postal_code::string as postal_code,
    country,
    regexp_replace(phone::string, '[^\d]', '', 'g') as phone,
    mobile_phone,
    email,
    website,
    lead_source,
    status,
    is_converted,
    created_date,
    last_modified_date,
    last_activity_date,
    last_viewed_date,
    last_referenced_date,
    email_bounced_reason,
    email_bounced_date,
    outreach_stage_c as outreach_state,
    current_enrollment_c as current_enrollment,
    capacity_c as capacity,
    lead_source_last_updated_c as lead_source_last_updated_date,
    brightwheel_school_uuid_c as brightwheel_school_uuid
from {{ source('brightwheel', 'salesforce_leads') }}