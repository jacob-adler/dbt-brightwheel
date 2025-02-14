with
renames as (
    select
        "Operation" as operation_id,
        "Agency Number" as agency_number,
        "Operation Name" as company_name,
        "Address" as address,
        "City" as city,
        "State" as state,
        "Zip" as postal_code,
        "County" as county,
        "Phone" as phone,
        "Type" as type,
        "Status" as permit_status,
        "Issue Date" as permit_issue_date,
        "Capacity" as capacity,
        "Email Address" as email,
        "Facility ID" as facility_id,
        "Monitoring Frequency" as monitoring_frequency,
        "Infant" as has_infant,
        "Toddler" as has_toddler,
        "Preschool" as has_preschool,
        "School" as has_school
    from {{ source('brightwheel', 'source3') }}
),

final as (
    select
        operation_id,
        agency_number,
        company_name,
        address,
        city,
        state,
        postal_code,
        county,
        regexp_replace(phone, '[^\d]', '', 'g') as phone,
        type,
        permit_status,
        permit_issue_date,
        capacity,
        email,
        facility_id,
        monitoring_frequency,
        has_infant,
        has_toddler,
        has_preschool,
        has_school
    from renames
)

select * from final