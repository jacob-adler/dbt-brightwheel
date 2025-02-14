with
renamed as (
select
    "Name" as company_name,
    "Credential Type" as credential_type,
    "Status" as status,
    "Expiration Date" as expiration_date,
    "Disciplinary Action" as disciplinary_action,
    "Address" as address, {# With more time, clean this to just street address #}
    "State" as state,
    "County" as county,
    "Phone" as phone,
    "Primary Contact Name" as primary_contact_name,
    "Primary Contact Role" as primary_contact_role
from {{ source('brightwheel', 'source1') }}
),

final as (
    select
        company_name,
        credential_type,
        status,
        expiration_date,
        disciplinary_action,
        address,
        state,
        county,
        regexp_replace(phone::string, '[^\d]', '', 'g') as phone,
        primary_contact_name,
        primary_contact_role
    from renamed
)

select * from final