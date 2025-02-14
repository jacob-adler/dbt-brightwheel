with
renamed as (
    select
        "Type License" as license_type,
        "Company" as company_name, {# Often this is the owner name, perhaps sole proprietorship #}
        "Accepts Subsidy" as does_accept_subsidy, {# Add a case when to turn this into a boolean #}
        "Year Round" as is_year_round, {# Add a case when to turn this into a boolean #}
        "Daytime Hours" as is_daytime_hours, {# Add a case when to turn this into a boolean #}
        "Star Level" as star_level, {# Extract first char and cast to number #}
        "Mon" as operating_hours_monday,
        "Tues" as operating_hours_tuesday,
        "Wed" as operating_hours_wednesday,
        "Thurs" as operating_hours_thursday,
        "Friday" as operating_hours_friday,
        "Saturday" as operating_hours_saturday,
        "Sunday" as operating_hours_sunday,
        "Primary Caregiver" as primary_caregiver, {# Need to trim "Primary Caregiver" with a regex #}
        "Phone" as phone, {# Trim all non-numeric characters #}
        "Email" as email,
        "Address1" as address_1, {# Combine these into one field called 'street' #}
        "Address2" as address_2,
        "City" as city,
        "State" as state,
        "Zip" as postal_code,
        "Subsidy Contract Number" as subsidy_contract_number, {# Need to trim "Subsidy Contract Number: " with a regex #}
        "Total Cap" as total_cap,
        "Ages Accepted 1" as ages_accepted_1,
        "AA2" as ages_accepted_2,
        "AA3" as ages_accepted_3,
        "AA4" as ages_accepted_4,
        "License Monitoring Since" as license_monitoring_since_date, {# Need to trim "Monitoring since" with a regex #}
        "School Year Only" as is_school_year_only, {# Nullif value is "Accepts Subsidy" #}
        "Evening Hours" as evening_hours {# Can't make of what data is supposed to be in there #}
    from {{ source('brightwheel', 'source2') }}
),

final as (
    select
        license_type,
        company_name,
        does_accept_subsidy,
        is_year_round,
        is_daytime_hours,
        star_level,
        operating_hours_monday,
        operating_hours_tuesday,
        operating_hours_wednesday,
        operating_hours_thursday,
        operating_hours_friday,
        operating_hours_saturday,
        operating_hours_sunday,
        primary_caregiver,
        regexp_replace(phone, '[^\d]', '', 'g') as phone,
        email,
        address_1,
        address_2,
        address_1 || ' ' || address_2 as street_address,
        city,
        state,
        postal_code,
        subsidy_contract_number,
        total_cap,
        ages_accepted_1,
        ages_accepted_2,
        ages_accepted_3,
        ages_accepted_4,
        license_monitoring_since_date,
        is_school_year_only,
        evening_hours
    from renamed
)

select * from final