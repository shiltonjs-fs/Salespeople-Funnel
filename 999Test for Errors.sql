select
    DATE_TRUNC(MONTH, HS_CREATE_DATE),
    COUNT(
        distinct case
            when HS_CREATE_DATE is not null then RECORD_ID
            else null
        end
    ) as LEAD,
    COUNT(
        distinct case
            when SIGN_UP_DATE is not null then RECORD_ID
            else null
        end
    ) as SIGNUP,
    COUNT(
        distinct case
            when MAKE_ONBOARDED_DATE is not null then RECORD_ID
            else null
        end
    ) as KYB,
    COUNT(
        distinct case
            when FIRST_PAYMENT_DATE_MAKE is not null then RECORD_ID
            else null
        end
    ) as TRANSACT,
from
    (
        select distinct
            RECORD_ID,
            COMPANY_ID,
            CONTACT_OWNER,
            CONTACT_OWNER_HISTORICAL,
            HS_CREATE_DATE,
            SIGN_UP_DATE,
            MAKE_ONBOARDED_DATE,
            FIRST_PAYMENT_DATE_MAKE,
            case
                when HS_CREATE_DATE is null
                and SIGN_UP_DATE is not null then 1
                else 0
            end as FUNNEL1_ERROR,
            case
                when SIGN_UP_DATE is null
                and MAKE_ONBOARDED_DATE is not null then 1
                else 0
            end as FUNNEL2_ERROR,
            case
                when MAKE_ONBOARDED_DATE is null
                and FIRST_PAYMENT_DATE_MAKE is not null then 1
                else 0
            end as FUNNEL3_ERROR
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
            where true and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
            )
    )
group by
    1;