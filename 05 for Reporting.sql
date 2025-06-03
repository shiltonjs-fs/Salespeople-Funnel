--overall funnel conversion and by month, nov 23 to oct 24
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
        where
            true
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
where
    DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
group by
    1;

--overall TAT, may to oct 24
with
    TEST as (
        select
            RECORD_ID,
            DAYS_LEAD_TO_SIGN_UP,
            DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
            DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
            DAYS_LEAD_TO_SIGN_UP + DAYS_SIGN_UP_TO_ONBOARDED_MAKE + DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE XX
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
        where
            DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
select
    APPROX_PERCENTILE(XX, 0.5) as MEDIAN,
    APPROX_PERCENTILE(DAYS_LEAD_TO_SIGN_UP, 0.5) as MEDIAN1,
    APPROX_PERCENTILE(DAYS_SIGN_UP_TO_ONBOARDED_MAKE, 0.5) as MEDIAN2,
    APPROX_PERCENTILE(DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE, 0.5) as MEDIAN3,
from
    TEST;

--F30D make median, nov 23 to oct 24
with
    X as (
        select
            RECORD_ID,
            FIRST_30D_PAYMENT_AMOUNT_MAKE
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
        where
            DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
            and FIRST_30D_PAYMENT_AMOUNT_MAKE is not null
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
select
    COUNT(1),
    APPROX_PERCENTILE(FIRST_30D_PAYMENT_AMOUNT_MAKE, 0.5) as MEDIAN
from
    X;

--overall funnel conversion and TAT by salesppl and month, may to oct 24
select
    case
        when LOWER(CONTACT_OWNER) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%' then 1
        else 0
    end as KEEGAN,
    case
        when LOWER(CONTACT_OWNER) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%' then 1
        else 0
    end as LEIA,
    case
        when LOWER(CONTACT_OWNER) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%' then 1
        else 0
    end as JON,
    DATE_TRUNC(MONTH, HS_CREATE_DATE),
    APPROX_PERCENTILE(FIRST_30D_PAYMENT_AMOUNT_MAKE, 0.5) as MEDIAN_F30DPAY_MAKE,
    APPROX_PERCENTILE(XX, 0.5) as MEDIAN,
    APPROX_PERCENTILE(DAYS_LEAD_TO_SIGN_UP, 0.5) as MEDIAN1,
    APPROX_PERCENTILE(DAYS_SIGN_UP_TO_ONBOARDED_MAKE, 0.5) as MEDIAN2,
    APPROX_PERCENTILE(DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE, 0.5) as MEDIAN3,
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
            CONTACT_OWNER_FROM_DWH,
            HS_CREATE_DATE,
            SIGN_UP_DATE,
            MAKE_ONBOARDED_DATE,
            FIRST_PAYMENT_DATE_MAKE,
            FIRST_30D_PAYMENT_AMOUNT_MAKE,
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
            end as FUNNEL3_ERROR,
            DAYS_LEAD_TO_SIGN_UP,
            DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
            DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
            DAYS_LEAD_TO_SIGN_UP + DAYS_SIGN_UP_TO_ONBOARDED_MAKE + DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE XX
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
        where
            true
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
where
    DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
group by
    ROLLUP (1, 2, 3),
    4;

--overall funnel conversion and TAT by salesppl, may to oct 24
select
    case
        when LOWER(CONTACT_OWNER) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%' then 1
        else 0
    end as KEEGAN,
    case
        when LOWER(CONTACT_OWNER) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%' then 1
        else 0
    end as LEIA,
    case
        when LOWER(CONTACT_OWNER) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%' then 1
        else 0
    end as JON,
    APPROX_PERCENTILE(FIRST_30D_PAYMENT_AMOUNT_MAKE, 0.5) as MEDIAN_F30DPAY_MAKE,
    APPROX_PERCENTILE(XX, 0.5) as MEDIAN,
    APPROX_PERCENTILE(DAYS_LEAD_TO_SIGN_UP, 0.5) as MEDIAN1,
    APPROX_PERCENTILE(DAYS_SIGN_UP_TO_ONBOARDED_MAKE, 0.5) as MEDIAN2,
    APPROX_PERCENTILE(DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE, 0.5) as MEDIAN3,
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
            CONTACT_OWNER_FROM_DWH,
            HS_CREATE_DATE,
            SIGN_UP_DATE,
            MAKE_ONBOARDED_DATE,
            FIRST_PAYMENT_DATE_MAKE,
            FIRST_30D_PAYMENT_AMOUNT_MAKE,
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
            end as FUNNEL3_ERROR,
            DAYS_LEAD_TO_SIGN_UP,
            DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
            DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
            DAYS_LEAD_TO_SIGN_UP + DAYS_SIGN_UP_TO_ONBOARDED_MAKE + DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE XX
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
        where
            true
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
where
    DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
group by
    ROLLUP (1, 2, 3);

--happy vs unhappy path TAT, may to oct 24
with
    TEST as (
        select
            RECORD_ID,
            DAYS_LEAD_TO_SIGN_UP,
            DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
            DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
            DAYS_LEAD_TO_SIGN_UP + DAYS_SIGN_UP_TO_ONBOARDED_MAKE + DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE XX
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING
        where
            DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
select
    case
        when DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE is null then 'unhappy'
        else 'happy'
    end as PATH,
    APPROX_PERCENTILE(XX, 0.5) as MEDIAN,
    APPROX_PERCENTILE(DAYS_LEAD_TO_SIGN_UP, 0.5) as MEDIAN1,
    APPROX_PERCENTILE(DAYS_SIGN_UP_TO_ONBOARDED_MAKE, 0.5) as MEDIAN2,
    APPROX_PERCENTILE(DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE, 0.5) as MEDIAN3,
from
    TEST
group by
    1;

select
    OUTBOUND_INBOUND_LEAD,
    case
        when LOWER(CONTACT_OWNER) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%' then 1
        else 0
    end as KEEGAN,
    case
        when LOWER(CONTACT_OWNER) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%' then 1
        else 0
    end as LEIA,
    case
        when LOWER(CONTACT_OWNER) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
        or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%' then 1
        else 0
    end as JON,
    APPROX_PERCENTILE(FIRST_30D_PAYMENT_AMOUNT_MAKE, 0.5) as MEDIAN_F30DPAY_MAKE,
    APPROX_PERCENTILE(XX, 0.5) as MEDIAN,
    APPROX_PERCENTILE(DAYS_LEAD_TO_SIGN_UP, 0.5) as MEDIAN1,
    APPROX_PERCENTILE(DAYS_SIGN_UP_TO_ONBOARDED_MAKE, 0.5) as MEDIAN2,
    APPROX_PERCENTILE(DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE, 0.5) as MEDIAN3,
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
            OUTBOUND_INBOUND_LEAD,
            CONTACT_OWNER,
            CONTACT_OWNER_HISTORICAL,
            CONTACT_OWNER_FROM_DWH,
            HS_CREATE_DATE,
            SIGN_UP_DATE,
            MAKE_ONBOARDED_DATE,
            FIRST_PAYMENT_DATE_MAKE,
            FIRST_30D_PAYMENT_AMOUNT_MAKE,
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
            end as FUNNEL3_ERROR,
            DAYS_LEAD_TO_SIGN_UP,
            DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
            DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
            DAYS_LEAD_TO_SIGN_UP + DAYS_SIGN_UP_TO_ONBOARDED_MAKE + DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE XX
        from
            DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING T1
        where
            true
            and (
                LOWER(CONTACT_OWNER) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%keegan%'
                or LOWER(CONTACT_OWNER) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%leia%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%leia%'
                or LOWER(CONTACT_OWNER) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_HISTORICAL) LIKE '%jon%'
                or LOWER(CONTACT_OWNER_FROM_DWH) LIKE '%jon%'
            )
    )
where
    DATE(HS_CREATE_DATE) between DATE('2024-05-01') and DATE('2024-10-31')
group by
    ROLLUP (2, 3, 4), 1;

select
    *
from
    DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT_INBOUND_OUTBOUND
limit
    10;

select distinct OUTBOUND_INBOUND_LEAD from DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING;