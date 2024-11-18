create or replace view DEV.SBOX_SHILTON.CARDUP_B2B_SG_FUNNEL_WITH_HISTORICAL_TAGGING as
with
    HS_TABLE as (
        select
            RECORD_ID,
            CURRENT_CONTACT_OWNER CONTACT_OWNER,
            CONCAT_WS(
                ' ',
                COALESCE(PREV_CONTACT_OWNER_1, ''),
                COALESCE(PREV_CONTACT_OWNER_2, ''),
                COALESCE(PREV_CONTACT_OWNER_3, ''),
                COALESCE(PREV_CONTACT_OWNER_4, '')
            ) CONTACT_OWNER_HISTORICAL
        from
            (
                select distinct
                    *
                from
                    DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL
            )
    ),
    MAKE_ONBOARDING_TABLE as (
        select
            U.USER_ID,
            X.KYB_COMPLETED_DATE MAKE_ONBOARDED_DATE,
            U.CREATED_AT_SG SIGN_UP_DATE,
            X.INDUSTRY INDUSTRY
        from
            CBM.CARDUP_DB_REPORTING.COMPANY_DATA X
            join CBM.CARDUP_DB_REPORTING.USER_DATA U on X.COMPANY_ID = U.COMPANY_ID
        where
            U.CU_LOCALE_ID = 1
            and X.CU_LOCALE_ID = 1
            and STATUS = 'Active'
    ),
    COLLECT_TABLE as (
        select
            PAYEE_ID USER_ID,
            COLLECT_SETUP_SUBMITTED_DATE CSS_DATE,
            COLLECTOR_ONBOARDED_DATE COLLECT_ONBOARDED_DATE
        from
            CBM.CARDUP_DB_REPORTING.COLLECT_PROPERTIES
        where
            CU_LOCALE_ID = 1
    ),
    PAYMENT_TABLE as (
        select
            USER_ID
            --, t2.cardup_payment_payment_type cardup_payment_payment_type_make
            --, t3.cardup_payment_payment_type cardup_payment_payment_type_collect
,
            FIRST_PAYMENT_DATE_MAKE,
            FIRST_PAYMENT_DATE_COLLECT
            --, t2.cardup_payment_usd_amt      first_payment_amount_make
            --, t3.cardup_payment_usd_amt      first_payment_amount_collect
        from
            (
                select
                    CARDUP_PAYMENT_USER_ID USER_ID,
                    MIN(
                        case
                            when CARDUP_PAYMENT_USER_TYPE = 'business' then CARDUP_PAYMENT_SUCCESS_AT_UTC_TS
                            else null
                        end
                    ) FIRST_PAYMENT_DATE_MAKE,
                    MIN(
                        case
                            when CARDUP_PAYMENT_USER_TYPE = 'guest' then CARDUP_PAYMENT_SUCCESS_AT_UTC_TS
                            else null
                        end
                    ) FIRST_PAYMENT_DATE_COLLECT
                from
                    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
                WHERE
                    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
                    AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
                    and CARDUP_PAYMENT_CU_LOCALE_ID = 1
                group by
                    1
            ) T1
            --   left join ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t2
            --             on t1.user_id = t2.CARDUP_PAYMENT_USER_ID and
            --                t1.first_payment_date_make =
            --                t2.cardup_payment_taken_date
            --   left join ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t3
            --             on t1.user_id = t3.CARDUP_PAYMENT_USER_PAYEE_ID and
            --                t1.first_payment_date_collect =
            --                t3.cardup_payment_taken_date
    ),
    MAIN as (
        select
            *
        from
            (
                select
                    RECORD_ID,
                    USER_ID,
                    CONTACT_OWNER,
                    CONTACT_OWNER_HISTORICAL,
                    DATE(HUBSPOT_CREATED_DATE) HS_CREATE_DATE,
                    SIGN_UP_DATE,
                    BECAME_INTERESTED_IN_COLLECT_DATE IIC_DATE,
                    INDUSTRY,
                    OUTBOUND_INBOUND_LEAD,
                    MAKE_ONBOARDED_DATE,
                    CSS_DATE,
                    COLLECT_ONBOARDED_DATE,
                    CARDUP_PAYMENT_PAYMENT_TYPE_MAKE,
                    CARDUP_PAYMENT_PAYMENT_TYPE_COLLECT,
                    FIRST_PAYMENT_DATE_MAKE,
                    FIRST_PAYMENT_DATE_COLLECT
                    --, first_payment_amount_make
                    --, first_payment_amount_collect
,
                    FIRST_30D_PAYMENT_AMOUNT_MAKE,
                    FIRST_30D_PAYMENT_AMOUNT_COLLECT
                from
                    HS_TABLE
                    left outer join (
                        select
                            CAST(HUBSPOT_CONTACT_ID as INT) RECORD_ID,
                            USER_UTM_ID USER_ID,
                            HUBSPOT_CREATED_DATE,
                            BECAME_INTERESTED_IN_COLLECT_DATE
                        from
                            CBM.CARDUP_DB_REPORTING.USER_UTM
                    ) USER_UTM using (RECORD_ID)
                    left outer join COLLECT_TABLE using (USER_ID)
                    left outer join MAKE_ONBOARDING_TABLE using (USER_ID)
                    left outer join DEV.SBOX_SHILTON.CARDUP_B2B_SG_USERID_F30DMAKE_F30DCOLLECT_AMOUNT F30D using (USER_ID)
                    left outer join PAYMENT_TABLE using (USER_ID)
                    left outer join DEV.SBOX_SHILTON.CARDUP_B2B_SG_CONTACT_OWNERS_OUTBOUNDINBOUND_CATEGORIZATION using (RECORD_ID)
            )
    )
select distinct
    *,
    case
        when DATEDIFF(minute, HS_CREATE_DATE, SIGN_UP_DATE) >= 0 then DATEDIFF(minute, HS_CREATE_DATE, SIGN_UP_DATE) / 1440
        else null
    end DAYS_LEAD_TO_SIGN_UP,
    case
        when DATEDIFF(minute, SIGN_UP_DATE, MAKE_ONBOARDED_DATE) >= 0 then DATEDIFF(minute, SIGN_UP_DATE, MAKE_ONBOARDED_DATE) / 1440
        else null
    end DAYS_SIGN_UP_TO_ONBOARDED_MAKE,
    case
        when DATEDIFF(minute, MAKE_ONBOARDED_DATE, FIRST_PAYMENT_DATE_MAKE) >= 0 then DATEDIFF(minute, MAKE_ONBOARDED_DATE, FIRST_PAYMENT_DATE_MAKE) / 1440
        else null
    end DAYS_ONBOARDED_MAKE_TO_FIRST_PAYMENT_MAKE,
    case
        when DATEDIFF(minute, IIC_DATE, CSS_DATE) >= 0 then DATEDIFF(minute, IIC_DATE, CSS_DATE) / 1440
        else null
    end DAYS_IIC_TO_CSS,
    case
        when DATEDIFF(minute, CSS_DATE, COLLECT_ONBOARDED_DATE) >= 0 then DATEDIFF(minute, CSS_DATE, COLLECT_ONBOARDED_DATE) / 1440
        else null
    end DAYS_CSS_TO_ONBOARDED_COLLECT,
    case
        when DATEDIFF(minute, COLLECT_ONBOARDED_DATE, FIRST_PAYMENT_DATE_COLLECT) >= 0 then DATEDIFF(minute, COLLECT_ONBOARDED_DATE, FIRST_PAYMENT_DATE_COLLECT) / 1440
        else null
    end DAYS_ONBOARDED_COLLECT_TO_FIRST_PAYMENT_COLLECT
from
    MAIN;