create or replace view DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT_INBOUND_OUTBOUND as
with
    ASHLEY_TABLE as (
        select
            *
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT
        where
            CONTACT_OWNER_VALUE like '%Ashley%'
    ),
    SALES_TABLE as (
        select
            *
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT
        where
            (
                CONTACT_OWNER_VALUE like '%Jonathan%'
                or CONTACT_OWNER_VALUE like '%Keegan%'
                or CONTACT_OWNER_VALUE like '%Leia%'
            )
    ),
    OTHERS_TABLE as (
        select
            *
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT
        where
            not (
                CONTACT_OWNER_VALUE like '%Jonathan%'
                or CONTACT_OWNER_VALUE like '%Keegan%'
                or CONTACT_OWNER_VALUE like '%Leia%'
            )
            and not CONTACT_OWNER_VALUE like '%Ashley%'
    ),
    MAIN as (
        select
            T0.RECORD_ID,
            T3.CONTACT_OWNER_VALUE OWNER_ASHLEY,
            T3.ORDER_OWNER ORDER_OWNER_ASHLEY,
            T2.CONTACT_OWNER_VALUE OWNER_SALES,
            T2.ORDER_OWNER ORDER_OWNER_SALES,
            T1.CONTACT_OWNER_VALUE OWNER_WHOEVER_ELSE,
            T1.ORDER_OWNER ORDER_OWNER_WHOEVER_ELSE,
            case
                when T3.ORDER_OWNER < T2.ORDER_OWNER then 'Inbound'
                when T2.ORDER_OWNER < T3.ORDER_OWNER
                or (
                    T2.ORDER_OWNER is not null
                    and T3.ORDER_OWNER is null
                ) then 'Outbound'
                when T3.ORDER_OWNER is not null then 'Self-serve'
                else 'Legacy'
            end as OUTBOUND_INBOUND_LEAD
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT T0
            left outer join OTHERS_TABLE T1 using (RECORD_ID)
            left outer join SALES_TABLE T2 using (RECORD_ID)
            left outer join ASHLEY_TABLE T3 using (RECORD_ID)
    )
select distinct
    RECORD_ID,
    OUTBOUND_INBOUND_LEAD
from
    MAIN;