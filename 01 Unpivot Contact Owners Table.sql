create or replace view DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL_UNPIVOT as
with
    MAIN_1 as (
        select
            RECORD_ID,
            COALESCE(CURRENT_CONTACT_OWNER, '') X0,
            PREV_CONTACT_OWNER_1 X1,
            PREV_CONTACT_OWNER_2 X2,
            PREV_CONTACT_OWNER_3 X3,
            PREV_CONTACT_OWNER_4 X4
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL
    ),

    MAIN_2 as (
        select
            RECORD_ID,
            COALESCE(CURRENT_CHANGE_DATE, CAST(0 as TIMESTAMP)) X0,
            PREV_CHANGE_DATE_1 X1,
            PREV_CHANGE_DATE_2 X2,
            PREV_CHANGE_DATE_3 X3,
            PREV_CHANGE_DATE_4 X4
        from
            DEV.SBOX_SHILTON.CONTACT_OWNERS_HISTORICAL
    ),

    TABLE_MAIN as (
        SELECT
            *
        FROM
            MAIN_1 UNPIVOT (CONTACT_OWNER_VALUE FOR CONTACT_OWNER_ORDER IN (X0, X1, X2, X3, X4))
    ),

    TABLE_MAIN_2 as (
        SELECT
            *
        FROM
            MAIN_2 UNPIVOT (CONTACT_OWNER_DATE FOR CONTACT_OWNER_ORDER IN (X0, X1, X2, X3, X4))
    )

select
    RECORD_ID,
    CONTACT_OWNER_VALUE,
    CONTACT_OWNER_DATE,
    ROW_NUMBER() OVER (
        partition by
            RECORD_ID
        order by
            CONTACT_OWNER_DATE ASC
    ) as ORDER_OWNER
from
    (
        select
            T1.*,
            T2.CONTACT_OWNER_DATE
        from
            TABLE_MAIN T1
            join TABLE_MAIN_2 T2 using (RECORD_ID, CONTACT_OWNER_ORDER)
    );