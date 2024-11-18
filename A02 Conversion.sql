--funnel split
select date_trunc(month, date(hs_create_date))                 month_lead_created
     , to_char(date_trunc(year, date(hs_create_date)), 'yyyy') year_lead_created
     , case
           when contact_owner like '%Keegan%' then 'Keegan'
           when contact_owner like '%Leia%' then 'Leia'
           when contact_owner like '%Jonathan%' then 'Jonathan'
           when contact_owner like '%Terence%' then 'Terence'
           when contact_owner like '%Xavier%' then 'Xavier'
           else 'Unmanaged' end as                             contact_owner
     , case
           when contact_owner like '%Keegan%' or contact_owner_historical like '%Keegan%' then 'Keegan'
           else null end        as                             contact_owner_historical_keegan
     , case
           when contact_owner like '%Leia%' or contact_owner_historical like '%Leia%' then 'Leia'
           else null end        as                             contact_owner_historical_leia
     , case
           when contact_owner like '%Jonathan%' or contact_owner_historical like '%Jonathan%' then 'Jonathan'
           else null end        as                             contact_owner_historical_jonathan
    /*, MLA_SOURCE */
    /*, cardup_payment_payment_type_make */
     , count(distinct record_id)
     , count(hs_create_date)
     , count(sign_up_date)
     , count(make_onboarded_date)
     , count(first_payment_date_make)
     , sum(first_30d_payment_amount_make)
from dev.sbox_shilton.cardup_b2b_sg_funnel_with_historical_tagging
where true
  and iic_date is null
  and date_trunc(month, date(hs_create_date)) >= date('2023-08-01')
  and date_trunc(month, date(hs_create_date)) < date('2024-08-01')
  and not (contact_owner_historical_keegan is null and contact_owner_historical_leia is null and
           contact_owner_historical_jonathan is null)
group by 1, 2, 3, 4, 5, 6 /*, 7 */;