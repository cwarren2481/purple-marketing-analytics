view: customer_journey_path {
  derived_table: {
    sql:
with x as (
        --Customer Journey
        select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
            , case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
            , p.dollars
            , p.product
            , p.token
        from analytics.HEAP.sessions s
        left join (select a.user_id, session_id, b.checkout_token,split_part(a.path,'/',-2) as token
        , listagg(c.name,', ') within group (order by c.name) as Product
        , a.dollars
        from analytics.heap.purchase a
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER" b
        on split_part(a.path,'/',-2) = b.checkout_token
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER_LINE" c
        on b.id = c.order_id
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."PRODUCT_VARIANT" d
        on c.variant_id = d.id
        where checkout_token is not null
        group by a.user_id, session_id, b.checkout_token, split_part(path,'/',-2), a.dollars
        order by user_id) p
        on s.user_id = p.user_id
        and s.session_id = p.session_id
        where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
        and (p.dollars > 0 or p.dollars is null))
, zza as(
select x.user_id, x.product
  , case when a.path = '/' then a.title
  when a.path like '%checkout%' then 'Checkout' else a.path end as path
  , a.time, a.landing_page, x.session_id, row_number() over (partition by a.user_id,a.session_id order by a.time) as event_number
from x
left join analytics.heap.pageviews a
  on x.user_id = a.user_id
  and x.session_id = a.session_id
where purchase_flag = 'PURCHASE')
, zzd as (
SELECT user_id, zza.product
                ,session_id
                ,MAX(CASE
                                WHEN event_number = 1
                                        THEN landing_page
                                ELSE NULL
                                END) AS e1
                ,MAX(CASE
                                WHEN event_number = 2
                                        THEN path
                                ELSE NULL
                                END) AS e2
                ,MAX(CASE
                                WHEN event_number = 3
                                        THEN path
                                ELSE NULL
                                END) AS e3
                ,MAX(CASE
                                WHEN event_number = 4
                                        THEN path
                                ELSE NULL
                                END) AS e4
                ,MAX(CASE
                                WHEN event_number = 5
                                        THEN path
                                ELSE NULL
                    END) as e5
                    from zza
         group by user_id, session_id, zza.product)

select count(session_id) as occurences, zzd.product, E1, E2, E3, E4, E5 from zzd
group by E1, E2, E3, E4, E5, zzd.product
order by occurences desc

       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: occurences {
    type: sum
    sql: ${TABLE}."OCCURENCES" ;;
  }

  dimension: e1 {
    type: string
    sql: ${TABLE}."E1" ;;
  }

  dimension: e2 {
    type: string
    sql: ${TABLE}."E2" ;;
  }

  dimension: e3 {
    type: string
    sql: ${TABLE}."E3" ;;
  }

  dimension: e4 {
    type: string
    sql: ${TABLE}."E4" ;;
  }

  dimension: e5 {
    type: string
    sql: ${TABLE}."E5" ;;
  }

  dimension: product {
    type: string
    sql: ${TABLE}."PRODUCT" ;;
  }

  set: detail {
    fields: [
      occurences,
      e1,
      e2,
      e3,
      e4,
      e5,
      product
    ]
  }
}
