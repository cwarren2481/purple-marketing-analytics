view: customer_journey_repeat_purchasers_LP {
  derived_table: {
    sql: with x as (
        --Customer Journey
        select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
            , row_number() over(partition by s.user_id order by time) as session_number
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
, xaa as (
    -- count purchases
select case when purchase_flag = 'PURCHASE' then row_number() over (partition by user_id, purchase_flag order by time) end purchase_session_cnt
    , x.*
from x
where purchase_flag = 'PURCHASE')

, xbb as (
    -- number of purchases
select xaa.*
  , z.num_purchases
from xaa
left join (select user_id, count(distinct purchase_session_cnt) num_purchases from xaa group by user_id) z
on xaa.user_id = z.user_id)

, xcc as (
    -- count repeat_purchase by landing_page
select user_id
  , count(case when num_purchases > 1 then landing_page end) repeat_purchase
  , landing_page
  , referrer
  , dollars
  , product
from xbb
where num_purchases > 1
group by landing_page, referrer, user_id, dollars, product)

    --avg repeat_purchase with total_purchases by landing_page
select avg(repeat_purchase) avg_repeat_purchase, a.total_purchases, xcc.landing_page, round(avg(dollars)) as avg_dollars, product
from xcc
left join (select count(purchase_flag) total_purchases , landing_page from x group by landing_page, purchase_flag
           having purchase_flag = 'PURCHASE') a
on a.landing_page = xcc.landing_page
left join (select landing_page, user_id from x where session_number = 1) as b
on xcc.user_id = b.user_id
group by xcc.landing_page, a.total_purchases, product
having avg_repeat_purchase > 0
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: avg_repeat_purchase {
    type: sum
    sql: ${TABLE}."AVG_REPEAT_PURCHASE" ;;
  }

  measure: total_purchases {
    type: number
    sql: ${TABLE}."TOTAL_PURCHASES" ;;
  }

  measure: avg_dollars {
    type: average
    value_format:"$#.00;($#.00)"
    sql: ${TABLE}."AVG_DOLLARS" ;;
  }

  dimension: landing_page {
    type: string
    sql: ${TABLE}."LANDING_PAGE" ;;
  }

  dimension: product {
    type: string
    sql: ${TABLE}."PRODUCT" ;;
  }

  set: detail {
    fields: [avg_repeat_purchase, total_purchases, avg_dollars, landing_page, product]
  }
}
