view: customer_journey_referrer {
  derived_table: {
    sql: with x as (
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

, xcs as(
select row_number() over (partition by user_id order by time) session_cnt
    , case when purchase_flag = 'PURCHASE' then row_number() over (partition by user_id, purchase_flag order by time) end purchase_session_cnt
    , x.*
from x),
xaa as(
select xcs.*, case when purchase_flag = 'PURCHASE' and purchase_session_cnt = 1 then time else NULL end as purchase_time from xcs),
xbb as(
select xaa.*, b.first_purchase, c.num_purchases, d.pageviews from xaa
left join (select distinct user_id, min(purchase_time) first_purchase from xaa
          group by user_id) b
on xaa.user_id = b.user_id
left join (select user_id, count(distinct purchase_session_cnt) num_purchases from xaa
          group by user_id) c
on xaa.user_id = c.user_id
left join (select session_id,user_id,count(time) as pageviews from analytics.heap.pageviews group by session_id, user_id) d
on xaa.user_id = d.user_id and xaa.session_id = d.session_id
),
xff as(
select count(distinct session_cnt) num_sessions, avg(pageviews) avg_views_sess,user_id, landing_page, referrer, product
from xbb
where time <= first_purchase
group by landing_page, referrer, user_id, product)

, xee as (select avg(num_sessions) avg_num_sessions,avg(avg_views_sess) avg_views_per_session, a.total_sessions,  xff.referrer, xff.product
          from xff
left join (select count(session_id) total_sessions, referrer from x group by referrer) a
on xff.referrer = a.referrer
where xff.referrer not like '%purple.com%'
group by xff.referrer, a.total_sessions, xff.product
order by total_sessions desc)

select avg(avg_num_sessions) avg_num_sessions, avg(avg_views_per_session) avg_views_per_session, sum(total_sessions) total_sessions
  , xee.product
, case when lower(referrer) like '%purple.com%' then 'PURPLE'
         when lower(referrer) like '%goog%' then 'GOOGLE'
         when lower(referrer) like '%fb%' then 'FACEBOOK'
         when lower(referrer) like '%faceb%' then 'FACEBOOK'
         when lower(referrer) like '%yaho%' then 'YAHOO'
         when lower(referrer) like '%bing%' then 'BING'
         when lower(referrer) like '%instag%' then 'INSTAGRAM'
         when lower(referrer) like '%youtu%' then 'YOUTUBE'
         when lower(referrer) like '%aol%' then 'AOL'
         when lower(referrer) like '%sleepop%' then 'SLEEPOPOLIS'
         when lower(referrer) like '%pintere%' then 'PINTEREST'
         when lower(referrer) like '%huff%' then 'HUFFINGTON POST'
         when lower(referrer) like '%mattressf%' then 'MATTRESS FIRM'
         when lower(referrer) like '%outbrain%' then 'OUTBRAIN'
         when referrer is null then null
         else 'OTHER' end initial_referrer
from xee
group by  case when lower(referrer) like '%purple.com%' then 'PURPLE'
         when lower(referrer) like '%goog%' then 'GOOGLE'
         when lower(referrer) like '%fb%' then 'FACEBOOK'
         when lower(referrer) like '%faceb%' then 'FACEBOOK'
         when lower(referrer) like '%yaho%' then 'YAHOO'
         when lower(referrer) like '%bing%' then 'BING'
         when lower(referrer) like '%instag%' then 'INSTAGRAM'
         when lower(referrer) like '%youtu%' then 'YOUTUBE'
         when lower(referrer) like '%aol%' then 'AOL'
         when lower(referrer) like '%sleepop%' then 'SLEEPOPOLIS'
         when lower(referrer) like '%pintere%' then 'PINTEREST'
         when lower(referrer) like '%huff%' then 'HUFFINGTON POST'
         when lower(referrer) like '%mattressf%' then 'MATTRESS FIRM'
         when lower(referrer) like '%outbrain%' then 'OUTBRAIN'
         when referrer is null then null
         else 'OTHER' end
  , xee.product;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: avg_num_sessions {
    type: average
    sql: ${TABLE}."AVG_NUM_SESSIONS" ;;
  }

  measure: avg_views_per_session {
    type: sum
    sql: ${TABLE}."AVG_VIEWS_PER_SESSION" ;;
  }

  measure: total_sessions {
    type: sum
    sql: ${TABLE}."TOTAL_SESSIONS" ;;
  }

  dimension: initial_referrer {
    type: string
    sql: ${TABLE}."INITIAL_REFERRER" ;;
  }

  dimension: product {
    type: string
    sql: ${TABLE}."PRODUCT" ;;
  }

  set: detail {
    fields: [avg_num_sessions, avg_views_per_session, total_sessions, initial_referrer, product]
  }
}
