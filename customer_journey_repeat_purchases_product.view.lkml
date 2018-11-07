view: customer_journey_repeat_purchases_product {
  derived_table: {
    sql: with x as (
        --Customer Journey
      select s.user_id, a.name as product, s.session_id, a.created_at as purchase_time, a.total_spent
            , row_number() over(partition by s.user_id order by time) as session_number
            , case when a.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
      from analytics.HEAP.sessions s

      left join (select p.user_id, p.session_id, c.name, b.created_at, sum(p.dollars) total_spent
                 from analytics.heap.purchase p
                 left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER" b
                 on split_part(p.path,'/',-2) = b.checkout_token
                 left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER_LINE" c
                 on b.id = c.order_id
                 left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."PRODUCT_VARIANT" d
                 on c.variant_id = d.id
                 where checkout_token is not null
                 group by p.user_id, session_id, b.checkout_token, split_part(p.path,'/',-2), p.dollars, c.name, b.created_at
                 order by user_id) a
      on s.user_id = a.user_id
      and s.session_id = a.session_id

      where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
      and (a.total_spent > 0 or a.total_spent is null)
      and time between '2018-01-01' and CURRENT_DATE)

      , xaa as (
          -- count purchases
      select case when purchase_flag = 'PURCHASE'
        then row_number() over (partition by user_id, purchase_flag order by purchase_time) end purchase_session_cnt
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

          -- Repeat purchase by Product
      select count(case when num_purchases > 1 then product end) repeat_purchase
        , user_id
        , product
      from xbb
      where num_purchases > 1
      group by product, user_id
      order by 2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: repeat_purchase {
    type: sum
    sql: ${TABLE}."REPEAT_PURCHASE" ;;
  }

measure: user_id {
    type: number
    sql: ${TABLE}."USER_ID" ;;
  }

  dimension: product {
    type: string
    sql: ${TABLE}."PRODUCT" ;;
  }

  set: detail {
    fields: [repeat_purchase, user_id, product]
  }
}
