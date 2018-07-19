connection: "analytics_warehouse"

# include all the views
include: "*.view"

datagroup: marketing_analytics_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: marketing_analytics_default_datagroup

explore: account {}

explore: cancelled_order {
  join: item {
    type: left_outer
    sql_on: ${cancelled_order.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }
}

explore: discount_code {}

explore: exchange_currency_rate {}

explore: item {}

explore: ns_mess {}

explore: payment_method {}

explore: product_line {}

explore: refund {
  join: refund {
    type: left_outer
    sql_on: ${refund.refund_id} = ${refund.customer_refund_id} ;;
    relationship: many_to_one
  }
}

explore: refund_line {
  join: refund {
    type: left_outer
    sql_on: ${refund_line.refund_id} = ${refund.customer_refund_id} ;;
    relationship: many_to_one
  }

  join: product_line {
    type: left_outer
    sql_on: ${refund_line.product_line_id} = ${product_line.product_line_id} ;;
    relationship: many_to_one
  }

  join: item {
    type: left_outer
    sql_on: ${refund_line.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }

  join: account {
    type: left_outer
    sql_on: ${refund_line.account_id} = ${account.account_id} ;;
    relationship: many_to_one
  }

  join: payment_method {
    type: left_outer
    sql_on: ${refund_line.payment_method_id} = ${payment_method.payment_method_id} ;;
    relationship: many_to_one
  }

  join: discount_code {
    type: left_outer
    sql_on: ${refund_line.discount_code_id} = ${discount_code.discount_code_id} ;;
    relationship: many_to_one
  }
}

explore: retroactive_discount {
  join: item {
    type: left_outer
    sql_on: ${retroactive_discount.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }

  join: discount_code {
    type: left_outer
    sql_on: ${retroactive_discount.discount_code_id} = ${discount_code.discount_code_id} ;;
    relationship: many_to_one
  }
}

explore: return_order {}

explore: return_order_line {
  join: return_order {
    type: left_outer
    sql_on: ${return_order_line.return_order_id} = ${return_order.return_order_id} ;;
    relationship: many_to_one
  }

  join: item {
    type: left_outer
    sql_on: ${return_order_line.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }

  join: product_line {
    type: left_outer
    sql_on: ${return_order_line.product_line_id} = ${product_line.product_line_id} ;;
    relationship: many_to_one
  }
}

explore: sales_order {}

explore: sales_order_line {
  join: item {
    type: left_outer
    sql_on: ${sales_order_line.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }
}

explore: warranty_order {}

explore: warranty_order_line {
  join: warranty_order {
    type: left_outer
    sql_on: ${warranty_order_line.warranty_order_id} = ${warranty_order.warranty_order_id} ;;
    relationship: many_to_one
  }

  join: item {
    type: left_outer
    sql_on: ${warranty_order_line.item_id} = ${item.item_id} ;;
    relationship: many_to_one
  }
}
