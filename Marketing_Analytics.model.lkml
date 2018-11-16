connection: "analytics_warehouse"

# include all the views
include: "*.view"

datagroup: marketing_analytics_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

explore: customer_journey {
  group_label: "Marketing analytics"
  label: "Customer Journey"
  description: "Customery Journey"
}
explore: ROI {
  group_label: "Marketing analytics"
  label: "ROI"
  description: "Daily adspend details, including channel, clicks, impressions, spend, device, platform, etc."
}
explore: customer_journey_referrer {
  group_label: "Marketing analytics"
  label: "Referral"
  description: "Breakdown of the customer journey by Referring URL"
}
explore: customer_journey_landing_page {
  group_label: "Marketing analytics"
  label: "Landing Page"
  description: "Breakdown of the customer journey by Landing Page"
}
explore: customer_journey_path {
  group_label: "Marketing analytics"
  label: "Path Analysis"
  description: "What are the most common customer journeys to a purchase?"
}
explore: customer_journey_repeat_purchasers_LP {
  group_label: "Marketing analytics"
  label: "Repeat Purchase Analysis LP"
  description: "Repeat Purchases by landing page"
}
explore: customer_journey_repeat_purchasers_ref {
  group_label: "Marketing analytics"
  label: "Repeat Purchase Analysis Ref"
  description: "Repeat Purchases by Referrer"
}
explore: page_views_analysis {
  group_label: "Marketing analytics"
  label: "Page Views Analysis"
  description: "How many times were pages viewed?"
}
explore: customer_journey_repeat_purchases_product {
  group_label: "Marketing analytics"
  label: "Repeat Purchase Analysis Product"
  description: "Repeat Purchases by Product"
}
explore: 30_day_attributions {
  group_label: "Marketing analytics"
  label: "Attribution"
  description: "Total 30 day touch per campaign"
}
