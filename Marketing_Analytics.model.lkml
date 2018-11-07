connection: "analytics_warehouse"

# include all the views
include: "*.view"

datagroup: marketing_analytics_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

explore: customer_journey {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Customer Journey"
  description: "Customery Journey"
}
explore: ROI {
  hidden: yes
  group_label: "Marketing analytics"
  label: "ROI"
  description: "Daily adspend details, including channel, clicks, impressions, spend, device, platform, etc."
}
explore: customer_journey_referrer {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Referral"
  description: "Breakdown of the customer journey by Referring URL"
}
explore: customer_journey_landing_page {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Landing Page"
  description: "Breakdown of the customer journey by Landing Page"
}
explore: customer_journey_path {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Path Analysis"
  description: "What are the most common customer journeys to a purchase?"
}
explore: customer_journey_repeat_purchasers_LP {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Repeat Purchase Analysis LP"
  description: "Repeat Purchases by landing page"
}
explore: customer_journey_repeat_purchasers_ref {
  hidden: yes
  group_label: "Marketing analytics"
  label: "Repeat Purchase Analysis Ref"
  description: "Repeat Purchases by referrer"
}
