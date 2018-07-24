- dashboard: marketing_analysis
  title: Marketing Analysis
  layout: tile
  tile_size: 100

  filters:

  elements:
    - name: hello_world
      type: looker_column
    - name: add_a_unique_name_1532453134
  title: Cost Per Acquisition - 30 Day
  model: Marketing_Analytics
  explore: ROI
  type: looker_bar
  fields: [ROI.source, ROI.spend, ROI.orders]
  filters:
    ROI.thirty_day: '1'
    ROI.spend: ">1000"
  sorts: [cost_per_acquisition]
  limit: 500
  dynamic_fields:
  - table_calculation: cost_per_acquisition
    label: Cost Per Acquisition
    expression: "${ROI.spend}/${ROI.orders}"
    value_format:
    value_format_name: usd_0
    _kind_hint: measure
    _type_hint: number
  query_timezone: America/Denver
  stacking: ''
  show_value_labels: true
  label_density: 25
  legend_position: center
  x_axis_gridlines: false
  y_axis_gridlines: true
  show_view_names: false
  point_style: none
  limit_displayed_rows: false
  y_axis_combined: true
  show_y_axis_labels: true
  show_y_axis_ticks: true
  y_axis_tick_density: default
  y_axis_tick_density_custom: 5
  show_x_axis_label: true
  show_x_axis_ticks: true
  x_axis_scale: auto
  y_axis_scale_mode: linear
  x_axis_reversed: false
  y_axis_reversed: false
  ordering: none
  show_null_labels: false
  show_totals_labels: false
  show_silhouette: false
  totals_color: "#808080"
  show_null_points: true
  series_types: {}
  hidden_fields: [ROI.orders, ROI.spend]
  colors: ['palette: Default']
  series_colors: {}
