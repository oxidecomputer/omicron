SELECT
  assumeNotNull(anyIf(fields_i32, field_name = 'foo')) AS foo,
  assumeNotNull(anyIf(fields_u32, field_name = 'index')) AS INDEX,
  assumeNotNull(anyIf(fields_string, field_name = 'name')) AS name,
  timeseries_key
FROM
  (
    SELECT
      timeseries_key,
      field_name,
      field_value AS fields_string,
      NULL AS fields_i32,
      NULL AS fields_u32
    FROM
      oximeter.fields_string
    WHERE
      timeseries_name = 'some_target:some_metric'
    UNION
    ALL
    SELECT
      timeseries_key,
      field_name,
      NULL AS fields_string,
      field_value AS fields_i32,
      NULL AS fields_u32
    FROM
      oximeter.fields_i32
    WHERE
      timeseries_name = 'some_target:some_metric'
    UNION
    ALL
    SELECT
      timeseries_key,
      field_name,
      NULL AS fields_string,
      NULL AS fields_i32,
      field_value AS fields_u32
    FROM
      oximeter.fields_u32
    WHERE
      timeseries_name = 'some_target:some_metric'
  )
GROUP BY
  timeseries_key