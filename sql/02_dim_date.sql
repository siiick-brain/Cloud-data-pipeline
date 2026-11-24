-- dim_date pre-generated for 2020-01-01 to 2030-12-31, key is YYYYMMDD
INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d::date,
    extract(year FROM d)::smallint,
    extract(quarter FROM d)::smallint,
    extract(month FROM d)::smallint,
    trim(to_char(d, 'Month')),
    extract(day FROM d)::smallint,
    extract(isodow FROM d)::smallint,
    trim(to_char(d, 'Day')),
    extract(isodow FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') AS d;
