create view palm_oil AS
SELECT f.*, p.province_of_production, p.kabupaten_of_production,
       pr.product_type, pr.exporter_group,
       pr.zero_deforestation_indonesia_palm_oil, pr.country_of_first_import,
       d.annual_palm_deforestation_hectares
FROM fact_palm_oil f
JOIN dim_province p ON f.province_key = p.province_key
JOIN dim_product pr ON f.product_key = pr.product_key
LEFT JOIN dim_deforestation d
    ON f.region_trase_id = d.region_trase_id AND f.year = d.year;