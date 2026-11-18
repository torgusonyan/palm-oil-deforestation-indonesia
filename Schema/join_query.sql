SELECT
    f.province_of_production_trase_id,
    f.province_of_production,
    f.year,
    f.kabupaten_of_production,
    f.product_type,
    f.exporter_group,
    f.zero_deforestation_indonesia_palm_oil,
    f.country_of_first_import,
    f.palm_oil_deforestation_annual_exposure,
    f.volume,
    f.total_emission_exposure,
    d.annual_palm_deforestation_hectares
FROM fact_palm_oil f
LEFT JOIN dim_deforestation d
    ON f.year = d.year
   AND f.province_of_production_trase_id = d.region_trase_id;