--- isi dim_deforestation
INSERT INTO dim_deforestation (region_trase_id, year, region, annual_palm_deforestation_hectares)
SELECT DISTINCT
    region_trase_id,
    year,
    region,
    annual_palm_deforestation_hectares
FROM stg_palm_deforestation
ON CONFLICT (region_trase_id, year) DO NOTHING;

--- isi dim_province
INSERT INTO dim_province (province_of_production_trase_id, province_of_production, kabupaten_of_production)
SELECT DISTINCT
    province_of_production_trase_id,
    province_of_production,
    kabupaten_of_production
FROM stg_palm_oil
ON CONFLICT (province_of_production_trase_id, kabupaten_of_production) DO NOTHING;

--- isi dim_product
INSERT INTO dim_product (product_type, exporter_group, zero_deforestation_indonesia_palm_oil, country_of_first_import)
SELECT DISTINCT
    product_type,
    exporter_group,
    zero_deforestation_indonesia_palm_oil,
    country_of_first_import
FROM stg_palm_oil
ON CONFLICT (product_type, exporter_group, zero_deforestation_indonesia_palm_oil, country_of_first_import) DO NOTHING;

-- cek baris yang tidak punya pasangan di deforestasi
SELECT DISTINCT s.province_of_production_trase_id, s.year
FROM stg_palm_oil s
LEFT JOIN dim_deforestation d
    ON s.province_of_production_trase_id = d.region_trase_id
   AND s.year = d.year
WHERE d.region_trase_id IS NULL;
--Kalau hasilnya kosong, lanjut ke langkah 5. Kalau ada baris, pilih salah satu: lengkapi data deforestasinya, atau hapus constraint foreign key komposit di tabel fakta (lalu tetap pakai LEFT JOIN saat query, sama seperti how='left' di pandas).

--isi fact_palm_oil
INSERT INTO fact_palm_oil (
    province_key,
    product_key,
    region_trase_id,
    year,
    palm_oil_deforestation_annual_exposure,
    volume,
    total_emission_exposure
)
SELECT
    p.province_key,
    pr.product_key,
    s.province_of_production_trase_id,
    s.year,
    s.palm_oil_deforestation_annual_exposure,
    s.volume,
    s.total_emission_exposure
FROM stg_palm_oil s
JOIN dim_province p
    ON  s.province_of_production_trase_id = p.province_of_production_trase_id
    AND s.kabupaten_of_production IS NOT DISTINCT FROM p.kabupaten_of_production
JOIN dim_product pr
    ON  s.product_type IS NOT DISTINCT FROM pr.product_type
    AND s.exporter_group IS NOT DISTINCT FROM pr.exporter_group
    AND s.zero_deforestation_indonesia_palm_oil IS NOT DISTINCT FROM pr.zero_deforestation_indonesia_palm_oil
    AND s.country_of_first_import IS NOT DISTINCT FROM pr.country_of_first_import;

-- verifikasi
-- jumlah baris fakta harus sama dengan jumlah baris staging:
SELECT
    (SELECT COUNT(*) FROM stg_palm_oil) AS baris_staging,
    (SELECT COUNT(*) FROM fact_palm_oil) AS baris_fakta;

SELECT region_trase_id, year, COUNT(*)
FROM stg_palm_deforestation
GROUP BY region_trase_id, year
HAVING COUNT(*) > 1;