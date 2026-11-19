-- table schema

-- 1. Dimensi deforestasi (unik per wilayah + tahun)
CREATE TABLE dim_deforestation (
    region_trase_id VARCHAR(100),
    year INT,
    region VARCHAR(100),
    annual_palm_deforestation_hectares FLOAT,
    PRIMARY KEY (region_trase_id, year)
);

-- 2. Dimensi provinsi/kabupaten
CREATE TABLE dim_province (
    province_key SERIAL PRIMARY KEY,
    province_of_production_trase_id VARCHAR(100),
    province_of_production VARCHAR(100),
    kabupaten_of_production VARCHAR(100),
    UNIQUE (province_of_production_trase_id, kabupaten_of_production)
);

-- 3. Dimensi produk
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_type VARCHAR(100),
    exporter_group VARCHAR(100),
    zero_deforestation_indonesia_palm_oil VARCHAR(100),
    country_of_first_import VARCHAR(100),
    UNIQUE (product_type, exporter_group, zero_deforestation_indonesia_palm_oil, country_of_first_import)
);

-- 4. Tabel fakta
CREATE TABLE fact_palm_oil (
    fact_id SERIAL PRIMARY KEY,
    province_key INT REFERENCES dim_province(province_key),
    product_key INT REFERENCES dim_product(product_key),
    region_trase_id VARCHAR(100),
    year INT,
    palm_oil_deforestation_annual_exposure FLOAT,
    volume FLOAT,
    total_emission_exposure FLOAT,
    FOREIGN KEY (region_trase_id, year)
        REFERENCES dim_deforestation(region_trase_id, year)
);

select * from dim_deforestation;
select * from fact_palm_oil;
