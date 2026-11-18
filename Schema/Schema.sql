-- table schema

-- 1. Tabel dimensi : deforestation
create table dim_deforestation (
	region_trase_id serial primary key,
	year int,
	region varchar(100),
	annual_palm_deforestation_hectares float
);

-- 2. Fact Table: Tabel utama yang menghubungkan semua dimensi (Foreign Keys)
CREATE TABLE fact_palm_oil (
    province_of_production_trase_id SERIAL PRIMARY KEY,
    region_trase_id INT REFERENCES dim_deforestation(region_trase_id),
    province_of_production VARCHAR(100),
    year INT,
    kabupaten_of_production VARCHAR(100),
    product_type VARCHAR(100),
    exporter_group VARCHAR(100),
    zero_deforestation_indonesia_palm_oil VARCHAR(100),
    country_of_first_import VARCHAR(100),
    palm_oil_deforestation_annual_exposure FLOAT,
    volume FLOAT,
    total_emission_exposure FLOAT
);

select * from dim_deforestation;
select * from fact_palm_oil;

drop table dim_deforestation;
drop table fact_palm_oil;