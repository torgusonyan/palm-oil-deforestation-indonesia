# **palm-oil-deforestation-indonesia**
### **Main Dataset Resource**:
https://trase.earth/open-data/datasets/supply-chains-indonesia-palm-oil
### **Supporting Dataset Resource**:
https://trase.earth/open-data/datasets/spatial-metrics-indonesia-palm-oil-palm-oil-deforestation-annual
### Team :
**Data Engineer & Airflow Pipeline**: Ryan Farid Pratama <br>
**Database & Data Modeling**: Muhammad Hanif Al Abid <br>
**Data Analyst**: Narda Parama Agung Noviantoro
## **Background**
Oil palm expansion has been an important driver of deforestation in Indonesia for the past 20 years, accounting for one third (3 million hectares) of Indonesia's loss of old-growth forest (Trase, 2024).
In recent years, deforestation for industrial palm oil has been concentrated in the forest-rich provinces of Kalimantan. Four Kalimantan provinces accounted for 72% of all palm oil deforestation in Indonesia in 2018 to 2022 (Trase / SEI).
Many exporters have zero-deforestation commitments, but the results are mixed: in 2021 to 2022, exporters with and without commitments sourced palm oil from areas with statistically similar deforestation rates (Trase / SEI).

## Stakeholder
The stakeholder in this case are the sustainability/procurement team of a companny planning to buy palm oil from Kalimantan.

## **SDG**
| SDG | Focus | Connection with Indonesian Palm oil |
|---|---|---|
| **SDG 15** | Life on Land | Deforestation and Habitat Loss, Land Degradation and Peatlands, and Sustainable Solution |

## **Data Engineering (Pipeline & Infrastructure)**
The Data Engineering workflow is built to process raw Kalimantan palm oil supply chain and annual deforestation datasets into an analytics-ready state through the following stages:

### 1. Pipeline Workflow (ELTL & Validation)
![Pipeline](https://github.com/torgusonyan/palm-oil-deforestation-indonesia/blob/main/images/pipeline.png?raw=true)

The pipeline follows an **ELTL (Extract → Clean & Validate → Load → Transform in Warehouse)** architecture:
- **Extraction (`extract.py`)**: Ingests raw palm oil supply chain (`palm_oil_data_raw.csv`) and annual provincial deforestation (`palm_oil_deforestation_annual_province_raw.csv`) datasets using **PySpark**, filters records specifically for the **Kalimantan** region, standardizes column names to `snake_case`, and writes intermediate **Parquet** files to `temp_data/raw/`.
- **Cleaning & Data Validation (`clean_validate.py`)**:
  - **Data Cleaning**: Selects relevant columns, enforces consistent data types, handles `NULL` values, and removes duplicate records—resulting in **588,476** palm oil rows and **125** deforestation rows.
  - **Data Validation**: Executes automated data quality checks using **Great Expectations** before writing validated outputs to `temp_data/clean/`.
- **Loading (`load.py`)**: Loads cleaned Parquet datasets into **PostgreSQL** staging tables (`stg_palm_oil` and `stg_palm_deforestation`) via **PySpark JDBC** using `overwrite` mode for idempotency, followed by an automated row-count verification check using `psycopg2`.
- **Transformation (`Schema.sql` & `join_query.sql`)**: Transforms staging data into a **Star Schema** inside PostgreSQL and creates a unified analytical view (`palm_oil`) for downstream exploration and visualization.

### 2. Airflow Orchestration
- **DAG Structure**: Uses a single DAG (`elt_DAG.py`, DAG ID: `palm_oil_deforestation_etl`) consisting of **3 sequential tasks**: `extract` → `clean_validate` → `load`.
- **Execution**: Orchestrated via `BashOperator`, running each stage as an independent Python subprocess inside `/workspace/dags/`. Configured with `max_active_runs=1` to prevent concurrent Spark sessions from exceeding container memory limits.
- **Schedule**: Scheduled to run automatically every day (`@daily` / `0 0 * * *` UTC, or **07:00 WIB**) with `catchup=False`.
- **Retry Policy**: Configured with **1 automatic retry** and a **1-minute retry delay** upon task failure.

### 3. Environment & Infrastructure
- **Docker Setup**: Built with **Docker Compose** (`docker-compose.yml` & `Dockerfile`) to run Apache Airflow, Apache Spark (**PySpark**), and **PostgreSQL** in an integrated containerized environment, pre-installed with `pyspark`, `great_expectations`, `pandas`, `psycopg2`, and the PostgreSQL JDBC Driver.
- **Storage Volume & Repository Management**: The `workspace/` directory (containing `dags/`, `gx/`, `notebooks/`, and `Schema/`) is mounted as a local volume so intermediate Parquet files (`temp_data/`) persist across container restarts, while large raw CSV files and `.env` credentials are excluded via `.gitignore`.

---

### Database & Data Modeling
This stage structures the staged Kalimantan palm oil supply chain and deforestation data into a relational **OLAP** model. Quantitative metrics—such as trade volume, annual deforestation exposure, and total emission exposure—are separated from regional, product, and deforestation attributes into fact and dimension tables to support efficient analytical querying.

#### Database Architecture
![Star Schema](https://github.com/torgusonyan/palm-oil-deforestation-indonesia/blob/d1de7a5f53bb64154021a5c297efc8fadd43a52b/Schema/Schema.png?raw=true)

The database is modeled using a **Star Schema** comprising:
- **Fact Table (`fact_palm_oil`)**: Stores core quantitative metrics (`palm_oil_deforestation_annual_exposure`, `volume`, `total_emission_exposure`), temporal and spatial identifiers (`year`, `region_trase_id`), and foreign keys (`province_key`, `product_key`).
- **Dimension Tables**: Normalize descriptive attributes to eliminate data redundancy:
  - `dim_deforestation`: `region_trase_id`, `year`, `region`, `annual_palm_deforestation_hectares`
  -

### Data Preparation
#### Data Type Formatting & Integrity


### SQL & Analytics
Untuk mendukung analisis dan menjawab berbagai pertanyaan riset (*Key Questions*) tanpa harus menyusun sintaks `JOIN` yang rumit secara berulang, dirancang *SQL Views* terpusat yang menghubungkan *fact table* dengan berbagai tabel dimensi seperti demografi, eksploitasi, dan perekrut.

Contoh implementasi *SQL View* untuk analisis demografi:

```sql
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
```
## **Data Dictionary** (note : ganti dictionary sesuai schema.sql)

#### A. Deforestation

| Column | Data Type | Definisi |
|---|---|---|
| `region_trase_id` | varchar | Primary key yang menjadi identitas unik setiap data region |
| `year` | integer | year |
| `region` | varchar(100)  | region name |
| `annual_palm_deforestation_hectares` | float | big deforestation happen |

---

#### B. Province

| Column | Data Type | Definisi |
|---|---|---|
| `province_key` | serial | Primary key yang menjadi identitas unik setiap data province |
| `province_of_production_trase_id` | varchar(100) | penomoran id |
| `province_of_production` | varchar(100) | name of province production |
| `kabupaten_of_production` | varchar(100) | name of kabupaten production |

---

#### C. Product

| Column | Data Type | Definisi |
|---|---|---|
| `product_key` | serial | Primary key yang menjadi identitas unik setiap data product |
| `product_type` | varchar(100) | name of the product type |
| `exporter_group` | varchar(100) | name of the exporter_group |
| `zero_deforestation_indonesia_palm_oil` | varchar(100) | show zero |
| `country_of_first_import` | varchar(100) | name of the first import country |

---

#### D. Palm Oil

| Column | Data Type | Definisi |
|---|---|---|
| `fact_id` | serial | Primary key yang menjadi identitas unik setiap data hubungan recruiter |
| `province_key` | integer | foreign key that connect fact_palm with dim_province |
| `product_key` | integer | Mforeign key that connect fact_palm with dim_product |
| `region_trase_id` | varchar(100) | id regin trase |
| `year` | integer | year |
| `palm_oil_deforestation_annual_exposure` | float |  |
| `volume` | float |  |
| `total_emission_exposure` | float |  |



## **Problem Statement**
We help the sustainability/procurement team of a palm oil buying company avoid sourcing from high deforestation risk areas, by analysing Trase palm oil supply chain data from 2018 to 2024 joined with industrial palm oil deforestation data per province, to identify the top 5 high-risk kabupaten in Kalimantan and the exporter groups sourcing from them, so that these can be avoided in sourcing decisions within the next 1 year.

## **Objective**
Identify the top 5 high-risk kabupaten in Kalimantan and the exporter groups sourcing from them, so that the sustainability/procurement team of a palm oil buying company can avoid high deforestation risk in its sourcing decisions. The analysis will also show which product types these high-risk exporter groups trade, compare deforestation intensity by commitment status and product type, calculate the emissions exposure linked to the top 5 kabupaten, and measure how much of each province's industrial palm oil deforestation is linked to them, using the supporting province deforestation data.

Results will be shown in a Tableau dashboard with recommendations on which kabupaten and exporters to avoid.

Data Period: 2018 to 2024, includes the full range of the main dataset. The supporting dataset is filtered to the same years.

## **Analysis Question (5w+1H)**
- **WHAT**: What is the deforestation intensity of Kalimantan's palm oil supply chain, how much higher is it in the top 5 kabupaten, and which product type carries higher intensity? How much greenhouse gas emissions exposure is linked to the top 5 high-risk kabupaten?

- **WHERE**: Which kabupaten have the highest deforestation intensity and exposure, and which provinces do they belong to? Which provinces have the largest industrial palm oil deforestation?

- **WHO**: Which exporter groups and importing countries source the most palm oil from the top 5 high-risk kabupaten, and which product types do these exporter groups trade? Which exporter groups have the lowest deforestation risk and can be recommended to buyers instead?

- **WHEN**: When did industrial palm oil deforestation in each Kalimantan province rise or fall between 2018 and 2024, and did supply chain deforestation exposure change in the same years?

- **WHY**: Why are some kabupaten riskier? Do they have a larger share of volume from exporters with None or Unknown commitment status, and are they in provinces with high industrial palm oil deforestation?

- **HOW**: How much of Kalimantan's deforestation exposure would be avoided if buyers stopped sourcing from the top 5 high-risk kabupaten?


## **Key Performance Indicators (KPI)**
**1. Deforestation Intensity**<br>
Total exposure / total volume x 1,000 (ha per 1,000 t), calculated per group.  
Baseline: overall Kalimantan intensity.

**2. Top 5 Share of Province Deforestation (%)**<br>
top 5 exposure / province industrial palm oil deforestation x 100

**3. Top 5 Volune Share (%)**<br>
top 5 volume / total volume x 100
  
**4. Exposure avoided (%)**<br>
top 5 exposure / total exposure x 100.
  
**5. Total Emission Exposure**<br>
sum of emissions for the top 5 kabupaten
  

## **Key Questions & Findings**
**KQ1**. Risiko eksploitasi sangat dipengaruhi gender dan usia, perempuan (khususnya anak 09–17 tahun) dominan mengalami eksploitasi seksual, sementara laki-laki dominan mengalami kerja paksa, dengan pola yang juga bervariasi menurut kewarganegaraan.<br>
**KQ2.** Setiap sektor memiliki "signature" eksploitasi yang khas dan hampir murni satu jenis seperti sektor seks (prostitusi, pornografi) hampir eksklusif eksploitasi seksual, sektor kerja fisik (agriculture, construction) hampir eksklusif kerja paksa, kecuali hospitality yang menunjukkan risiko ganda.<br>
**KQ3.** Relasi personal yang dekat (intimate partner, family) justru berkorelasi dengan rate eksploitasi seksual tertinggi, sedangkan relasi non-personal ("other"/agen) lebih berkorelasi dengan kerja paksa  membalikkan asumsi umum bahwa orang asing adalah ancaman utama.<br>
**KQ4.** Anak perempuan yang direkrut melalui relasi dekat memiliki risiko eksploitasi seksual paling ekstrem, sementara anak laki-laki menunjukkan pola eksploitasi yang lebih campuran antara kerja paksa dan seksual.<br>
**KQ5.** Profil korban dengan risiko paling ekstrem adalah kombinasi anak usia sangat muda, sektor eksploitasi seksual, dan direkrut oleh keluarga sendiri seperti menunjukkan bahwa kombinasi faktor demografis, sektor, dan relasi recruiter jauh lebih prediktif dibanding faktor tunggal manapun.<br>
**KQ6**. Berdasarkan uji statistik Spearman ($\rho = 0,7204$; $p = 0,000106$), jumlah korban dengan hubungan rekrutmen Other menunjukkan tren peningkatan jangka panjang yang signifikan dengan puncak tertinggi pada 2016, sementara kategori Family, Friend, dan Intimate Partner masing-masing mencapai puncaknya pada 2016 atau 2019 sebelum mengalami penurunan hingga tahun 2024.

## **Conclusion**
Proyek ini berhasil mengungkap bahwa risiko TPPO terbentuk dari kombinasi spesifik antara siapa korbannya (usia, gender, kewarganegaraan), di sektor apa mereka dieksploitasi, dan melalui relasi apa mereka direkrut dengan relasi kedekatan personal sebagai titik kerentanan yang paling kritis namun paling sulit dideteksi. Temuan ini memberikan dasar berbasis data bagi pemangku kepentingan untuk merancang intervensi yang lebih tepat sasaran tidak hanya berfokus pada pengawasan jalur migrasi kerja formal dan sindikat eksternal, tetapi juga pada edukasi dan deteksi dini terhadap pola manipulasi hubungan dekat di lingkungan terdekat korban.

Correlation between deforestation and palm oil industries
