# **palm-oil-deforestation-indonesia**
### **Main Dataset Resource**:
https://trase.earth/open-data/datasets/supply-chains-indonesia-palm-oil
### **Supporting Dataset Resource**:
https://trase.earth/open-data/datasets/spatial-metrics-indonesia-palm-oil-palm-oil-deforestation-annual
### Team :
**Data Engineer & Airflow Pipeline**: Ryan Farid Pratama

**Database & Data Modeling**: Muhammad Hanif Al Abid

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
### Data Engineering Pipeline

Alur kerja *data engineering* dirancang untuk memproses data dari sumber hingga siap dianalisis melalui beberapa tahapan utama:

#### 1. Alur Proses Pipeline (ETL & Validation)
![Pipeline](https://github.com/dutarangga3107-max/CODA-020-Final_Project/blob/main/Schema/pipeline%20.png?raw=true)
* **Extraction**: Membaca dan mengumpulkan data mentah dari *website* CTDC tanpa membebani kinerja sistem sumber.
* **Data Validation**: Memastikan data mematuhi logika bisnis (misal: validasi usia dan format data yang sesuai).
* **Transformation**: 
  * Pembersihan data: Menangani nilai kosong (*NULL*) dan menghapus baris duplikat.
  * Pemformatan data: Mengubah data mentah menjadi format yang mudah dianalisis.
* **Loading**: Memindahkan data yang sudah bersih dan valid ke destinasi akhir di basis data Neon.

#### 2. Airflow Orchestration
* **DAG Structure**: Menggunakan 1 DAG (`FP_dag.py`) dengan 3 *task* linear: `extract` → `transform` → `load`.
* **Execution**: Eksekusi dijalankan via `BashOperator`, di mana tiap *task* berjalan sebagai *subprocess* Python independen.
* **Schedule**: Dijadwalkan berjalan setiap hari Sabtu, menit ke-10 hingga 30 (kelipatan 10) pada jam 09:00 pagi.
* **Retry Policy**: Konfigurasi otomatis 1 kali percobaan ulang (*retry*) dengan jeda 5 menit jika terjadi kegagalan pada *task*.

#### 3. Environment & Infrastructure
* **Docker Setup**: Menggunakan Airflow via *Docker Compose* dengan *custom image* `airflow-spark` (basis `apache/airflow:2.3.4` yang dilengkapi Java serta pustaka pendukung seperti *great_expectations*, *pandas*, *sqlalchemy*, *beautifulsoup4*, *pyarrow*, dll).
* **Storage Volume**: Folder direktori `data/`, `dags/`, `scripts/`, dan `logs/` di-mount sebagai *volume* lokal sehingga file *parquet* tetap aman meskipun *container* direstart.
### Database & Data Modeling 
Proses ini mengubah data kasus *trafficking* dan *exploitation* menjadi struktur basis data relasional yang terorganisasi. Informasi terkait korban, eksploitasi, *recruitment*, dan faktor pendukung dipisahkan ke dalam tabel *fact* dan *dimension* untuk memudahkan analisis risiko dan *profiling* korban.
#### Database Architecture 
![Star Schema](https://github.com/torgusonyan/palm-oil-deforestation-indonesia/blob/617afc73be751ac72135764f137c67f7515305bf/Schema/Schema.png?raw=true)
Arsitektur basis data dirancang menggunakan *Star Schema*, yang terdiri dari:
* **Fact Table (`fact_trafficking_cases`)**: Menyimpan metrik utama kasus dan kunci relasi (*Foreign Keys*).
* **Dimension Tables**: Memisahkan atribut detail untuk menghindari redundansi data, yang meliputi:
  * `dim_demographics`: Umur, *gender*, kewarganegaraan, dan negara eksploitasi.
  * `dim_exploitation` & `dim_means`: Sektor, jenis eksploitasi, dan cara penjeratan.
  * `dim_recruiter`: Hubungan perekrut dengan korban.
### Data Preparation
#### Data Type Formatting & Integrity
* **Penyesuaian Tipe Data**: Mengonfigurasi kolom waktu (seperti `yearOfRegistration`) menjadi format teks (`VARCHAR`) untuk menjaga konsistensi karakteristik data.
* **Integritas Relasional**: Menerapkan *Primary Key* dan *Foreign Key* yang ketat antar tabel untuk mencegah inkonsistensi data saat proses penggabungan.

### SQL & Analytics
Untuk mendukung analisis dan menjawab berbagai pertanyaan riset (*Key Questions*) tanpa harus menyusun sintaks `JOIN` yang rumit secara berulang, dirancang *SQL Views* terpusat yang menghubungkan *fact table* dengan berbagai tabel dimensi seperti demografi, eksploitasi, dan perekrut.

Contoh implementasi *SQL View* untuk analisis demografi:

```sql
CREATE VIEW analisis_demografi AS
SELECT
    f.case_id,
    f.yearofregistration,
    f.traffickmonths,
    d.gender,
    d.agebroad,
    d.citizenship,
    d.countryofexploitation
FROM fact_trafficking_cases f
JOIN dim_demographics d ON f.demographic_id = d.demographic_id;
```
## **Data Dictionary**

#### A. DEMOGRAPHICS

| Column | Data Type | Definisi |
|---|---|---|
| `demographic_id` | serial | Primary key yang menjadi identitas unik setiap data demografi korban |
| `gender` | varchar(50) | Gender korban |
| `agebroad` | varchar(50) | Kelompok usia korban |
| `citizenship` | varchar(100) | Kewarganegaraan korban |
| `countryofexploitation` | varchar(100) | Negara tempat korban mengalami eksploitasi |

---

#### B. EXPLOITATION

| Column | Data Type | Definisi |
|---|---|---|
| `exploitation_id` | serial | Primary key yang menjadi identitas unik setiap data eksploitasi |
| `isforcedlabour` | boolean | Menunjukkan apakah korban mengalami forced labour |
| `issexualexploit` | boolean | Menunjukkan apakah korban mengalami eksploitasi seksual |
| `isotherexploit` | boolean | Menunjukkan apakah korban mengalami bentuk eksploitasi lainnya |
| `sectoroflabouragriculture` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan sektor pertanian |
| `sectoroflabourconstruction` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan sektor konstruksi |
| `sectoroflabourdomesticwork` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan pekerjaan domestik |
| `sectoroflabourhospitality` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan sektor hospitality |
| `sectorofsexprostitution` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan sektor prostitusi |
| `sectorofsexpornography` | boolean | Menunjukkan apakah eksploitasi berkaitan dengan sektor pornografi |

---

#### C. MEANS

| Column | Data Type | Definisi |
|---|---|---|
| `means_id` | serial | Primary key yang menjadi identitas unik setiap data metode eksploitasi |
| `meansdebtbondageearnings` | boolean | Menunjukkan penggunaan debt bondage atau keterikatan melalui utang/penghasilan dalam eksploitasi |
| `meansthreats` | boolean | Menunjukkan penggunaan ancaman terhadap korban |
| `meansabusepsyphysex` | boolean | Menunjukkan adanya penyalahgunaan atau kekerasan psikologis, fisik, atau seksual |
| `meansfalsepromises` | boolean | Menunjukkan penggunaan janji palsu dalam proses recruitment atau eksploitasi |
| `meansdrugsalcohol` | boolean | Menunjukkan keterlibatan drugs atau alcohol dalam proses eksploitasi |
| `meansdenybasicneeds` | boolean | Menunjukkan adanya pembatasan atau penolakan kebutuhan dasar korban |
| `meansexcessiveworkhours` | boolean | Menunjukkan pemberian jam kerja yang berlebihan kepada korban |
| `meanswithholddocs` | boolean | Menunjukkan adanya penahanan dokumen pribadi korban |

---

#### D. RECRUITER

| Column | Data Type | Definisi |
|---|---|---|
| `recruiter_id` | serial | Primary key yang menjadi identitas unik setiap data hubungan recruiter |
| `recruiterrelationintimatepartner` | boolean | Menunjukkan apakah recruiter merupakan pasangan atau intimate partner korban |
| `recruiterrelationfriend` | boolean | Menunjukkan apakah recruiter merupakan teman korban |
| `recruiterrelationfamily` | boolean | Menunjukkan apakah recruiter memiliki hubungan keluarga dengan korban |
| `recruiterrelationother` | boolean | Menunjukkan apakah recruiter memiliki hubungan lainnya dengan korban |

---

#### E. TRAFFICKING CASES

| Column | Data Type | Definisi |
|---|---|---|
| `case_id` | serial | Primary key yang menjadi identitas unik setiap kasus trafficking |
| `yearofregistration` | varchar(10) | Tahun ketika kasus trafficking dicatat atau diregistrasikan |
| `traffickmonths` | varchar(50) | Durasi korban mengalami trafficking atau eksploitasi dalam bulan |
| `demographic_id` | integer | Foreign key yang menghubungkan kasus dengan data demografi korban |
| `means_id` | integer | Foreign key yang menghubungkan kasus dengan metode eksploitasi |
| `exploitation_id` | integer | Foreign key yang menghubungkan kasus dengan bentuk dan sektor eksploitasi |
| `recruiter_id` | integer | Foreign key yang menghubungkan kasus dengan hubungan recruiter |

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
