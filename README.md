# Retail Data Analysis with Apache Spark and Airflow

## Tujuan Proyek
Proyek ini bertujuan untuk melakukan analisis data retail menggunakan Apache Spark, PostgreSQL, dan Airflow. Analisis ini mencakup perhitungan pendapatan bulanan, retensi pelanggan, churn rate, dan nilai pesanan rata-rata. Proyek ini juga menyertakan pemeringkatan pelanggan berdasarkan total pendapatan.

## Struktur Direktori
Berikut adalah struktur direktori proyek ini:

```
project-root/
├── dags/
│   └── spark-retail-dag.py
├── spark-scripts/
│   └── spark-retail-analysis.py
├── logs/
├── config/
│   └── airflow.cfg
├── data/
│   └── online-retail-dataset.csv
├── sql/
│   ├── ddl-retail.sql
│   ├── ingest-retail.sql
│   └── warehouse-ddl.sql
├── docker-compose.yml
└── README.md
```

## Prasyarat
Pastikan Anda telah menginstal Docker dan Docker Compose sebelum menjalankan proyek ini.

## Instruksi Menjalankan Proyek

### 1. Build Docker Images
Untuk membangun gambar Docker, jalankan perintah berikut:
```sh
make docker-build
```
atau untuk pengguna chip arm:
```sh
make docker-build-arm
```

### 2. Menjalankan Layanan

#### a. Menjalankan Postgres
```sh
make postgres
```

#### b. Menjalankan Spark Cluster
```sh
make spark
```

#### c. Menjalankan Airflow
```sh
make airflow
```

## Penjelasan Kode

### `spark-retail-dag.py`
File ini mendefinisikan DAG Airflow yang digunakan untuk mengirimkan pekerjaan Spark. DAG ini dijalankan setiap hari dan menggunakan `SparkSubmitOperator` untuk menjalankan skrip Spark.

### `spark-retail-analysis.py`
File ini berisi skrip Spark yang melakukan analisis data retail. Analisis ini mencakup:

1. **Pendapatan Bulanan**: Menghitung pendapatan bulanan berdasarkan tanggal faktur.
2. **Retensi Pelanggan**: Menghitung jumlah retensi pelanggan berdasarkan ID pelanggan.
3. **Churn Rate**: Menghitung churn rate pelanggan.
4. **Nilai Pesanan Rata-rata**: Menghitung nilai rata-rata pesanan per pelanggan.
5. **Pemeringkatan Pelanggan**: Memeringkat pelanggan berdasarkan total pendapatan.

Output analisis ini dicetak ke konsol dan disimpan kembali ke tabel Postgres.

### Analisis Data

1. **Churn Rate**: Menampilkan churn rate dari pelanggan.
2. **Pendapatan Bulanan**:
    - Untuk pelanggan dengan `CustomerID` yang valid.
    - Untuk transaksi tanpa `CustomerID`.
3. **Retensi Pelanggan**: Menampilkan retensi pelanggan untuk pelanggan dengan `CustomerID` yang valid.
4. **Nilai Pesanan Rata-rata**: Menampilkan nilai pesanan rata-rata untuk pelanggan dengan `CustomerID` yang valid.
5. **Pemeringkatan Pelanggan**: Menampilkan peringkat pelanggan berdasarkan total pendapatan untuk pelanggan dengan `CustomerID` yang valid.

### Contoh Hasil
Output dari skrip Spark akan terlihat seperti ini di log Airflow:
```
Number of rows with null CustomerID: X
Churn Rate: Y%
Monthly Revenue for Customers with Valid CustomerID:
+------------+------------------+
| InvoiceDate|   MonthlyRevenue |
+------------+------------------+
| 2021-01-01 | 12345.67         |
| 2021-02-01 | 23456.78         |
+------------+------------------+
Customer Retention for Customers with Valid CustomerID:
+-----------+-----------------+
| CustomerID| RetentionCount  |
+-----------+-----------------+
| 123       | 3               |
| 456       | 2               |
+-----------+-----------------+
Average Order Value for Customers with Valid CustomerID:
+-----------+-------------------+
| CustomerID| AverageOrderValue |
+-----------+-------------------+
| 123       | 123.45            |
| 456       | 234.56            |
+-----------+-------------------+
Customer Ranking by Total Revenue for Customers with Valid CustomerID:
+-----------+-------------+----+
| CustomerID| TotalRevenue|Rank|
+-----------+-------------+----+
| 123       | 123456.78   | 1  |
| 456       | 234567.89   | 2  |
+-----------+-------------+----+
Monthly Revenue for Transactions without CustomerID:
+------------+------------------+
| InvoiceDate|   MonthlyRevenue |
+------------+------------------+
| 2021-01-01 | 1234.56          |
| 2021-02-01 | 2345.67          |
+------------+------------------+
```
