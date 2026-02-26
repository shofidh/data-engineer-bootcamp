# Kafka Basic Concept

# Kafka Assignment - Data Engineer

## 📌 Objective
Proyek ini bertujuan untuk membangun dan mengonfigurasi Kafka Producer serta Consumer dengan menerapkan konsep:
* **Key-Based Partitioning:** Menjamin urutan data (*ordering*) berdasarkan entitas spesifik.
* **Consumer Group:** Memastikan pemrosesan data secara efisien dan paralel.
* **Data Processing:** Melakukan agregasi sederhana secara *real-time* dari *event streaming*.

---

## 1. Producer & Key-Based Partitioning

Producer dikonfigurasi untuk mengirimkan *event* setiap 5 detik ke topic `events_topic`. Tiga *key* yang digunakan untuk simulasi adalah: `user_1`, `user_2`, dan `user_3`.

**Bagaimana Kafka menentukan partisi berdasarkan Key?**

Kafka menggunakan algoritma *hashing* pada *key* pesan untuk menentukan partisi tujuan. Formula dasarnya adalah:
`hash(key) % jumlah_partition = nomor_partition`

**Efek dari mekanisme ini:**
*Event* yang memiliki *key* yang sama (misalnya `user_1`) akan **selalu** masuk ke partisi yang sama. Hal ini sangat krusial dalam *Data Engineering* karena menjaga urutan aktivitas (*ordering*) per *user* agar tidak tertukar saat diproses.

---

## 2. Consumer Implementation & Processing

Consumer dibangun untuk melakukan *subscribe* ke topic `events_topic` dan memproses aliran data secara langsung. 

**Pemrosesan Sederhana yang Dilakukan:**
Consumer membaca *payload* JSON dan melakukan agregasi berupa **pencatatan dan perhitungan jumlah *event* (aktivitas) per *user*** secara *real-time*. Hasil agregasi dan informasi partisi asal *event* langsung dicetak ke log terminal.

**Contoh Output Terminal:**
*(Screenshot terminal saat Producer mengirim data dan Consumer memprosesnya)*
![Producer Running](https://github.com/shofidh/data-engineer-bootcamp/blob/main/kafka_basic/screenshoots/producer_running.png)
![Consumer Running](https://github.com/shofidh/data-engineer-bootcamp/blob/main/kafka_basic/screenshoots/consumer_running.png)

---

## 3. Observasi Consumer Group (Minimal 2 Consumer)

Untuk menguji skalabilitas, dijalankan dua Kafka Consumer secara bersamaan dengan `group.id` yang sama (`events_group`).


**Laporan Pengamatan Pembagian Data:**
Ketika dua consumer berada dalam satu *Consumer Group*, Kafka secara otomatis membagi beban kerja (*load balancing*). Berdasarkan observasi saat program dijalankan:
* **Consumer A** (*left*) ditugaskan untuk membaca Partisi 1.
* **Consumer B** (*right*) ditugaskan untuk membaca Partisi 2 (dan Partisi 0 jika ada).
* Data dari `user_1` dan `user_2` (yang berada di Partisi 2) hanya diproses oleh Consumer B, sementara data `user_3` (di Partisi 1) diproses oleh Consumer A.

**Screenshot 2 Consumer Berjalan Paralel:**
![Two Consumers](https://github.com/shofidh/data-engineer-bootcamp/blob/main/kafka_basic/screenshoots/two_consumers.png)

---

## 4. Rebalancing Mechanism

Sebagai bentuk pengujian keandalan sistem (*fault tolerance*), salah satu Consumer dimatikan secara paksa (CTRL + C). 

**Hasil Pengamatan:**
Kafka mendeteksi bahwa salah satu *node* terputus dan langsung memicu proses **Rebalancing**. Consumer yang tersisa secara otomatis mengambil alih seluruh partisi dari consumer yang mati, sehingga pemrosesan data tidak terhenti.

**Screenshot Rebalancing:**
![Rebalance](https://github.com/shofidh/data-engineer-bootcamp/blob/main/kafka_basic/screenshoots/rebalance.png)

---

## 5. Kesimpulan Observasi

Dari implementasi dan pengujian di atas, dapat ditarik kesimpulan mengenai perilaku *Consumer Group* di Kafka:
* **No Duplication:** Setiap partisi di dalam sebuah *topic* hanya bisa dibaca oleh **satu** consumer di dalam satu *consumer group* yang sama.
* **Maximum Parallelism:** Jumlah consumer maksimal yang efektif dalam satu grup adalah sama dengan jumlah partisi. Jika jumlah consumer melebihi jumlah partisi, maka consumer yang berlebih akan menganggur (*idle*).
* **Scalability & Reliability:** Penggunaan *consumer group* membuat sistem mudah diskalakan untuk membagi beban kerja yang besar, sekaligus menjaga sistem tetap tangguh meskipun ada *node* yang gagal.
