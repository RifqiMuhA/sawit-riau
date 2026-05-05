# Bab 4 — Hasil dan Pembahasan

Bab ini menyajikan hasil analisis dan interpretasi temuan dari Sistem Monitoring Perkebunan Sawit Riau. Setiap sub-bab mengacu pada modul analitik yang tersedia di dasbor dan menjelaskan cara angka dihitung, apa yang divisualisasikan, serta implikasi strategisnya.

---

## 4.1 Evaluasi Vitalitas Kebun (NDVI)

> *[Screenshot: Tab "Kondisi Kebun" — Peta spasial kondisi lahan periode terkini (April 2026) dan panel Tren NDVI Bulanan dengan filter Semua Kabupaten]*

**Sumber Data & Cara Penghitungan**

Data NDVI (*Normalized Difference Vegetation Index*) diperoleh dari citra satelit Sentinel-2 via Google Earth Engine (GEE) dan disimpan dalam tabel `fact_ndvi`. NDVI mengukur kerapatan kanopi vegetasi dengan nilai berkisar antara -1 hingga 1, di mana nilai mendekati 1 menunjukkan vegetasi yang lebat dan sehat. Nilai NDVI yang ditampilkan adalah **rata-rata piksel per kabupaten per bulan** (`ndvi_mean`).

Status kebun diklasifikasikan menggunakan **metode ranking persentil bulanan** (`Status Kebun = Ranking Persentil NDVI Bulanan`):
- **Tinggi (Top 33%)** — NDVI tertinggi se-Riau pada periode tersebut
- **Sedang (Mid 33%)** — NDVI menengah
- **Rendah (Bottom 33%)** — NDVI terendah, mengindikasikan kanopi yang rusak atau terdegradasi

Pendekatan persentil dipilih agar penilaian bersifat *relatif terhadap kondisi Riau secara keseluruhan* di setiap periode, bukan berdasarkan ambang batas tetap yang tidak merespons kondisi iklim.

**Interpretasi Visualisasi**

Peta choropleth periode terkini (April 2026) menunjukkan pola spasial yang sangat jelas: wilayah **pesisir timur dan utara Riau** — Bengkalis, Dumai, Kepulauan Meranti, dan sebagian Rokan Hulu — secara konsisten tampil dalam warna merah (*Rendah/Bottom 33%*), sementara wilayah di bagian tengah-selatan seperti Indragiri Hilir, Siak, dan Kuantan Singingi tampil hijau (*Tinggi/Top 33%*).

Pola ini bukan anomali satu periode, melainkan mencerminkan **kondisi ekologis struktural yang berbeda** antar wilayah. Kabupaten pesisir memiliki kombinasi faktor yang menekan kualitas kanopi secara berkepanjangan: lahan gambut yang rentan terhadap kekeringan, tekanan konversi lahan, serta risiko kebakaran yang lebih tinggi di musim kemarau.

Pada panel Tren NDVI Bulanan, grafik garis memperlihatkan nilai NDVI seluruh kabupaten berfluktuasi mengikuti siklus tahunan. Namun ketika filter dipersempit pada kabupaten **Kab. Bengkalis** — salah satu yang berstatus Rendah — tren nilai NDVI-nya tidak hanya rendah secara absolut, tetapi juga menunjukkan *pemulihan yang lambat* setelah periode musim kemarau dibandingkan kabupaten lain. Ini mengindikasikan bahwa lahan sawit di Bengkalis membutuhkan waktu lebih lama untuk pulih, yang berkaitan langsung dengan kondisi lahan gambut yang mendominasi wilayah tersebut.

> **Saran Decision Savi:** *"Lahan di Bengkalis, Dumai, Kepulauan Meranti, Rokan Hulu berada di kelompok 33% terbawah se-Riau. Perlu alokasi anggaran replanting dan intervensi pemupukan segera di wilayah tersebut!"*

Saran ini menunjukkan bahwa sistem tidak hanya memvisualisasikan kondisi, tetapi secara otomatis mengidentifikasi wilayah prioritas intervensi berdasarkan data terkini — menjadikan dasbor ini efektif sebagai alat bantu pengambilan keputusan operasional bagi Dinas Perkebunan.

---

## 4.2 Klasifikasi Produktivitas PKS dengan K-Means Clustering

> *[Screenshot: Tab "Produktivitas" — Grafik tren rata-rata produktivitas per cluster (semua tahun) dan kotak saran Savi]*

**Sumber Data & Cara Penghitungan**

Data produktivitas bersumber dari `fact_produksi` yang mengintegrasikan laporan Excel PKS (dari Dinas Perkebunan) dan database operasional perusahaan. Nilai yang dianalisis adalah **produktivitas ton TBS per hektar** (`produktivitas = produksi_tbs_ton ÷ luas_panen_ha`), dihitung per perusahaan per bulan.

Klasifikasi cluster menggunakan algoritma **K-Means dengan k=3** yang dijalankan oleh DAG analitik (`dag4_analitik.py`). Setiap bulan, 12 PKS dikelompokkan ke dalam:
- **Overperform** — produktivitas tertinggi, berada di atas centroid cluster
- **Average** — produktivitas menengah
- **Underperform** — produktivitas terendah

Penggunaan K-Means yang dijalankan ulang setiap bulan memungkinkan label cluster bersifat dinamis dan merespons perubahan performa relatif antar perusahaan.

**Interpretasi Visualisasi**

Grafik tren memperlihatkan tiga garis yang bergerak secara paralel dari Januari 2023 hingga April 2026: garis hijau tua (*Overperform*) secara konsisten berada di atas, garis merah (*Underperform*) di bawah, dengan selisih yang tidak menyempit secara signifikan selama tiga tahun pengamatan.

Ini merupakan temuan yang perlu dicermati: **kesenjangan produktivitas antar cluster tidak berkurang**. Kondisi ini mengindikasikan bahwa perbedaan kinerja antar PKS bukan semata-mata akibat faktor musiman yang bersifat sementara, melainkan mencerminkan **perbedaan kapasitas manajemen, kondisi lahan, atau skala investasi** yang struktural. Ketika filter tahun dikecilkan ke **Tahun 2025**, pola tersebut tetap bertahan — cluster Overperform konsisten unggul bahkan di tahun yang lebih baru.

Fluktuasi yang tampak pada ketiga garis (naik di pertengahan tahun, turun di awal dan akhir tahun) bukan mengindikasikan ketidakstabilan kinerja, melainkan mencerminkan **siklus panen sawit** yang memang memuncak di bulan-bulan tertentu. Yang menjadi perhatian adalah cluster Underperform mengalami penurunan yang lebih dalam saat musim rendah (*off-season*), menunjukkan ketidakmampuan dalam mengelola produktivitas minimum saat kondisi tidak optimal.

> **Saran Decision Savi:** *"4 dari 12 Perusahaan Kelapa Sawit (PKS) berada dalam kategori Underperform secara rata-rata! Segera instruksikan dinas terkait untuk melakukan audit mesin dan evaluasi manajemen pabrik-pabrik tersebut."*

Angka "4 dari 12" ini dihitung secara otomatis oleh sistem berdasarkan mayoritas label cluster yang diterima masing-masing PKS sepanjang sejarah data. Ini memberikan gambaran bahwa hampir sepertiga PKS di Riau membutuhkan intervensi aktif dari sisi manajemen dan operasional.

---

## 4.3 Deteksi Dini Indikasi Penimbunan CPO

> *[Screenshot: Tab "Deteksi Penimbunan" — KPI card total insiden, grafik tren insiden + harga CPO, dan bar chart Top 5 Perusahaan Terindikasi]*

**Sumber Data & Cara Penghitungan**

Data operasional bersumber dari tabel `fact_operasional` yang mencatat stok CPO akhir bulan (`stok_akhir_ton`) dan volume penjualan (`volume_penjualan_ton`) per perusahaan per bulan, sementara harga CPO pasar diperoleh dari `dim_periode.harga_cpo` berdasarkan data resmi Dinas Perkebunan Riau.

Indikasi penimbunan ditandai (`indikasi_timbun = TRUE`) ketika **ketiga kondisi berikut terpenuhi secara simultan dalam satu bulan**:
1. Harga CPO pasar sedang turun (di bawah rata-rata 3 bulan sebelumnya)
2. Volume penjualan PKS turun (di bawah rata-rata historis rolling 3 bulan)
3. Stok akhir CPO naik (melebihi rata-rata historis rolling 3 bulan)

Logika ini didasarkan pada perilaku ekonomi: ketika harga turun, produsen yang berupaya menghindari kerugian akan cenderung menahan penjualan (*wait and see*) sambil membiarkan stok menumpuk.

**Interpretasi Visualisasi**

Grafik tren dual-axis memperlihatkan hubungan yang intuitif: **batang merah (jumlah PKS terindikasi) cenderung muncul lebih tinggi tepat saat garis harga CPO mengalami penurunan**. Pola ini paling terlihat pada periode pertengahan 2023 ketika harga CPO mengalami koreksi yang cukup signifikan — batang insiden di periode tersebut merupakan yang tertinggi dalam rentang data yang ditampilkan.

Pada periode 2024–2025, seiring harga CPO yang merangkak naik dan stabil di level lebih tinggi, frekuensi insiden penimbunan berkurang. Ini **mengkonfirmasi validitas logika rule-based** yang dibangun: deteksi bereaksi secara proporsional terhadap dinamika pasar, bukan menghasilkan alarm palsu secara acak.

Bar chart "Top 5 Perusahaan Terindikasi" menunjukkan bahwa insiden **tidak merata antar PKS**. Beberapa perusahaan muncul jauh lebih sering dibandingkan yang lain — hal ini mengisyaratkan perbedaan dalam kapasitas gudang penyimpanan atau strategi penjualan. PKS dengan kapasitas stok lebih besar memiliki fleksibilitas lebih untuk menahan penjualan saat harga tidak menguntungkan.

Ketika filter perusahaan diarahkan ke salah satu PKS yang terindikasi paling sering, grafik garis individual memperlihatkan secara jelas periode-periode di mana stok naik bersamaan dengan volume jual yang turun — memberikan bukti visual yang konkret untuk masing-masing kejadian yang terdeteksi.

> **Saran Decision Savi:** *"Secara total provinsi, terdapat 74 temuan indikasi penimbunan. Cek grafik 'Top 5' di bawah untuk melihat perusahaan mana saja yang paling sering terindikasi."*

Angka 74 ini merepresentasikan total bulan-perusahaan di mana ketiga kondisi penimbunan terpenuhi sekaligus. Dengan 12 PKS dan 40 bulan periode data, terdapat 480 kemungkinan kejadian — dan 74 di antaranya (15.4%) memenuhi kriteria indikasi.

---

## 4.4 Evaluasi Pencapaian Target Panen

> *[Screenshot: Tab "Realisasi Panen" — KPI card total target, realisasi, dan gap; grouped bar chart target vs. realisasi per perusahaan; dan donut chart distribusi status panen]*

**Sumber Data & Cara Penghitungan**

Data panen berasal dari tabel `fact_panen` yang memuat target dan realisasi panen TBS per blok kebun per bulan. Gap dihitung menggunakan rumus yang ditampilkan langsung di dasbor:

**Gap Panen (%) = ((Realisasi − Target) ÷ Target) × 100%**

Nilai gap negatif berarti realisasi lebih rendah dari target. Status panen dibagi tiga: *selesai* (panen selesai dilaksanakan), *tertunda* (belum terealisasi), dan *batal*.

**Interpretasi Visualisasi**

KPI card di bagian atas halaman menampilkan gambaran agregat seluruh PKS Riau: total target yang ditetapkan dan total realisasi yang dicapai, dengan gap rata-rata -9.0%. Ini berarti secara keseluruhan, produksi TBS aktual berada **hampir 10% di bawah rencana yang telah disusun**.

Grouped bar chart memberikan gambaran yang lebih terperinci: **setiap perusahaan** ditampilkan dengan dua batang berdampingan (hijau muda = target, hijau tua = realisasi). Tanpa terkecuali, batang realisasi selalu lebih pendek dari batang target di semua 12 PKS. Ini bukan hanya berarti beberapa PKS gagal mencapai target — ini berarti **tidak ada satu pun PKS yang berhasil melampaui target secara agregat**. Temuan ini mengisyaratkan permasalahan yang lebih dalam: apakah penetapan target terlalu optimistis, atau ada kendala operasional yang belum terselesaikan secara sistemik.

Ketika filter diarahkan ke **PT Kuansing Mas** (PKS dengan gap relatif terbesar), perbedaan antara batang target dan realisasi tampak paling mencolok secara visual. Sebaliknya, PKS seperti **PT Meranti Jaya** menunjukkan gap yang lebih kecil, meskipun total produksinya juga lebih rendah secara absolut.

Donut chart status panen menunjukkan bahwa **mayoritas blok (94%) berstatus selesai**, namun ada sebagian kecil yang masih *tertunda*. Blok-blok dengan status tertunda ini memiliki realisasi nol — artinya panen belum dilaksanakan sama sekali pada periode tersebut, bukan panen yang baru sebagian.

> **Saran Decision Savi:** *"Rata-rata panen 9.0% di bawah target. Segera evaluasi kebun yang berada di zona merah (Under-Target) pada grafik sebaran di bawah."*

Saran ini dikaitkan langsung dengan scatter plot gap di bawahnya, di mana titik-titik merah merepresentasikan blok kebun yang secara spesifik berada dalam kondisi under-target — memandu pengguna dasbor untuk langsung mengidentifikasi objek yang perlu ditindaklanjuti.

---

## 4.5 Hasil Reporting: Gambaran Sawit Riau dan Alert Operasional

### 4.5.1 Profil Lahan dan Produksi TBS

> *[Screenshot: Tab "Gambaran Sawit" — Pie chart status lahan, bar chart varietas, peta choropleth produksi per kabupaten, dan area chart tren produksi TBS bulanan]*

**Sumber Data:** Data lahan bersumber dari `dim_kebun` (profil blok kebun tiap PKS), sementara data produksi berasal dari `fact_produksi`. Produksi agregat per kabupaten dihitung dengan menjumlahkan seluruh produksi TBS dari PKS yang berlokasi di kabupaten tersebut.

Pie chart status lahan menunjukkan bahwa porsi terbesar lahan berada dalam status **produktif** — ini merupakan dasar aktif produksi TBS yang dimonitor. Porsi **TBM (Tanaman Belum Menghasilkan)** yang cukup besar mengindikasikan bahwa Riau sedang dalam fase ekspansi lahan sawit, sehingga kapasitas produksi total diproyeksikan akan meningkat ketika lahan-lahan TBM ini mencapai usia produktif. Sementara porsi **replanting** mencerminkan siklus regenerasi kebun yang berjalan secara alami.

Peta choropleth produksi per kabupaten memperlihatkan konsentrasi produksi yang signifikan di kabupaten **Rokan Hulu dan Rokan Hilir** — ditandai dengan warna lebih gelap pada peta. Sebaliknya, Kepulauan Meranti, Pekanbaru, dan Dumai memiliki warna lebih terang, mencerminkan kapasitas produksi yang lebih terbatas akibat keterbatasan lahan perkebunan di daerah tersebut. Pola ini konsisten dengan data produktivitas di tab sebelumnya.

Area chart tren produksi bulanan menunjukkan **volume produksi yang relatif stabil** sepanjang tiga tahun pengamatan (2023–2025), dengan fluktuasi musiman yang wajar. Tidak terlihat tren penurunan jangka panjang yang mengkhawatirkan, mengindikasikan keberlanjutan produksi sawit Riau dalam kondisi yang terjaga.

---

### 4.5.2 Monitoring Alert Operasional

> *[Screenshot: Tab "Alert Operasional" — KPI card total alert dan tingkat penanganan; stacked bar chart alert bulanan; bar chart jenis alert terbanyak]*

**Sumber Data:** Data alert bersumber dari `fact_alert_operasional`, yang merupakan agregasi bulanan dari log peringatan harian yang disimpan di MongoDB masing-masing perusahaan. Setiap alert yang terbaca dari MongoDB dikategorikan berdasarkan jenisnya (`jenis_alert_terbanyak`) dan dilabeli apakah sudah ditangani atau belum.

KPI card menampilkan total seluruh alert yang pernah tercatat, berapa yang berhasil ditangani (*alert_ditangani*), yang tidak ditangani (*alert_tidak_ditangani*), dan tingkat penanganan keseluruhan sebesar **67%**. Artinya, dari setiap 3 alert yang muncul, rata-rata hanya 2 yang berhasil direspons — satu dibiarkan tanpa tindak lanjut.

Stacked bar chart tren bulanan memperlihatkan bahwa jumlah alert yang tidak ditangani (segmen merah) hadir secara konsisten di setiap bulan tanpa penurunan yang berarti dari waktu ke waktu. Ini berbeda dari skenario "backlog sementara yang akan terselesaikan" — pola yang konsisten ini mengindikasikan adanya **keterbatasan kapasitas respons yang bersifat struktural**, bukan insidental.

Bar chart jenis alert terbanyak menampilkan empat kategori yang dominan: `produksi_rendah`, `tekanan_boiler`, `suhu_sterilizer`, dan `downtime_alat`. Keempat jenis ini seluruhnya berkaitan dengan **kegagalan proses di dalam pabrik** — bukan faktor eksternal seperti cuaca atau harga. Ini mengisyaratkan bahwa banyak PKS menghadapi tantangan dalam hal perawatan mesin dan konsistensi proses sterilisasi, yang berpotensi langsung berdampak pada kualitas dan volume output CPO.

Ketika filter perusahaan diterapkan, terdapat perbedaan yang signifikan dalam tingkat penanganan antar PKS. Beberapa PKS menunjukkan tingkat penanganan di atas rata-rata provinsi, sementara yang lain secara konsisten berada di bawah — mengindikasikan perbedaan kualitas manajemen operasional yang perlu menjadi perhatian dalam pembinaan Dinas Perkebunan.

---

## Ringkasan Temuan

| Dimensi | Temuan Kunci | Implikasi Kebijakan |
|---|---|---|
| **Vitalitas Kebun (NDVI)** | Pesisir timur Riau (Bengkalis, Dumai, Meranti, Rohul) secara struktural berada di persentil NDVI terendah | Prioritas program replanting dan pemupukan di wilayah pesisir |
| **Produktivitas K-Means** | Kesenjangan antar cluster tidak menyempit selama 3 tahun — bersifat struktural, bukan musiman | Audit manajemen untuk 4 PKS yang konsisten Underperform |
| **Deteksi Penimbunan** | Lonjakan insiden berkorelasi langsung dengan penurunan harga CPO, mengkonfirmasi validitas sistem deteksi | Aktivasi pengawasan intensif saat harga CPO turun |
| **Target Panen** | Seluruh 12 PKS tidak mencapai target; sebagian blok memiliki realisasi nol | Review metodologi penetapan target dan kesiapan operasional blok |
| **Alert Operasional** | Tingkat penanganan 67% — 1 dari 3 alert tidak ditangani; didominasi isu mesin pabrik | Program perawatan preventif dan peningkatan responsivitas PKS |

> **Kesimpulan:** Integrasi data multisumber ke dalam satu *data mart* terbukti menghasilkan gambaran yang komprehensif dan dapat ditindaklanjuti. Temuan lintas modul memperlihatkan adanya **korelasi struktural** antara kondisi lahan (NDVI rendah di pesisir), kinerja PKS (produktivitas underperform), dan tantangan operasional (gap panen dan alert tidak tertangani) — pola yang tidak akan terlihat jika data dianalisis secara terpisah. Dasbor ini menjawab kebutuhan Dinas Perkebunan Provinsi Riau akan satu platform analitik yang dapat memandu prioritas intervensi secara *data-driven* dan *real-time*.
