ETL / ELT Pipeline Case
Bu proje, farklı veri kaynaklarından elde edilen ham verilerin BigQuery'e yüklenmesi, dönüştürülmesi, test edilmesi ve analiz için hazır hale getirilmesini amaçlayan bir ELT pipeline örneğidir.

🎯 Amaç

Farklı veri kaynaklarını BigQuery'e yüklemek
Verileri temizleyip dönüştürmek (dim/fact/mart modelleri)
Veri kalitesi testlerini uygulamak
Günlük analiz tabloları oluşturmak

📂 Yapı

🧱 Veri Modelleri

Dimensions

dim_customer(customer_id, city, signup_date, …)
dim_subscription(subscription_id, customer_id, status, start_date, end_date)

Facts

fct_order(order_id, customer_id, subscription_id, order_date, items_total, discount_total, shipping_fee, net_revenue, payment_status)
fct_shipment(shipment_id, order_id, latest_status, delivered_at, carrier)
fct_marketing_spend(date, channel, spend_try, clicks)

Marts

mart_subscription_daily — günlük aktif abone, yeni abone, iptaller
mart_revenue_daily — günlük gelir, teslim edilmiş siparişler
mart_acquisition_efficiency — günlük CAC: spend_try / new_subs
mart_kpis_latest — tek satır özet: aktif abone, MRR, churn oranı, CAC geri ödeme süresi

🧪 Data Quality Kontrolleri

Uniqueness: orders.order_id, customers.customer_id, subscriptions.subscription_id
Null Kontrolü: Birincil/harici anahtarlar NULL olamaz
İlişki Bütünlüğü: orders.customer_id → customers.customer_id
Tazelik: Kaynakların güncelliği _PARTITIONTIME ile izlenir
İş Kuralları: discount_total ≥ 0, net_revenue ≥ 0

🔁 Orchestration (Prefect)

Veriler BigQuery’e yüklenir
dbt dönüşümleri sırasıyla çalıştırılır
Data quality testleri uygulanır
Akış metrikleri (çalışma süresi, satır sayısı, hata durumu) Prefect üzerinden loglanır

🧩 Varsayımlar ve Notlar

Clicks (tıklama sayısı)
Mevcut kaynaklarda clicks alanı bulunmamaktadır.
Şimdilik clicks = 0 olarak atanmıştır.
Gerçek senaryoda tıklama verisi reklam API’lerinden (ör. Meta Ads, Google Ads) alınır ve fct_marketing_spend(date, channel, spend_try, clicks) tablosuna eklenir.

İptal (churn) tarihi
Kaynaklarda iptal tarihi (end_date) alanı bulunmadığından,
nextOrderDate iptal tarihi gibi kabul edilmiştir.
Bu yaklaşım sadece tahmini churn oranı hesaplamak içindir; gerçek sistemde end_date alanı kullanılmalıdır.

Partitioning (ingestion-time)
Sorgularda _PARTITIONDATE veya _PARTITIONTIME alanları üzerinden filtreleme yapılır:

WHERE _PARTITIONDATE BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
