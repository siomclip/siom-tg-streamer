# SiomJourney Telegram Video Streamer Bridge

Bu servis, Telegram kanalınızdaki (`FilmDepom`) filmleri ve videoları web tarayıcılarına (HTML5 video oynatıcılarına) kesintisiz, Range (HTTP 206) ve ileri/geri sarma desteğiyle yayınlayan ücretsiz köprüdür.

## URL Formatı
- `https://SUNUCU-ADRESINIZ/stream/{mesaj_id}`
- Örnek: `https://SUNUCU-ADRESINIZ/stream/4` (*Sadece Sen* filmi)

## Özellikler
- **Sıfır Boyut Sınırı:** 20 MB kısıtlamasını kaldırır; 1 GB, 2 GB ve 4 GB filmleri anında oynatır.
- **Range (HTTP 206) Desteği:** Ziyaretçi filmin 45. dakikasına tıkladığında tüm filmi indirmeden o saniyeden başlatır.
- **CORS Desteği:** GitHub Pages ve harici alan adlarından doğrudan erişim sağlar.
