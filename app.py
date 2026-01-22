import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime
import time

st.set_page_config(page_title="MSCI Para Akışı Analizi", layout="wide")
st.title("📊 MSCI Para Akış Sinyal Terminali")

# Hisse listesi - DÜZENLEME YOK
hisseler = ['ASELS.IS', 'BIMAS.IS', 'AKBNK.IS', 'TUPRS.IS', 'KCHOL.IS', 'THYAO.IS', 'TCELL.IS','ISCTR.IS','YKBNK.IS','FROTO.IS']

# Kullanıcıdan seçim ALMA, hep tüm hisseler analiz edilir
secili_hisseler = hisseler

@st.cache_data(show_spinner="Veriler çekiliyor...")
def hisse_verisi_cek(hisse, max_deneme=3, bekleme_suresi=2):
    """
    Bir hisse için veri çekme fonksiyonu - retry mekanizması ile
    """
    for deneme in range(max_deneme):
        try:
            if deneme > 0:
                time.sleep(bekleme_suresi * deneme)
            ticker = yf.Ticker(hisse)
            hisse_df = ticker.history(period="1mo", auto_adjust=True)

            if hisse_df.empty:
                if deneme < max_deneme - 1:
                    continue
                else:
                    return None
            return hisse_df
        except Exception:
            if deneme < max_deneme - 1:
                continue
            else:
                return None
    return None

def analiz_yap(hisse_listesi):
    analiz_listesi = []
    rapor_progress = st.progress(0, text="Analiz başlatılıyor...")
    toplam = len(hisse_listesi)
    for idx, hisse in enumerate(hisse_listesi, 1):
        rapor_progress.progress(idx / toplam, text=f"{hisse} işleniyor ({idx}/{toplam})...")
        hisse_df = hisse_verisi_cek(hisse)
        if hisse_df is None:
            continue
        try:
            close_prices = hisse_df['Close']
            volumes = hisse_df['Volume']
            if len(close_prices) < 6 or len(volumes) < 20:
                continue
            fiyat_5g = close_prices.pct_change(5).iloc[-1] * 100
            hacim_ort_20 = volumes.rolling(window=20).mean().iloc[-1]
            son_hacim = volumes.iloc[-1]
            hacim_gucu = son_hacim / hacim_ort_20 if hacim_ort_20 else 0.0

            if fiyat_5g > 0 and hacim_gucu > 1.2:
                durum, puan = "GÜÇLÜ GİRİŞ", 3
            elif fiyat_5g < 0 and hacim_gucu > 1.2:
                durum, puan = "GÜÇLÜ ÇIKIŞ", -3
            else:
                durum, puan = "NORMAL / ROTASYON", 0

            analiz_listesi.append({
                'Tarih': datetime.now().strftime('%Y-%m-%d'),
                'Hisse': hisse,
                'Fiyat Değişim (5G %)': round(fiyat_5g, 2),
                'Hacim Gücü (x)': round(hacim_gucu, 2),
                'Para Akış Sinyali': durum,
                'Skor': puan
            })
            time.sleep(0.2)  # UI'nin "donmaması" için kısa bekleme
        except Exception:
            continue
    rapor_progress.empty()
    return analiz_listesi


if st.button("Analiz Et"):
    st.info(f"Veriler analiz ediliyor... ({len(secili_hisseler)} hisse seçili)")
    analiz_sonuclari = analiz_yap(secili_hisseler)
    if analiz_sonuclari:
        df = pd.DataFrame(analiz_sonuclari).sort_values(by='Skor', ascending=False)
        st.success(f"✅ {len(df)} hisse analiz edildi.")
        st.dataframe(df, use_container_width=True)
        # Excel download
        dosya_adi = f"msci_para_akisi_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
        import io
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine="openpyxl")
        excel_bytes = excel_buffer.getvalue()
        st.download_button(
            label="Raporu Excel Olarak İndir",
            data=excel_bytes,
            file_name=dosya_adi,
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        # Ana özet tabloyu sade göster
        st.subheader("Özet Para Akışı Durumları")
        st.dataframe(df[['Hisse', 'Para Akış Sinyali', 'Skor']], use_container_width=True)
    else:
        st.warning("Uygun veri bulunamadı veya analiz gerçekleştirilemedi.")
else:
    st.write(
        '''
        Bu uygulama seçili MSCI Türkiye hisseleri için son 1 ayda **para akışı sinyali** çıkarır.

        - **GÜÇLÜ GİRİŞ:** Hisse yukarı ve hacim güçlü.
        - **GÜÇLÜ ÇIKIŞ:** Hisse aşağı ve hacim güçlü.
        - **ROTASYON:** Farklı senaryolar.

        Analiz için aşağıdaki 'Analiz Et' butonuna tıklayın.
        '''
    )
