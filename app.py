import streamlit as st
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import io
from datetime import datetime

# Page configuration
st.set_page_config(page_title="BIST Analysis App", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Sayfa Seçiniz:",
    ["BIST Data Analysis", "MSCI Para Akışı Analizi", "Sektörel Analiz"]
)

# Page 1: BIST Data Analysis (from app.py)
if page == "BIST Data Analysis":
    st.title("BIST Data Analysis")

    # First dropdown: Period selection
    period_options = ["3d", "7d", "1mo", "1y"]
    selected_period = st.selectbox(
        "Dönem Seçiniz:",
        options=period_options,
        index=0
    )
    # Add a selectbox to choose between Close (Kapanis) and Volume (Hacim)
    column_options = {"Kapanis": "Close", "Hacim": "Volume"}
    selected_column_label = st.selectbox(
        "Veri Türü Seçiniz:",  # Select Data Type
        options=list(column_options.keys()),
        index=0
    )
    selected_column = column_options[selected_column_label]

    # Determine interval based on period (not shown to user)
    if selected_period in ["3d", "7d"]:
        selected_interval = "1h"
    else:  # 1mo or 1y
        selected_interval = "1d"

    bt1 = st.button("Analizi Çalıştır", key="run_analysis")

    if bt1:
        tickers = ["FROTO.IS", "BIMAS.IS", "ASELS.IS", "AKBNK.IS","TUPRS.IS","THYAO.IS","TCELL.IS","YKBNK.IS","ISCTR.IS","SAHOL.IS","KCHOL.IS"]
        ticks = {}
        for tick in tickers:
            try:
                df = yf.Ticker(tick).history(period=selected_period, interval=selected_interval)
                ticks[tick] = df[selected_column]
                time.sleep(1)  # prevent throttling
            except Exception as e:
                print(f"{tick} failed: {e}")

        close_df = pd.DataFrame(ticks)
        close_df = close_df.loc[~(close_df == 0).all(axis=1)]

        returns = close_df.pct_change().dropna()

        corr_matrix = returns.corr()
        corr = returns.corr()

        # Display correlation matrix as Streamlit figure
        fig, ax = plt.subplots(figsize=(9, 6))
        sns.heatmap(
            corr,
            annot=True,
            fmt=".2f",
            cmap="coolwarm",
            vmin=-1,
            vmax=1,
            linewidths=0.5,
            ax=ax
        )
        ax.set_title(f"Correlation Matrix {selected_column_label}-{selected_period}")
        plt.tight_layout()
        st.pyplot(fig)

        excel_buffer = io.BytesIO()
        corr.to_excel(excel_buffer, index=True)
        excel_buffer.seek(0)

        st.download_button(
            label="Korelasyon Matrisi Excel İndir",
            data=excel_buffer,
            file_name=f"{selected_column_label}_correlation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Page 2: MSCI Para Akışı Analizi (from demo.py)
elif page == "MSCI Para Akışı Analizi":
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

# Page 3: Sektörel Analiz
elif page == "Sektörel Analiz":
    st.title("📊 MSCI Turkey Sektörel Analiz")

    st.write(
        """
        Bu sayfa seçili hisseler üzerinden **sektörel para giriş hızını** analiz eder.

        - Son 1 ay verisi kullanılır.
        - 5 günlük fiyat getirisi ve 20 günlük ortalama hacim baz alınır.
        - Sektör skoru = Haftalık Getiri % x Hacim Gücü
        """
    )

    # 1. Sektörel Gruplandırma
    sektor_haritasi = {
    'PETKM.IS': 'İşlenebilen endüstriler',
    'SASA.IS': 'İşlenebilen endüstriler',
    'GUBRF.IS': 'İşlenebilen endüstriler',

    'TCELL.IS': 'İletişim',
    'TTKOM.IS': 'İletişim',

    'ASTOR.IS': 'Üretici imalatı',

    'TAVHL.IS': 'Taşımacılık',
    'PGSUS.IS': 'Taşımacılık',
    'THYAO.IS': 'Taşımacılık',

    'BIMAS.IS': 'Perakende satış',
    'MGROS.IS': 'Perakende satış',

    'AKBNK.IS': 'Finans',
    'SAHOL.IS': 'Finans',
    'DSTKF.IS': 'Finans',
    'EKGYO.IS': 'Finans',
    'YKBNK.IS': 'Finans',
    'GARAN.IS': 'Finans',
    'ISCTR.IS': 'Finans',

    'EREGL.IS': 'Enerji-dışı mineraller',
    'TRALT.IS': 'Enerji-dışı mineraller',
    'KRDMD.IS': 'Enerji-dışı mineraller',

    'TUPRS.IS': 'Enerji mineralleri',
    'KCHOL.IS': 'Enerji mineralleri',

    'ENKAI.IS': 'Endüstriyel hizmetler',

    'ASELS.IS': 'Elektronik teknoloji',

    'SISE.IS': 'Dayanıklı tüketim malları',
    'TOASO.IS': 'Dayanıklı tüketim malları',
    'FROTO.IS': 'Dayanıklı tüketim malları',

    'AEFES.IS': 'Dayanıklı olmayan tüketici ürünleri',
    'ULKER.IS': 'Dayanıklı olmayan tüketici ürünleri'
    }

    hisseler = list(sektor_haritasi.keys())

    if st.button("Sektörel Analizi Çalıştır"):
        with st.spinner("Sektörel trendler hesaplanıyor..."):
            try:
                # Veri çekimi
                data = yf.download(hisseler, period="1mo")

                if data.empty:
                    st.warning("Veri çekilemedi. Lütfen daha sonra tekrar deneyin.")
                else:
                    # 2. Getiri ve Hacim Hesaplama
                    returns = data['Close'].pct_change(5).iloc[-1] * 100
                    volumes = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]

                    # 3. Verileri Birleştirme
                    df = pd.DataFrame({
                        'Hisse': returns.index,
                        'Sektör': [sektor_haritasi[h] for h in returns.index],
                        'Haftalık Getiri %': returns.values,
                        'Hacim Gücü': volumes.values
                    })

                    # 4. Sektörel Ortalama Hesaplama (Ağırlıklı Güç)
                    df['Sektör Skoru'] = df['Haftalık Getiri %'] * df['Hacim Gücü']
                    sektor_ozet = df.groupby('Sektör')['Sektör Skoru'].mean().sort_values(ascending=False)

                    st.subheader("Sektörel Güç Sıralaması (Para Nereye Gidiyor?)")
                    st.dataframe(
                        sektor_ozet.reset_index().rename(columns={'Sektör Skoru': 'Ortalama Sektör Skoru'}),
                        use_container_width=True
                    )

                    # 5. Görselleştirme (Barplot)
                    fig, ax = plt.subplots(figsize=(10, 6))
                    sns.barplot(
                        x=sektor_ozet.values,
                        y=sektor_ozet.index,
                        palette='RdYlGn',
                        ax=ax
                    )
                    ax.set_title('MSCI Turkey Sektörel Para Giriş Hızı')
                    ax.set_xlabel('Güç Skoru (Fiyat x Hacim)')
                    ax.grid(axis='x', linestyle='--', alpha=0.7)
                    st.pyplot(fig)

                    # Detaylı hisse tablosu
                    st.subheader("Hisse Bazında Detaylı Veriler")
                    st.dataframe(df.sort_values('Sektör Skoru', ascending=False), use_container_width=True)
            except Exception as e:
                st.error(f"Sektörel analiz sırasında bir hata oluştu: {e}")
