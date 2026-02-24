import streamlit as st
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import io
import json
from datetime import datetime

def _chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def download_selected_column(
    tickers,
    *,
    period,
    interval,
    selected_column,
    auto_adjust=True,
    batch_size=20,
    pause_s=2.0,
    tries=3,
    timeout=20,
):
    """
    Download a single OHLCV column (Close/Volume) for many tickers.

    Uses batching + small pauses to reduce timeouts/throttling and returns a
    DataFrame indexed by datetime with tickers as columns.
    """
    tickers_list = list(tickers) if isinstance(tickers, (list, tuple, set)) else [tickers]
    frames = []

    for batch in _chunk_list(tickers_list, batch_size):
        last = None
        for attempt in range(tries):
            try:
                last = yf.download(
                    batch,
                    period=period,
                    interval=interval,
                    group_by="column",
                    auto_adjust=auto_adjust,
                    threads=False,
                    progress=False,
                    timeout=timeout,
                )
            except Exception:
                last = None

            if isinstance(last, pd.DataFrame) and not last.empty:
                break
            time.sleep(pause_s * (attempt + 1))

        if last is None or last.empty:
            time.sleep(pause_s)
            continue

        try:
            selected = last[selected_column]
        except Exception:
            time.sleep(pause_s)
            continue

        if isinstance(selected, pd.Series):
            # Single ticker edge case
            name = batch[0] if batch else selected_column
            selected = selected.to_frame(name=name)

        frames.append(selected)
        time.sleep(pause_s)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def get_safe_returns(df):
    """Safely calculates returns and handles empty/partial data."""
    if df is None or df.empty:
        return pd.DataFrame()
    # Drop columns that are all NaN or all 0
    df = df.loc[:, (df != 0).any(axis=0)].dropna(axis=1, how="all")
    returns = df.pct_change(fill_method=None).dropna(how="all")
    return returns

# Page configuration
st.set_page_config(page_title="BIST Analysis App", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Sayfa Seçiniz:",
    ["BIST Data Analysis", "MSCI Para Akışı Analizi", "BIST30 Para Akışı", "Sektörel Analiz", "BIST30 Hacim Analizi", "BIST30 Correlation", "Bist30-Full", "Kontrat-Tum"]
)

# Page 1: BIST Data Analysis (from app.py)
if page == "BIST Data Analysis":
    st.title("BIST Data Analysis")

    # First dropdown: Period selection
    period_options = ["5d", "7d", "3d", "1mo", "1y"]
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
    if selected_period in ["5d", "7d", "3d"]:
        selected_interval = "1h"
    else:  # 1mo or 1y
        selected_interval = "1d"

    bt1 = st.button("Analizi Çalıştır", key="run_analysis")

    if bt1:
        tickers = ["FROTO.IS", "BIMAS.IS", "ASELS.IS", "AKBNK.IS","TUPRS.IS","THYAO.IS","TCELL.IS","YKBNK.IS","ISCTR.IS","SAHOL.IS","KCHOL.IS"]
        with st.spinner("Veriler indiriliyor..."):
            close_df = download_selected_column(
                tickers,
                period=selected_period,
                interval=selected_interval,
                selected_column=selected_column,
                auto_adjust=True,
                batch_size=20,
                pause_s=1.0,
                tries=2,
            )

        if close_df.empty:
            st.warning("Veri çekilemedi. Lütfen daha sonra tekrar deneyin.")
            st.stop()

        close_df = close_df.loc[~(close_df == 0).all(axis=1)]
        returns = get_safe_returns(close_df)

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
                hisse_df = ticker.history(period="1mo", auto_adjust=True, timeout=20)

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
                fiyat_5g = close_prices.pct_change(5, fill_method=None).iloc[-1] * 100
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

# Page 3: BIST30 Para Akışı
elif page == "BIST30 Para Akışı":
    st.title("📊 BIST30 Para Akış Sinyal Terminali")

    # BIST30 hisse listesi
    hisseler = [
        'PETKM.IS',
        'SASA.IS',
        'GUBRF.IS',
        'TCELL.IS',
        'TTKOM.IS',
        'ASTOR.IS',
        'TAVHL.IS',
        'PGSUS.IS',
        'THYAO.IS',
        'BIMAS.IS',
        'MGROS.IS',
        'AKBNK.IS',
        'SAHOL.IS',
        'DSTKF.IS',
        'EKGYO.IS',
        'YKBNK.IS',
        'GARAN.IS',
        'ISCTR.IS',
        'EREGL.IS',
        'TRALT.IS',
        'KRDMD.IS',
        'TUPRS.IS',
        'KCHOL.IS',
        'ENKAI.IS',
        'ASELS.IS',
        'SISE.IS',
        'TOASO.IS',
        'FROTO.IS',
        'AEFES.IS',
        'ULKER.IS'
    ]

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
                hisse_df = ticker.history(period="1mo", auto_adjust=True, timeout=20)

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
                fiyat_5g = close_prices.pct_change(5, fill_method=None).iloc[-1] * 100
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
            dosya_adi = f"bist30_para_akisi_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
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
            Bu uygulama seçili BIST30 hisseleri için son 1 ayda **para akışı sinyali** çıkarır.

            - **GÜÇLÜ GİRİŞ:** Hisse yukarı ve hacim güçlü.
            - **GÜÇLÜ ÇIKIŞ:** Hisse aşağı ve hacim güçlü.
            - **ROTASYON:** Farklı senaryolar.

            Analiz için aşağıdaki 'Analiz Et' butonuna tıklayın.
            '''
        )

# Page 4: Sektörel Analiz
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
                data = yf.download(hisseler, period="1mo", auto_adjust=True, threads=False, progress=False, timeout=20)

                if data.empty:
                    st.warning("Veri çekilemedi. Lütfen daha sonra tekrar deneyin.")
                else:
                    # 2. Getiri ve Hacim Hesaplama
                    returns = data['Close'].pct_change(5, fill_method=None).iloc[-1] * 100
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
                    
                    # Store data in session state for Excel download
                    sektor_ozet_df = sektor_ozet.reset_index().rename(columns={'Sektör Skoru': 'Ortalama Sektör Skoru'})
                    df_sorted = df.sort_values('Sektör Skoru', ascending=False)
                    st.session_state.sektor_ozet_df = sektor_ozet_df
                    st.session_state.sektor_detay_df = df_sorted

                    st.subheader("Sektörel Güç Sıralaması (Para Nereye Gidiyor?)")
                    st.dataframe(
                        sektor_ozet_df,
                        use_container_width=True
                    )
                    
                    # Download sector summary button
                    if 'sektor_ozet_df' in st.session_state:
                        excel_buffer_ozet = io.BytesIO()
                        st.session_state.sektor_ozet_df.to_excel(excel_buffer_ozet, index=False, engine='openpyxl')
                        excel_buffer_ozet.seek(0)
                        
                        st.download_button(
                            label="Sektörel Özet Excel İndir",
                            data=excel_buffer_ozet,
                            file_name=f"sektorel_ozet_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            key="download_ozet"
                        )

                    # 5. Görselleştirme (Barplot)
                    fig, ax = plt.subplots(figsize=(10, 6))
                    sns.barplot(
                        x=sektor_ozet.values,
                        y=sektor_ozet.index,
                        ax=ax
                    )
                    ax.set_title('MSCI Turkey Sektörel Para Giriş Hızı')
                    ax.set_xlabel('Güç Skoru (Fiyat x Hacim)')
                    ax.grid(axis='x', linestyle='--', alpha=0.7)
                    st.pyplot(fig)

                    # Detaylı hisse tablosu
                    st.subheader("Hisse Bazında Detaylı Veriler")
                    st.dataframe(df_sorted, use_container_width=True)
                    
                    # Download detailed stock data button
                    if 'sektor_detay_df' in st.session_state:
                        excel_buffer_detay = io.BytesIO()
                        st.session_state.sektor_detay_df.to_excel(excel_buffer_detay, index=False, engine='openpyxl')
                        excel_buffer_detay.seek(0)
                        
                        st.download_button(
                            label="Hisse Detayları Excel İndir",
                            data=excel_buffer_detay,
                            file_name=f"sektorel_detay_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            key="download_detay"
                        )
            except Exception as e:
                st.error(f"Sektörel analiz sırasında bir hata oluştu: {e}")

# Page 5: BIST30 Hacim Analizi
elif page == "BIST30 Hacim Analizi":
    st.title("📊 BIST30 Hacim Analizi")

    st.write(
        """
        Bu sayfa BIST30 hisseleri için **haftalık getiri ve hacim gücü** analizi yapar.

        - Son 1 ay verisi kullanılır.
        - 5 günlük fiyat getirisi ve 20 günlük ortalama hacim baz alınır.
        - Veriler Hacim Gücü'ne göre azalan sırada gösterilir.
        """
    )

    # BIST30 hisse listesi
    hisseler = [
        'PETKM.IS', 'SASA.IS', 'GUBRF.IS', 'TCELL.IS', 'TTKOM.IS',
        'ASTOR.IS', 'TAVHL.IS', 'PGSUS.IS', 'THYAO.IS', 'BIMAS.IS',
        'MGROS.IS', 'AKBNK.IS', 'SAHOL.IS', 'DSTKF.IS', 'EKGYO.IS',
        'YKBNK.IS', 'GARAN.IS', 'ISCTR.IS', 'EREGL.IS', 'TRALT.IS',
        'KRDMD.IS', 'TUPRS.IS', 'KCHOL.IS', 'ENKAI.IS', 'ASELS.IS',
        'SISE.IS', 'TOASO.IS', 'FROTO.IS', 'AEFES.IS', 'ULKER.IS'
    ]

    if st.button("Hacim Analizini Çalıştır"):
        with st.spinner("Hacim analizi hesaplanıyor..."):
            try:
                # Veri çekimi
                data = yf.download(hisseler, period="1mo", auto_adjust=True, threads=False, progress=False, timeout=20)

                if data.empty:
                    st.warning("Veri çekilemedi. Lütfen daha sonra tekrar deneyin.")
                else:
                    # Getiri ve Hacim Hesaplama
                    returns = data['Close'].pct_change(5, fill_method=None).iloc[-1] * 100
                    volumes = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]
                    current_prices = data['Close'].iloc[-1]

                    # Verileri Birleştirme (Sektör sütunu olmadan)
                    df = pd.DataFrame({
                        'Hisse': returns.index,
                        'Güncel Fiyat': current_prices.values,
                        'Haftalık Getiri %': returns.values,
                        'Hacim Gücü': volumes.values
                    })
                    
                    # Round the price column
                    df['Güncel Fiyat'] = df['Güncel Fiyat'].round(2)

                    # Hacim Gücü'ne göre azalan sırada sırala
                    df_sorted = df.sort_values('Hacim Gücü', ascending=False).reset_index(drop=True)
                    
                    # Store data in session state for Excel download
                    st.session_state.hacim_analiz_df = df_sorted

                    # Hisse tablosu
                    st.subheader("BIST30 Hisse Detayları (Hacim Gücü Sıralaması)")
                    st.dataframe(df_sorted, use_container_width=True)
                    
                    # Download button
                    if 'hacim_analiz_df' in st.session_state:
                        excel_buffer = io.BytesIO()
                        st.session_state.hacim_analiz_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                        excel_buffer.seek(0)
                        
                        st.download_button(
                            label="Hisse Detayları Excel İndir",
                            data=excel_buffer,
                            file_name=f"bist30_hacim_analizi_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            key="download_hacim"
                        )
            except Exception as e:
                st.error(f"Hacim analizi sırasında bir hata oluştu: {e}")

# Page 6: BIST30 Correlation
elif page == "BIST30 Correlation":
    st.title("BIST30 Correlation Analysis")

    # Sektör haritasından hisse listesini al
    tickers = [
        'PETKM.IS',
        'SASA.IS',
        'GUBRF.IS',
        'TCELL.IS',
        'TTKOM.IS',
        'ASTOR.IS',
        'TAVHL.IS',
        'PGSUS.IS',
        'THYAO.IS',
        'BIMAS.IS',
        'MGROS.IS',
        'AKBNK.IS',
        'SAHOL.IS',
        'DSTKF.IS',
        'EKGYO.IS',
        'YKBNK.IS',
        'GARAN.IS',
        'ISCTR.IS',
        'EREGL.IS',
        'TRALT.IS',
        'KRDMD.IS',
        'TUPRS.IS',
        'KCHOL.IS',
        'ENKAI.IS',
        'ASELS.IS',
        'SISE.IS',
        'TOASO.IS',
        'FROTO.IS',
        'AEFES.IS',
        'ULKER.IS'
    ]


    period_options = ["5d", "7d", "3d", "1mo", "1y"]
    selected_period = st.selectbox("Dönem Seçiniz:", options=period_options, key="b30_p")

    selected_column_label = st.selectbox("Veri Türü:", ["Kapanis", "Hacim"], key="b30_c")
    selected_column = "Close" if selected_column_label == "Kapanis" else "Volume"

    selected_interval = "1h" if selected_period in ["5d", "7d", "3d"] else "1d"

    if st.button("BIST30 Korelasyonu Hesapla"):
        with st.spinner("BIST30 verileri indiriliyor..."):
            data = download_selected_column(
                tickers,
                period=selected_period,
                interval=selected_interval,
                selected_column=selected_column,
                auto_adjust=True,
                batch_size=20,
                pause_s=1.0,
                tries=2,
            )

        returns = get_safe_returns(data)

        if not returns.empty:
            corr = returns.corr()
            pairs = []
            cols = corr.columns
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    pairs.append(
                        {
                            "Stock 1": cols[i],
                            "Stock 2": cols[j],
                            "Correlation": round(corr.iloc[i, j], 4),
                        }
                    )

            pairs_df = pd.DataFrame(pairs).sort_values(by="Correlation", ascending=False)
            st.dataframe(pairs_df, use_container_width=True, height=500)

            excel_buffer = io.BytesIO()
            pairs_df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)
            st.download_button(
                "Çiftleri Excel Olarak İndir",
                excel_buffer.getvalue(),
                "bist30_pairs.xlsx",
            )
        else:
            st.warning("BIST30 için seçilen dönem/türde yeterli veri bulunamadı; bazı hisseler indirilememiş olabilir.")

# Page 7: Bist30-Full
elif page == "Bist30-Full":
    st.title("📊 BIST30 Full Analysis")
    
    st.write(
        """
        Bu sayfa tüm BIST30 analizlerini tek bir yerde birleştirir:
        
        - **Korelasyon Analizi:** Hisse korelasyon matrisi ve çiftleri
        - **Para Akışı Analizi:** Para giriş/çıkış sinyalleri
        - **Sektörel Analiz:** Sektör bazında para akış hızı
        - **Hacim Analizi:** Hacim gücü ve getiri analizi
        
        Tüm analizler tek bir butonla çalıştırılır ve sonuçlar Excel veya JSON formatında indirilebilir.
        """
    )
    
    # BIST30 ticker list
    tickers = [
        'PETKM.IS', 'SASA.IS', 'GUBRF.IS', 'TCELL.IS', 'TTKOM.IS',
        'ASTOR.IS', 'TAVHL.IS', 'PGSUS.IS', 'THYAO.IS', 'BIMAS.IS',
        'MGROS.IS', 'AKBNK.IS', 'SAHOL.IS', 'DSTKF.IS', 'EKGYO.IS',
        'YKBNK.IS', 'GARAN.IS', 'ISCTR.IS', 'EREGL.IS', 'TRALT.IS',
        'KRDMD.IS', 'TUPRS.IS', 'KCHOL.IS', 'ENKAI.IS', 'ASELS.IS',
        'SISE.IS', 'TOASO.IS', 'FROTO.IS', 'AEFES.IS', 'ULKER.IS'
    ]
    
    # Sector mapping
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
    
    # Period and column selection for correlation
    period_options = ["5d", "7d", "3d", "1mo", "1y"]
    selected_period = st.selectbox(
        "Dönem Seçiniz (Korelasyon için):",
        options=period_options,
        index=0  # Default to 5d
    )
    
    column_options = {"Kapanis": "Close", "Hacim": "Volume"}
    selected_column_label = st.selectbox(
        "Veri Türü Seçiniz (Korelasyon için):",
        options=list(column_options.keys()),
        index=0
    )
    selected_column = column_options[selected_column_label]
    
    # Determine interval based on period
    if selected_period in ["5d", "7d", "3d"]:
        selected_interval = "1h"
    else:
        selected_interval = "1d"
    
    # Main analysis button
    if st.button("Tüm Analizleri Çalıştır", key="run_full_analysis", type="primary"):
        progress_bar = st.progress(0, text="Analizler başlatılıyor...")
        status_text = st.empty()
        
        try:
            # 1. Correlation Analysis
            status_text.text("1/4: Korelasyon analizi yapılıyor...")
            progress_bar.progress(0.1)
            
            close_df = download_selected_column(
                tickers,
                period=selected_period,
                interval=selected_interval,
                selected_column=selected_column,
                auto_adjust=True,
                batch_size=20,
                pause_s=1.0,
                tries=2,
            )
            close_df = close_df.loc[~(close_df == 0).all(axis=1)]
            returns = close_df.pct_change().dropna()
            corr_matrix = returns.corr()
            
            # Create correlation pairs
            correlation_pairs = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i + 1, len(corr_matrix.columns)):
                    stock1 = corr_matrix.columns[i]
                    stock2 = corr_matrix.columns[j]
                    pair = tuple(sorted([stock1, stock2]))
                    correlation_value = corr_matrix.iloc[i, j]
                    correlation_pairs.append((pair[0], pair[1], correlation_value))
            
            pairs_df = pd.DataFrame(correlation_pairs, columns=['Stock 1', 'Stock 2', 'Correlation'])
            pairs_df['Correlation'] = pairs_df['Correlation'].round(4)
            
            st.session_state.correlation_matrix = corr_matrix
            st.session_state.correlation_pairs = pairs_df
            progress_bar.progress(0.25)
            
            # 2. Para Akisi Analizi
            status_text.text("2/4: Para akışı analizi yapılıyor...")
            progress_bar.progress(0.35)
            
            @st.cache_data(show_spinner=False)
            def hisse_verisi_cek(hisse, max_deneme=3, bekleme_suresi=2):
                for deneme in range(max_deneme):
                    try:
                        if deneme > 0:
                            time.sleep(bekleme_suresi * deneme)
                        ticker = yf.Ticker(hisse)
                        hisse_df = ticker.history(period="1mo", auto_adjust=True, timeout=20)
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
            
            analiz_listesi = []
            for idx, hisse in enumerate(tickers):
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
                    time.sleep(0.1)
                except Exception:
                    continue
            
            para_akisi_df = pd.DataFrame(analiz_listesi).sort_values(by='Skor', ascending=False)
            st.session_state.para_akisi_df = para_akisi_df
            progress_bar.progress(0.5)
            
            # 3. Sektorel Analiz
            status_text.text("3/4: Sektörel analiz yapılıyor...")
            progress_bar.progress(0.6)
            
            data = yf.download(tickers, period="1mo", auto_adjust=True, threads=False, progress=False, timeout=20)
            if not data.empty:
                returns = data['Close'].pct_change(5).iloc[-1] * 100
                volumes = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]
                
                df_sektorel = pd.DataFrame({
                    'Hisse': returns.index,
                    'Sektör': [sektor_haritasi.get(h, 'Bilinmeyen') for h in returns.index],
                    'Haftalık Getiri %': returns.values,
                    'Hacim Gücü': volumes.values
                })
                
                df_sektorel['Sektör Skoru'] = df_sektorel['Haftalık Getiri %'] * df_sektorel['Hacim Gücü']
                sektor_ozet = df_sektorel.groupby('Sektör')['Sektör Skoru'].mean().sort_values(ascending=False)
                sektor_ozet_df = sektor_ozet.reset_index().rename(columns={'Sektör Skoru': 'Ortalama Sektör Skoru'})
                sektor_detay_df = df_sektorel.sort_values('Sektör Skoru', ascending=False)
                
                st.session_state.sektor_ozet_df = sektor_ozet_df
                st.session_state.sektor_detay_df = sektor_detay_df
            else:
                st.session_state.sektor_ozet_df = pd.DataFrame()
                st.session_state.sektor_detay_df = pd.DataFrame()
            
            progress_bar.progress(0.75)
            
            # 4. Hacim Analizi
            status_text.text("4/4: Hacim analizi yapılıyor...")
            progress_bar.progress(0.85)
            
            if not data.empty:
                returns_hacim = data['Close'].pct_change(5).iloc[-1] * 100
                volumes_hacim = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]
                current_prices = data['Close'].iloc[-1]
                
                hacim_df = pd.DataFrame({
                    'Hisse': returns_hacim.index,
                    'Güncel Fiyat': current_prices.values,
                    'Haftalık Getiri %': returns_hacim.values,
                    'Hacim Gücü': volumes_hacim.values
                })
                hacim_df['Güncel Fiyat'] = hacim_df['Güncel Fiyat'].round(2)
                hacim_df_sorted = hacim_df.sort_values('Hacim Gücü', ascending=False).reset_index(drop=True)
                
                st.session_state.hacim_analiz_df = hacim_df_sorted
            else:
                st.session_state.hacim_analiz_df = pd.DataFrame()
            
            progress_bar.progress(1.0)
            status_text.text("✅ Tüm analizler tamamlandı!")
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            
            st.success("✅ Tüm analizler başarıyla tamamlandı!")
            
            # Store metadata
            st.session_state.analysis_metadata = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'period': selected_period,
                'column_type': selected_column_label
            }
            
        except Exception as e:
            st.error(f"Analiz sırasında bir hata oluştu: {e}")
            progress_bar.empty()
            status_text.empty()
    
    # Display results if available
    if 'correlation_matrix' in st.session_state:
        with st.expander("📊 Korelasyon Analizi", expanded=False):
            st.subheader("Korelasyon Matrisi")
            st.dataframe(st.session_state.correlation_matrix, use_container_width=True)
            st.subheader("Korelasyon Çiftleri")
            st.dataframe(st.session_state.correlation_pairs, use_container_width=True, height=300)
    
    if 'para_akisi_df' in st.session_state and not st.session_state.para_akisi_df.empty:
        with st.expander("💰 Para Akışı Analizi", expanded=False):
            st.dataframe(st.session_state.para_akisi_df, use_container_width=True)
    
    if 'sektor_ozet_df' in st.session_state and not st.session_state.sektor_ozet_df.empty:
        with st.expander("🏭 Sektörel Analiz", expanded=False):
            st.subheader("Sektörel Özet")
            st.dataframe(st.session_state.sektor_ozet_df, use_container_width=True)
            st.subheader("Hisse Detayları")
            st.dataframe(st.session_state.sektor_detay_df, use_container_width=True)
    
    if 'hacim_analiz_df' in st.session_state and not st.session_state.hacim_analiz_df.empty:
        with st.expander("📈 Hacim Analizi", expanded=False):
            st.dataframe(st.session_state.hacim_analiz_df, use_container_width=True)
    
    # Export buttons
    if 'correlation_matrix' in st.session_state:
        st.subheader("📥 Veri Dışa Aktarım")
        col1, col2 = st.columns(2)
        
        with col1:
            # Excel Export
            if st.button("📥 Excel Dosyası Oluştur", key="export_excel"):
                try:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        # Sheet 1: Correlation Matrix
                        st.session_state.correlation_matrix.to_excel(writer, sheet_name='Correlation Matrix', index=True)
                        
                        # Sheet 2: Correlation Pairs
                        st.session_state.correlation_pairs.to_excel(writer, sheet_name='Correlation Pairs', index=False)
                        
                        # Sheet 3: Para Akisi
                        if 'para_akisi_df' in st.session_state and not st.session_state.para_akisi_df.empty:
                            st.session_state.para_akisi_df.to_excel(writer, sheet_name='Para Akisi', index=False)
                        
                        # Sheet 4: Sektorel Ozet
                        if 'sektor_ozet_df' in st.session_state and not st.session_state.sektor_ozet_df.empty:
                            st.session_state.sektor_ozet_df.to_excel(writer, sheet_name='Sektorel Ozet', index=False)
                        
                        # Sheet 5: Sektorel Detay
                        if 'sektor_detay_df' in st.session_state and not st.session_state.sektor_detay_df.empty:
                            st.session_state.sektor_detay_df.to_excel(writer, sheet_name='Sektorel Detay', index=False)
                        
                        # Sheet 6: Hacim Analizi
                        if 'hacim_analiz_df' in st.session_state and not st.session_state.hacim_analiz_df.empty:
                            st.session_state.hacim_analiz_df.to_excel(writer, sheet_name='Hacim Analizi', index=False)
                    
                    excel_buffer.seek(0)
                    st.session_state.excel_buffer = excel_buffer.getvalue()
                    st.success("✅ Excel dosyası hazır! İndir butonuna tıklayın.")
                except Exception as e:
                    st.error(f"Excel dosyası oluşturulurken hata: {e}")
            
            if 'excel_buffer' in st.session_state:
                st.download_button(
                    label="📥 Excel Dosyasını İndir",
                    data=st.session_state.excel_buffer,
                    file_name=f"BIST30_Full_Analysis_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    key="download_excel_full"
                )
        
        with col2:
            # JSON Export
            if st.button("📄 JSON Dosyası Oluştur", key="export_json"):
                try:
                    json_data = {
                        "metadata": st.session_state.get('analysis_metadata', {}),
                        "correlation": {
                            "matrix": st.session_state.correlation_matrix.to_dict() if 'correlation_matrix' in st.session_state else {},
                            "pairs": st.session_state.correlation_pairs.to_dict('records') if 'correlation_pairs' in st.session_state else []
                        },
                        "para_akisi": st.session_state.para_akisi_df.to_dict('records') if 'para_akisi_df' in st.session_state and not st.session_state.para_akisi_df.empty else [],
                        "sektorel": {
                            "ozet": st.session_state.sektor_ozet_df.to_dict('records') if 'sektor_ozet_df' in st.session_state and not st.session_state.sektor_ozet_df.empty else [],
                            "detay": st.session_state.sektor_detay_df.to_dict('records') if 'sektor_detay_df' in st.session_state and not st.session_state.sektor_detay_df.empty else []
                        },
                        "hacim_analizi": st.session_state.hacim_analiz_df.to_dict('records') if 'hacim_analiz_df' in st.session_state and not st.session_state.hacim_analiz_df.empty else []
                    }
                    
                    json_str = json.dumps(json_data, indent=2, ensure_ascii=False, default=str)
                    json_bytes = json_str.encode('utf-8')
                    st.session_state.json_bytes = json_bytes
                    st.success("✅ JSON dosyası hazır! İndir butonuna tıklayın.")
                except Exception as e:
                    st.error(f"JSON dosyası oluşturulurken hata: {e}")
            
            if 'json_bytes' in st.session_state:
                st.download_button(
                    label="📄 JSON Dosyasını İndir",
                    data=st.session_state.json_bytes,
                    file_name=f"BIST30_Full_Analysis_{datetime.now().strftime('%Y-%m-%d')}.json",
                    mime='application/json',
                    key="download_json_full"
                )

# Page 8: Kontrat-Tum
elif page == "Kontrat-Tum":
    st.title("📊 Kontrat-Tum Full Analysis")
    
    st.write(
        """
        Bu sayfa tüm kontrat hisseleri için analizlerini tek bir yerde birleştirir:
        
        - **Korelasyon Analizi:** Hisse korelasyon matrisi ve çiftleri
        - **Para Akışı Analizi:** Para giriş/çıkış sinyalleri
        - **Sektörel Analiz:** Sektör bazında para akış hızı
        - **Hacim Analizi:** Hacim gücü ve getiri analizi
        
        Tüm analizler tek bir butonla çalıştırılır ve sonuçlar Excel veya JSON formatında indirilebilir.
        """
    )
    
    # Kontrat ticker list (without .IS suffix, will add when fetching)
    tickers_base = [
        'AEFES', 'AKBNK', 'AKSEN', 'ALARK', 'ARCLK', 'ASELS', 'ASTOR', 
        'BIMAS', 'BRSAN', 'CIMSA', 'DOAS', 'DOHOL', 'EKGYO', 'ENJSA', 
        'ENKAI', 'EREGL', 'FROTO', 'GARAN', 'GUBRF', 'HALKB', 'HEKTS', 
        'ISCTR', 'KCHOL', 'KONTR', 'KRDMD', 'MGROS', 'ODAS', 'OYAKC', 
        'PETKM', 'PGSUS', 'SAHOL', 'SASA', 'SISE', 'SOKM', 'TAVHL', 
        'TCELL', 'THYAO', 'TKFEN', 'TOASO', 'TRALT', 'TRMET', 'TSKB', 
        'TTKOM', 'TUPRS', 'ULKER', 'VAKBN', 'VESTL', 'YKBNK'
    ]
    
    # Add .IS suffix for yfinance
    tickers = [t + '.IS' for t in tickers_base]
    
    # Sector mapping
    sektor_haritasi = {
        # İşlenebilen endüstriler (Process Industries / Chemicals & Materials)
        'PETKM.IS': 'İşlenebilen endüstriler',
        'SASA.IS': 'İşlenebilen endüstriler',
        'GUBRF.IS': 'İşlenebilen endüstriler',
        'OYAKC.IS': 'İşlenebilen endüstriler',
        'CIMSA.IS': 'İşlenebilen endüstriler',

        # İletişim (Communications)
        'TCELL.IS': 'İletişim',
        'TTKOM.IS': 'İletişim',

        # Üretici imalatı (Producer Manufacturing / Capital Goods)
        'ASTOR.IS': 'Üretici imalatı',
        'KONTR.IS': 'Üretici imalatı',
        'TKFEN.IS': 'Üretici imalatı',
        'BRSAN.IS': 'Üretici imalatı',

        # Taşımacılık (Transportation)
        'TAVHL.IS': 'Taşımacılık',
        'PGSUS.IS': 'Taşımacılık',
        'THYAO.IS': 'Taşımacılık',

        # Perakende satış (Retail Trade)
        'BIMAS.IS': 'Perakende satış',
        'MGROS.IS': 'Perakende satış',
        'SOKM.IS': 'Perakende satış',
        'DOAS.IS': 'Perakende satış',

        # Finans (Finance / Banking / Holding)
        'AKBNK.IS': 'Finans',
        'SAHOL.IS': 'Finans',
        'EKGYO.IS': 'Finans',
        'YKBNK.IS': 'Finans',
        'GARAN.IS': 'Finans',
        'ISCTR.IS': 'Finans',
        'HALKB.IS': 'Finans',
        'VAKBN.IS': 'Finans',
        'TSKB.IS': 'Finans',
        'ALARK.IS': 'Finans',
        'DOHOL.IS': 'Finans',

        # Enerji-dışı mineraller (Non-Energy Minerals / Metals)
        'EREGL.IS': 'Enerji-dışı mineraller',
        'TRALT.IS': 'Enerji-dışı mineraller',
        'KRDMD.IS': 'Enerji-dışı mineraller',
        'TRMET.IS': 'Enerji-dışı mineraller',

        # Enerji mineralleri (Energy Minerals / Oil & Gas)
        'TUPRS.IS': 'Enerji mineralleri',
        'KCHOL.IS': 'Enerji mineralleri',

        # Endüstriyel hizmetler (Industrial Services)
        'ENKAI.IS': 'Endüstriyel hizmetler',
        'AKSEN.IS': 'Endüstriyel hizmetler',
        'ENJSA.IS': 'Endüstriyel hizmetler',
        'ODAS.IS': 'Endüstriyel hizmetler',

        # Elektronik teknoloji (Electronic Technology / Defense)
        'ASELS.IS': 'Elektronik teknoloji',

        # Dayanıklı tüketim malları (Consumer Durables)
        'SISE.IS': 'Dayanıklı tüketim malları',
        'TOASO.IS': 'Dayanıklı tüketim malları',
        'FROTO.IS': 'Dayanıklı tüketim malları',
        'ARCLK.IS': 'Dayanıklı tüketim malları',
        'VESTL.IS': 'Dayanıklı tüketim malları',

        # Dayanıklı olmayan tüketici ürünleri (Consumer Non-Durables)
        'AEFES.IS': 'Dayanıklı olmayan tüketici ürünleri',
        'ULKER.IS': 'Dayanıklı olmayan tüketici ürünleri',
        'HEKTS.IS': 'Dayanıklı olmayan tüketici ürünleri'
    }
    
    # Period and column selection for correlation
    period_options = ["5d", "7d", "3d", "1mo", "1y"]
    selected_period = st.selectbox(
        "Dönem Seçiniz (Korelasyon için):",
        options=period_options,
        index=0  # Default to 5d
    )
    
    column_options = {"Kapanis": "Close", "Hacim": "Volume"}
    selected_column_label = st.selectbox(
        "Veri Türü Seçiniz (Korelasyon için):",
        options=list(column_options.keys()),
        index=0
    )
    selected_column = column_options[selected_column_label]
    
    # Determine interval based on period
    if selected_period in ["5d", "7d", "3d"]:
        selected_interval = "1h"
    else:
        selected_interval = "1d"
    
    # Main analysis button
    if st.button("Tüm Analizleri Çalıştır", key="run_full_analysis_kontrat", type="primary"):
        progress_bar = st.progress(0, text="Analizler başlatılıyor...")
        status_text = st.empty()
        
        try:
            # 1. Correlation Analysis
            status_text.text("1/4: Korelasyon analizi yapılıyor...")
            progress_bar.progress(0.1)
            
            close_df = download_selected_column(
                tickers,
                period=selected_period,
                interval=selected_interval,
                selected_column=selected_column,
                auto_adjust=True,
                batch_size=20,
                pause_s=1.0,
                tries=2,
            )
            close_df = close_df.loc[~(close_df == 0).all(axis=1)]
            returns = close_df.pct_change().dropna()
            corr_matrix = returns.corr()
            
            # Create correlation pairs
            correlation_pairs = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i + 1, len(corr_matrix.columns)):
                    stock1 = corr_matrix.columns[i]
                    stock2 = corr_matrix.columns[j]
                    pair = tuple(sorted([stock1, stock2]))
                    correlation_value = corr_matrix.iloc[i, j]
                    correlation_pairs.append((pair[0], pair[1], correlation_value))
            
            pairs_df = pd.DataFrame(correlation_pairs, columns=['Stock 1', 'Stock 2', 'Correlation'])
            pairs_df['Correlation'] = pairs_df['Correlation'].round(4)
            
            st.session_state.kontrat_correlation_matrix = corr_matrix
            st.session_state.kontrat_correlation_pairs = pairs_df
            progress_bar.progress(0.25)
            
            # 2. Para Akisi Analizi
            status_text.text("2/4: Para akışı analizi yapılıyor...")
            progress_bar.progress(0.35)
            
            @st.cache_data(show_spinner=False)
            def hisse_verisi_cek_kontrat(hisse, max_deneme=3, bekleme_suresi=2):
                for deneme in range(max_deneme):
                    try:
                        if deneme > 0:
                            time.sleep(bekleme_suresi * deneme)
                        ticker = yf.Ticker(hisse)
                        hisse_df = ticker.history(period="1mo", auto_adjust=True, timeout=20)
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
            
            analiz_listesi = []
            for idx, hisse in enumerate(tickers):
                hisse_df = hisse_verisi_cek_kontrat(hisse)
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
                    time.sleep(0.1)
                except Exception:
                    continue
            
            para_akisi_df = pd.DataFrame(analiz_listesi).sort_values(by='Skor', ascending=False)
            st.session_state.kontrat_para_akisi_df = para_akisi_df
            progress_bar.progress(0.5)
            
            # 3. Sektorel Analiz
            status_text.text("3/4: Sektörel analiz yapılıyor...")
            progress_bar.progress(0.6)
            
            data = yf.download(tickers, period="1mo", auto_adjust=True, threads=False, progress=False, timeout=20)
            if not data.empty:
                returns = data['Close'].pct_change(5).iloc[-1] * 100
                volumes = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]
                
                df_sektorel = pd.DataFrame({
                    'Hisse': returns.index,
                    'Sektör': [sektor_haritasi.get(h, 'Bilinmeyen') for h in returns.index],
                    'Haftalık Getiri %': returns.values,
                    'Hacim Gücü': volumes.values
                })
                
                df_sektorel['Sektör Skoru'] = df_sektorel['Haftalık Getiri %'] * df_sektorel['Hacim Gücü']
                sektor_ozet = df_sektorel.groupby('Sektör')['Sektör Skoru'].mean().sort_values(ascending=False)
                sektor_ozet_df = sektor_ozet.reset_index().rename(columns={'Sektör Skoru': 'Ortalama Sektör Skoru'})
                sektor_detay_df = df_sektorel.sort_values('Sektör Skoru', ascending=False)
                
                st.session_state.kontrat_sektor_ozet_df = sektor_ozet_df
                st.session_state.kontrat_sektor_detay_df = sektor_detay_df
            else:
                st.session_state.kontrat_sektor_ozet_df = pd.DataFrame()
                st.session_state.kontrat_sektor_detay_df = pd.DataFrame()
            
            progress_bar.progress(0.75)
            
            # 4. Hacim Analizi
            status_text.text("4/4: Hacim analizi yapılıyor...")
            progress_bar.progress(0.85)
            
            if not data.empty:
                returns_hacim = data['Close'].pct_change(5).iloc[-1] * 100
                volumes_hacim = data['Volume'].iloc[-1] / data['Volume'].rolling(20).mean().iloc[-1]
                current_prices = data['Close'].iloc[-1]
                
                hacim_df = pd.DataFrame({
                    'Hisse': returns_hacim.index,
                    'Güncel Fiyat': current_prices.values,
                    'Haftalık Getiri %': returns_hacim.values,
                    'Hacim Gücü': volumes_hacim.values
                })
                hacim_df['Güncel Fiyat'] = hacim_df['Güncel Fiyat'].round(2)
                hacim_df_sorted = hacim_df.sort_values('Hacim Gücü', ascending=False).reset_index(drop=True)
                
                st.session_state.kontrat_hacim_analiz_df = hacim_df_sorted
            else:
                st.session_state.kontrat_hacim_analiz_df = pd.DataFrame()
            
            progress_bar.progress(1.0)
            status_text.text("✅ Tüm analizler tamamlandı!")
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            
            st.success("✅ Tüm analizler başarıyla tamamlandı!")
            
            # Store metadata
            st.session_state.kontrat_analysis_metadata = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'period': selected_period,
                'column_type': selected_column_label
            }
            
        except Exception as e:
            st.error(f"Analiz sırasında bir hata oluştu: {e}")
            progress_bar.empty()
            status_text.empty()
    
    # Display results if available
    if 'kontrat_correlation_matrix' in st.session_state:
        with st.expander("📊 Korelasyon Analizi", expanded=False):
            st.subheader("Korelasyon Matrisi")
            st.dataframe(st.session_state.kontrat_correlation_matrix, use_container_width=True)
            st.subheader("Korelasyon Çiftleri")
            st.dataframe(st.session_state.kontrat_correlation_pairs, use_container_width=True, height=300)
    
    if 'kontrat_para_akisi_df' in st.session_state and not st.session_state.kontrat_para_akisi_df.empty:
        with st.expander("💰 Para Akışı Analizi", expanded=False):
            st.dataframe(st.session_state.kontrat_para_akisi_df, use_container_width=True)
    
    if 'kontrat_sektor_ozet_df' in st.session_state and not st.session_state.kontrat_sektor_ozet_df.empty:
        with st.expander("🏭 Sektörel Analiz", expanded=False):
            st.subheader("Sektörel Özet")
            st.dataframe(st.session_state.kontrat_sektor_ozet_df, use_container_width=True)
            st.subheader("Hisse Detayları")
            st.dataframe(st.session_state.kontrat_sektor_detay_df, use_container_width=True)
    
    if 'kontrat_hacim_analiz_df' in st.session_state and not st.session_state.kontrat_hacim_analiz_df.empty:
        with st.expander("📈 Hacim Analizi", expanded=False):
            st.dataframe(st.session_state.kontrat_hacim_analiz_df, use_container_width=True)
    
    # Export buttons
    if 'kontrat_correlation_matrix' in st.session_state:
        st.subheader("📥 Veri Dışa Aktarım")
        col1, col2 = st.columns(2)
        
        with col1:
            # Excel Export
            if st.button("📥 Excel Dosyası Oluştur", key="export_excel_kontrat"):
                try:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        # Sheet 1: Correlation Matrix
                        st.session_state.kontrat_correlation_matrix.to_excel(writer, sheet_name='Correlation Matrix', index=True)
                        
                        # Sheet 2: Correlation Pairs
                        st.session_state.kontrat_correlation_pairs.to_excel(writer, sheet_name='Correlation Pairs', index=False)
                        
                        # Sheet 3: Para Akisi
                        if 'kontrat_para_akisi_df' in st.session_state and not st.session_state.kontrat_para_akisi_df.empty:
                            st.session_state.kontrat_para_akisi_df.to_excel(writer, sheet_name='Para Akisi', index=False)
                        
                        # Sheet 4: Sektorel Ozet
                        if 'kontrat_sektor_ozet_df' in st.session_state and not st.session_state.kontrat_sektor_ozet_df.empty:
                            st.session_state.kontrat_sektor_ozet_df.to_excel(writer, sheet_name='Sektorel Ozet', index=False)
                        
                        # Sheet 5: Sektorel Detay
                        if 'kontrat_sektor_detay_df' in st.session_state and not st.session_state.kontrat_sektor_detay_df.empty:
                            st.session_state.kontrat_sektor_detay_df.to_excel(writer, sheet_name='Sektorel Detay', index=False)
                        
                        # Sheet 6: Hacim Analizi
                        if 'kontrat_hacim_analiz_df' in st.session_state and not st.session_state.kontrat_hacim_analiz_df.empty:
                            st.session_state.kontrat_hacim_analiz_df.to_excel(writer, sheet_name='Hacim Analizi', index=False)
                    
                    excel_buffer.seek(0)
                    st.session_state.kontrat_excel_buffer = excel_buffer.getvalue()
                    st.success("✅ Excel dosyası hazır! İndir butonuna tıklayın.")
                except Exception as e:
                    st.error(f"Excel dosyası oluşturulurken hata: {e}")
            
            if 'kontrat_excel_buffer' in st.session_state:
                st.download_button(
                    label="📥 Excel Dosyasını İndir",
                    data=st.session_state.kontrat_excel_buffer,
                    file_name=f"Kontrat_Tum_Analysis_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    key="download_excel_kontrat"
                )
        
        with col2:
            # JSON Export
            if st.button("📄 JSON Dosyası Oluştur", key="export_json_kontrat"):
                try:
                    json_data = {
                        "metadata": st.session_state.get('kontrat_analysis_metadata', {}),
                        "correlation": {
                            "matrix": st.session_state.kontrat_correlation_matrix.to_dict() if 'kontrat_correlation_matrix' in st.session_state else {},
                            "pairs": st.session_state.kontrat_correlation_pairs.to_dict('records') if 'kontrat_correlation_pairs' in st.session_state else []
                        },
                        "para_akisi": st.session_state.kontrat_para_akisi_df.to_dict('records') if 'kontrat_para_akisi_df' in st.session_state and not st.session_state.kontrat_para_akisi_df.empty else [],
                        "sektorel": {
                            "ozet": st.session_state.kontrat_sektor_ozet_df.to_dict('records') if 'kontrat_sektor_ozet_df' in st.session_state and not st.session_state.kontrat_sektor_ozet_df.empty else [],
                            "detay": st.session_state.kontrat_sektor_detay_df.to_dict('records') if 'kontrat_sektor_detay_df' in st.session_state and not st.session_state.kontrat_sektor_detay_df.empty else []
                        },
                        "hacim_analizi": st.session_state.kontrat_hacim_analiz_df.to_dict('records') if 'kontrat_hacim_analiz_df' in st.session_state and not st.session_state.kontrat_hacim_analiz_df.empty else []
                    }
                    
                    json_str = json.dumps(json_data, indent=2, ensure_ascii=False, default=str)
                    json_bytes = json_str.encode('utf-8')
                    st.session_state.kontrat_json_bytes = json_bytes
                    st.success("✅ JSON dosyası hazır! İndir butonuna tıklayın.")
                except Exception as e:
                    st.error(f"JSON dosyası oluşturulurken hata: {e}")
            
            if 'kontrat_json_bytes' in st.session_state:
                st.download_button(
                    label="📄 JSON Dosyasını İndir",
                    data=st.session_state.kontrat_json_bytes,
                    file_name=f"Kontrat_Tum_Analysis_{datetime.now().strftime('%Y-%m-%d')}.json",
                    mime='application/json',
                    key="download_json_kontrat"
                )
