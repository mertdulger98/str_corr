import streamlit as st
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import io
import json
from datetime import datetime

# Page configuration
st.set_page_config(page_title="BIST Analysis App", layout="wide")

# --- Helper Functions ---
def get_safe_returns(df):
    """Safely calculates returns and handles empty data bugs."""
    if df is None or df.empty:
        return pd.DataFrame()
    # Drop columns that are all NaN or all 0
    df = df.loc[:, (df != 0).any(axis=0)].dropna(axis=1, how='all')
    returns = df.pct_change().dropna(how='all')
    return returns

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Sayfa Seçiniz:",
    ["BIST Data Analysis", "MSCI Para Akışı Analizi", "BIST30 Para Akışı", "Sektörel Analiz", "BIST30 Hacim Analizi", "BIST30 Correlation", "Bist30-Full", "Kontrat-Tum"]
)

# Shared Tickers and Mappings
bist30_tickers = [
    'PETKM.IS', 'SASA.IS', 'GUBRF.IS', 'TCELL.IS', 'TTKOM.IS', 'ASTOR.IS', 'TAVHL.IS', 'PGSUS.IS', 
    'THYAO.IS', 'BIMAS.IS', 'MGROS.IS', 'AKBNK.IS', 'SAHOL.IS', 'EKGYO.IS', 'YKBNK.IS', 'GARAN.IS', 
    'ISCTR.IS', 'EREGL.IS', 'KRDMD.IS', 'TUPRS.IS', 'KCHOL.IS', 'ENKAI.IS', 'ASELS.IS', 'SISE.IS', 
    'TOASO.IS', 'FROTO.IS', 'AEFES.IS', 'ULKER.IS'
]

sektor_haritasi = {
    'PETKM.IS': 'İşlenebilen endüstriler', 'SASA.IS': 'İşlenebilen endüstriler', 'GUBRF.IS': 'İşlenebilen endüstriler',
    'TCELL.IS': 'İletişim', 'TTKOM.IS': 'İletişim', 'ASTOR.IS': 'Üretici imalatı', 'TAVHL.IS': 'Taşımacılık',
    'PGSUS.IS': 'Taşımacılık', 'THYAO.IS': 'Taşımacılık', 'BIMAS.IS': 'Perakende satış', 'MGROS.IS': 'Perakende satış',
    'AKBNK.IS': 'Finans', 'SAHOL.IS': 'Finans', 'EKGYO.IS': 'Finans', 'YKBNK.IS': 'Finans', 'GARAN.IS': 'Finans',
    'ISCTR.IS': 'Finans', 'EREGL.IS': 'Enerji-dışı mineraller', 'KRDMD.IS': 'Enerji-dışı mineraller',
    'TUPRS.IS': 'Enerji mineralleri', 'KCHOL.IS': 'Enerji mineralleri', 'ENKAI.IS': 'Endüstriyel hizmetler',
    'ASELS.IS': 'Elektronik teknoloji', 'SISE.IS': 'Dayanıklı tüketim malları', 'TOASO.IS': 'Dayanıklı tüketim malları',
    'FROTO.IS': 'Dayanıklı tüketim malları', 'AEFES.IS': 'Dayanıklı olmayan tüketici ürünleri', 'ULKER.IS': 'Dayanıklı olmayan tüketici ürünleri'
}

# --- Page Logic ---

if page == "BIST Data Analysis":
    st.title("BIST Data Analysis")
    
    period_options = ["3d", "7d", "1mo", "1y"]
    selected_period = st.selectbox("Dönem Seçiniz:", options=period_options)
    column_options = {"Kapanis": "Close", "Hacim": "Volume"}
    selected_column_label = st.selectbox("Veri Türü Seçiniz:", options=list(column_options.keys()))
    selected_column = column_options[selected_column_label]
    selected_interval = "1h" if selected_period in ["3d", "7d"] else "1d"

    if st.button("Analizi Çalıştır"):
        tickers = ["FROTO.IS", "BIMAS.IS", "ASELS.IS", "AKBNK.IS","TUPRS.IS","THYAO.IS","TCELL.IS","YKBNK.IS","ISCTR.IS","SAHOL.IS","KCHOL.IS"]
        with st.spinner("Veriler Çekiliyor..."):
            data = yf.download(tickers, period=selected_period, interval=selected_interval, group_by='column')[selected_column]
            
        returns = get_safe_returns(data)
        
        if not returns.empty and returns.shape[1] > 1:
            corr = returns.corr()
            fig, ax = plt.subplots(figsize=(9, 6))
            sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1, ax=ax)
            ax.set_title(f"Correlation Matrix {selected_column_label}-{selected_period}")
            st.pyplot(fig)
            plt.close(fig)

            excel_buffer = io.BytesIO()
            corr.to_excel(excel_buffer)
            st.download_button("Excel İndir", excel_buffer.getvalue(), f"{selected_column_label}_corr.xlsx")
        else:
            st.error("Yetersiz veri. Lütfen farklı bir dönem seçin veya piyasanın açık olduğundan emin olun.")

elif page == "BIST30 Correlation":
    st.title("BIST30 Correlation Analysis")
    
    period_options = ["3d", "7d", "1mo", "1y"]
    selected_period = st.selectbox("Dönem Seçiniz:", options=period_options, key="b30_p")
    selected_column_label = st.selectbox("Veri Türü:", ["Kapanis", "Hacim"], key="b30_c")
    selected_column = "Close" if selected_column_label == "Kapanis" else "Volume"
    selected_interval = "1h" if selected_period in ["3d", "7d"] else "1d"

    if st.button("BIST30 Korelasyonu Hesapla"):
        with st.spinner("BIST30 verileri indiriliyor..."):
            data = yf.download(bist30_tickers, period=selected_period, interval=selected_interval)[selected_column]
            
        returns = get_safe_returns(data)
        
        if not returns.empty:
            corr = returns.corr()
            # Convert to pairs
            pairs = []
            cols = corr.columns
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    pairs.append({'Stock 1': cols[i], 'Stock 2': cols[j], 'Correlation': round(corr.iloc[i, j], 4)})
            
            pairs_df = pd.DataFrame(pairs).sort_values(by='Correlation', ascending=False)
            st.dataframe(pairs_df, use_container_width=True, height=500)
            
            # Download
            excel_buffer = io.BytesIO()
            pairs_df.to_excel(excel_buffer, index=False)
            st.download_button("Çiftleri Excel Olarak İndir", excel_buffer.getvalue(), "bist30_pairs.xlsx")
        else:
            st.warning("Veri bulunamadı.")

elif page == "Sektörel Analiz":
    st.title("📊 Sektörel Para Giriş Hızı")
    if st.button("Sektörel Analizi Başlat"):
        with st.spinner("Veriler indiriliyor..."):
            data = yf.download(list(sektor_haritasi.keys()), period="1mo")
            
        if not data.empty:
            # 5-day return and Volume strength (Volume / 20d Avg Volume)
            returns_5g = data['Close'].pct_change(5).iloc[-1] * 100
            vol_avg_20 = data['Volume'].rolling(20).mean().iloc[-1]
            vol_strength = data['Volume'].iloc[-1] / vol_avg_20
            
            df = pd.DataFrame({
                'Hisse': returns_5g.index,
                'Sektör': [sektor_haritasi.get(h, 'Diğer') for h in returns_5g.index],
                'Haftalık Getiri %': returns_5g.values,
                'Hacim Gücü': vol_strength.values
            }).dropna()
            
            df['Sektör Skoru'] = df['Haftalık Getiri %'] * df['Hacim Gücü']
            sektor_ozet = df.groupby('Sektör')['Sektör Skoru'].mean().sort_values(ascending=False).reset_index()
            
            st.subheader("Sektörel Güç Sıralaması")
            st.dataframe(sektor_ozet, use_container_width=True)
            
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.barplot(data=sektor_ozet, x='Sektör Skoru', y='Sektör', palette='RdYlGn', ax=ax)
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.error("Veri çekilemedi.")

elif page == "Bist30-Full":
    st.title("📊 BIST30 Full Analysis Terminal")
    st.info("Bu sayfa Korelasyon, Para Akışı ve Hacim analizlerini tek seferde yapar.")
    
    if st.button("TÜM ANALİZLERİ ÇALIŞTIR", type="primary"):
        with st.spinner("Kapsamlı BIST30 analizi yapılıyor (Yaklaşık 10 saniye)..."):
            # Fetch all data at once for speed
            full_data = yf.download(bist30_tickers, period="1mo")
            
            if full_data.empty:
                st.error("Veri alınamadı.")
            else:
                # 1. Correlation
                returns = get_safe_returns(full_data['Close'])
                corr = returns.corr()
                
                # 2. Money Flow & Volume
                r5 = full_data['Close'].pct_change(5).iloc[-1] * 100
                v_strength = full_data['Volume'].iloc[-1] / full_data['Volume'].rolling(20).mean().iloc[-1]
                
                flow_list = []
                for ticker in bist30_tickers:
                    f5 = r5.get(ticker, 0)
                    vs = v_strength.get(ticker, 0)
                    status = "GÜÇLÜ GİRİŞ" if f5 > 0 and vs > 1.2 else ("GÜÇLÜ ÇIKIŞ" if f5 < 0 and vs > 1.2 else "ROTASYON")
                    flow_list.append({'Hisse': ticker, '5G Getiri %': round(f5, 2), 'Hacim Gücü': round(vs, 2), 'Sinyal': status})
                
                flow_df = pd.DataFrame(flow_list).sort_values(by='Hacim Gücü', ascending=False)
                
                # Layout
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Para Akışı ve Hacim")
                    st.dataframe(flow_df, use_container_width=True)
                with col2:
                    st.subheader("Korelasyon Isı Haritası")
                    fig, ax = plt.subplots()
                    sns.heatmap(corr, cmap="viridis", ax=ax)
                    st.pyplot(fig)
                    plt.close(fig)
                
                # Export all to Excel with sheets
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    flow_df.to_excel(writer, sheet_name='Para Akisi', index=False)
                    corr.to_excel(writer, sheet_name='Korelasyon')
                st.download_button("Full Raporu İndir (Excel)", output.getvalue(), "BIST30_Full_Report.xlsx")

# Handle other pages similarly using the yf.download logic...
else:
    st.write("Bu sayfa geliştirme aşamasındadır veya benzer mantıkla kurgulanmıştır.")
