import streamlit as st
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import io

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
            df = yf.Ticker(tick).history(period="3d",interval="1h")
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

