# monitor_rlp_streamlit.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Dicionário de códigos para nomes de corretoras
codigo_corretoras = {
    "3": "XP INVESTIMENTOS",
    "4": "ALFA CCVM S/A",
    "8": "UBS BRASIL CCTVM S/A",
    "13": "MERRILL LYNCH S/A CTVM",
    "15": "GUIDE INVESTIMENTOS SA CORRETORA DE VALORES",
    "16": "J.P. MORGAN CCVM S/A",
    "18": "BOCOM BBM CCVM S/A",
    "21": "VOTORANTIM ASSET MANAGEMENT DTVM LTDA",
    "23": "NECTON INVESTIMENTOS S.A. CVMC",
    "27": "SANTANDER CCVM S/A",
    "39": "AGORA CTVM S/A",
    "41": "ING CCT S/A",
    "45": "CREDIT SUISSE (BRASIL) S.A. CTVM",
    "59": "SAFRA CORRETORA DE VALORES E CAMBIO LTDA",
    "63": "NOVINVEST CVM LTDA",
    "72": "BRADESCO S/A CTVM",
    "77": "CITIGROUP GLOBAL MARKETS BRASIL CCTVM S/A",
    "85": "BTG PACTUAL CTVM S/A",
    "88": "CM CAPITAL MARKETS CCTVM LTDA",
    "90": "EASYNVEST – TITULO CV S/A",
    "92": "RENASCENCA DTVM LTDA",
    "93": "NOVA FUTURA CTVM LTDA",
    "106": "MERCANTIL DO BRASIL C. S/A CTVM",
    "107": "TERRA INVESTIMENTOS DTVM LTDA",
    "120": "GENIAL INSTITUCIONAL CCTVM S/A",
    "122": "BGC LIQUIDEZ DTVM LTDA",
    "127": "TULLETT PREBON BRASIL CVC LTDA.",
    "129": "PLANNER CORRETORA DE VALORES S/A",
    "147": "ATIVA INVESTIMENTOS S/A CTCV",
    "174": "ELITE CCVM LTDA",
    "190": "WARREN CVMC LTDA",
    "226": "AMARIL FRANKLIN CTV LTDA",
    "2446": "ITAU CV S/A",
    "252": "BANCO ITAU BBA S/A",
    "308": "CLEAR CORRETORA",
    "386": "RICO INVESTIMENTOS",
    "4090": "TORO CTVM LTDA",
    "50935": "BANCO XP S.A"
}

# Função para formatar o DataFrame
def formatar_dados(df):
    df['horario'] = pd.to_datetime(df['horario'], format='%H:%M:%S.%f', errors='coerce')
    df['corretora_comprou_nome'] = df['corretora_comprou'].astype(str).map(codigo_corretoras).fillna(df['corretora_comprou'])
    df['corretora_vendeu_nome'] = df['corretora_vendeu'].astype(str).map(codigo_corretoras).fillna(df['corretora_vendeu'])
    return df

@st.cache_resource
def conectar_db():
    return sqlite3.connect("negocios_log.db", check_same_thread=False)

conn = conectar_db()

st.set_page_config(layout="wide")
st.title("Monitor de RLP, Market Maker e Agressões")

with st.sidebar:
    st.header("Filtros de Tempo")
    hoje = datetime.now().date()
    data_inicio = st.date_input("Data de Início", hoje)
    data_fim = st.date_input("Data de Fim", hoje)

    hora_inicio = st.time_input("Hora Inicial", datetime.now().replace(hour=9, minute=0).time())
    hora_fim = st.time_input("Hora Final", datetime.now().replace(hour=18, minute=0).time())

    botao_filtrar = st.button("Filtrar")

if botao_filtrar:
    inicio = datetime.combine(data_inicio, hora_inicio).strftime('%H:%M:%S.%f')
    fim = datetime.combine(data_fim, hora_fim).strftime('%H:%M:%S.%f')

    query = f"""
        SELECT * FROM negocios
        WHERE horario BETWEEN '{inicio}' AND '{fim}'
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        st.warning("Nenhum dado encontrado para o período selecionado.")
    else:
        df = formatar_dados(df)

        st.subheader("Negócios Filtrados")
        st.dataframe(df[['id', 'ativo', 'operacao', 'horario', 'preco', 'quantidade',
                        'corretora_comprou_nome', 'corretora_vendeu_nome', 'agressor', 'rlp', 'rlp_liquido']], use_container_width=True)

        st.subheader("Métricas Calculadas")
        rlp_compra = df[(df['rlp'] == 'RLP COMPRADOR')]['quantidade'].sum()
        rlp_venda = df[(df['rlp'] == 'RLP VENDEDOR')]['quantidade'].sum()

        agressao_compra = df[df['agressor'] == 'A']['quantidade'].sum()
        agressao_venda = df[df['agressor'] == 'V']['quantidade'].sum()

        passivo_compra = df[(df['operacao'] == 'C') & (df['agressor'] != 'A')]['quantidade'].sum()
        passivo_venda = df[(df['operacao'] == 'V') & (df['agressor'] != 'V')]['quantidade'].sum()

        st.metric("RLP Comprador", rlp_compra)
        st.metric("RLP Vendedor", rlp_venda)
        st.metric("Agressão de Compra", agressao_compra)
        st.metric("Agressão de Venda", agressao_venda)
        st.metric("Passivo de Compra", passivo_compra)
        st.metric("Passivo de Venda", passivo_venda)

        corretoras_pf = ['630', '127', '3', '386']  # Ex: XP, RICO, Modal, Clear
        df_pf = df[df['corretora_vendeu'].astype(str).isin(corretoras_pf)]
        exposicao_pf = df_pf['quantidade'].sum()

        st.metric("Exposição Pessoa Física (venda)", exposicao_pf)

        df_mm = df[df['corretora_comprou'] == df['corretora_vendeu']]
        freq_mm = df_mm['corretora_comprou'].astype(str).map(codigo_corretoras).fillna(df_mm['corretora_comprou']).value_counts().reset_index()
        freq_mm.columns = ['Corretora', 'Frequência']

        st.subheader("Possíveis Market Makers")
        st.dataframe(freq_mm, use_container_width=True)

        st.download_button("Exportar para CSV", df.to_csv(index=False), file_name="negocios_filtrados.csv")
