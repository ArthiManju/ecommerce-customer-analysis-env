import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
from dotenv import load_dotenv
import os
st.set_page_config(page_title="E-Commerce Dashboard", layout="wide")
load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
print("db connected successfully")
@st.cache_data
def load_data():
    query = "SELECT * FROM ecommerce_data"
    df = pd.read_sql(query, engine)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
    return df

df = load_data()


st.sidebar.title("Filters")
country = st.sidebar.selectbox("Select Country", df['Country'].unique())
df_filtered = df[df['Country'] == country]
df_filtered['Month'] = df_filtered['InvoiceDate'].dt.to_period('M').astype(str)
monthly_revenue = df_filtered.groupby('Month')['TotalPrice'].sum().reset_index()

st.title("🛒 E-Commerce Dashboard")
st.metric("Total Revenue", f"${df_filtered['TotalPrice'].sum():,.2f}")
st.metric("Total Orders", df_filtered['InvoiceNo'].nunique())
st.metric("Unique Customers", df_filtered['CustomerID'].nunique())

st.subheader("📊 Monthly Revenue")
fig = px.bar(
    monthly_revenue,
    x='Month',
    y='TotalPrice',
    title='Monthly Revenue',
    labels={'TotalPrice': 'Revenue ($)', 'Month': 'Month'},
    color='TotalPrice',
    color_continuous_scale='Blues'
)
st.plotly_chart(fig, use_container_width=True)

