"""
Streamlit Analytics Dashboard for Weather DWH Data Marts
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(
    page_title="Weather ETL Analytics DWH",
    layout="wide",
)


def get_db_connection():
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "weather_dwh")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


@st.cache_data(ttl=60)
def load_daily_summary():
    engine = get_db_connection()
    query = "SELECT * FROM analytics.mart_daily_weather_summary ORDER BY observation_date DESC, city_name ASC;"
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_fact_observations():
    engine = get_db_connection()
    query = "SELECT * FROM analytics.fact_weather_observations ORDER BY observation_time DESC LIMIT 1000;"
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_dim_cities():
    engine = get_db_connection()
    query = "SELECT * FROM analytics.dim_cities;"
    return pd.read_sql(query, engine)


# Main Interface
st.title("Pipeline ETL / ELT Weather Analytics Dashboard")
st.markdown("Orchestré par **Apache Airflow**, transformé par **dbt**, stocké dans **PostgreSQL** (Source : API Open-Meteo).")

try:
    df_cities = load_dim_cities()
    df_daily = load_daily_summary()
    df_facts = load_fact_observations()

    # KPI Summary Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Villes Suivies", len(df_cities))
    col2.metric("Observations Horaires", f"{len(df_facts):,}")
    col3.metric("Température Moyenne Global", f"{df_daily['avg_temperature_celsius'].mean():.1f} °C")
    col4.metric("Cumul Précipitations (mm)", f"{df_daily['total_precipitation_mm'].sum():.1f} mm")

    st.markdown("---")

    # City selector
    cities_list = sorted(df_daily["city_name"].unique())
    selected_cities = st.multiselect("Sélectionner les villes à analyser :", cities_list, default=cities_list)

    df_filtered_daily = df_daily[df_daily["city_name"].isin(selected_cities)]

    # Charts
    st.subheader("Évolution des Températures Quotidiennes (Min / Moy / Max)")
    fig_temp = px.line(
        df_filtered_daily,
        x="observation_date",
        y="avg_temperature_celsius",
        color="city_name",
        markers=True,
        title="Température Moyenne (°C) par Ville",
        labels={"observation_date": "Date", "avg_temperature_celsius": "Température Moyenne (°C)"},
    )
    st.plotly_chart(fig_temp, width="stretch")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Cumul des Précipitations par Ville (mm)")
        fig_precip = px.bar(
            df_filtered_daily.groupby("city_name")["total_precipitation_mm"].sum().reset_index(),
            x="city_name",
            y="total_precipitation_mm",
            color="city_name",
            title="Précipitations Totales Cumulées",
        )
        st.plotly_chart(fig_precip, width="stretch")

    with col_right:
        st.subheader("Vitesse Maximale du Vent (km/h)")
        fig_wind = px.bar(
            df_filtered_daily.groupby("city_name")["max_wind_speed_kmh"].max().reset_index(),
            x="city_name",
            y="max_wind_speed_kmh",
            color="city_name",
            title="Rafale Maximale Enregistrée",
        )
        st.plotly_chart(fig_wind, width="stretch")

    # Detailed Facts Table
    st.subheader("Aperçu des Faits dbt (fact_weather_observations)")
    st.dataframe(df_facts.head(100), width="stretch")

except Exception as e:
    st.warning("Connexion à la base de données PostgreSQL indisponible ou tables analytiques non encore générées.")
    st.info("Lancez le pipeline Airflow / dbt pour alimenter les schémas analytiques de PostgreSQL.")
    st.error(f"Détail de l'erreur : {e}")
