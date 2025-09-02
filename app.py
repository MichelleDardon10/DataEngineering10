# -*- coding: utf-8 -*-
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import streamlit as st

# ------------------ Conn ------------------
PGHOST = os.getenv("PGHOST", "db")  # nombre del servicio en Compose
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGDATABASE = os.getenv("PGDATABASE", "citibike")

SCHEMA = "citibike"
TABLE = "trips"

st.set_page_config(page_title="CitiBike Dashboard", layout="wide")

@st.cache_resource(show_spinner=False)
def get_pool() -> ThreadedConnectionPool:
    return ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
        cursor_factory=RealDictCursor,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

@contextmanager
def get_cursor():
    pool = get_pool()
    conn = pool.getconn()
    try:
        if getattr(conn, "closed", 0):
            pool.putconn(conn, close=True)
            conn = pool.getconn()
        with conn.cursor() as cur:
            yield cur
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass

@st.cache_data(ttl=300, show_spinner=False)
def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    def _run():
        with get_cursor() as cur:
            cur.execute(sql, params or {})
            rows = cur.fetchall()
        return pd.DataFrame(rows)

    try:
        return _run()
    except psycopg2.InterfaceError:
        # Conexión rota: resetea y reintenta
        pool = get_pool()
        try:
            pool.closeall()
        except Exception:
            pass
        return _run()

st.title("🚲 CitiBike – Dashboard")

# --------- Rango global ----------
rng = query_df(f"""
    SELECT MIN(started_at) AS min_started_at,
           MAX(started_at) AS max_started_at,
           COUNT(*)::bigint AS n_rows
    FROM {SCHEMA}.{TABLE};
""")

if rng.empty or pd.isna(rng.loc[0,"min_started_at"]):
    st.warning("No hay datos en la tabla citibike.trips.")
    st.stop()

min_dt = pd.to_datetime(rng.at[0, "min_started_at"]).date()
max_dt = pd.to_datetime(rng.at[0, "max_started_at"]).date()
n_rows = int(rng.at[0, "n_rows"])

# --------- Filtros ----------
st.sidebar.header("Filtros")
start_date = st.sidebar.date_input("Desde", min_dt, min_value=min_dt, max_value=max_dt)
end_date   = st.sidebar.date_input("Hasta",  max_dt, min_value=min_dt, max_value=max_dt)
if start_date > end_date:
    st.sidebar.error("La fecha 'Desde' no puede ser mayor que 'Hasta'.")
    st.stop()

member_filter = st.sidebar.multiselect(
    "Tipo de usuario", options=["member","casual","unknown"], default=["member","casual","unknown"]
)

where = ["started_at >= %(d0)s::date", "started_at < (%(d1)s::date + interval '1 day')"]
params = {"d0": start_date.isoformat(), "d1": end_date.isoformat()}
if member_filter:
    where.append("COALESCE(member_casual,'') = ANY(%(m)s)")
    params["m"] = member_filter
where_sql = "WHERE " + " AND ".join(where)

# --------- KPIs ----------
c1,c2,c3 = st.columns(3)
c1.metric("Filas totales", f"{n_rows:,}")
c2.metric("Primer viaje", min_dt.isoformat())
c3.metric("Último viaje", max_dt.isoformat())
st.divider()

# --------- Tabs ----------
tab_summary, tab_series, tab_stations, tab_dur, tab_map = st.tabs(
    ["Resumen", "Serie", "Estaciones", "Duración", "Mapa"]
)

with tab_summary:
    with st.spinner("Calculando KPIs…"):
        kpis = query_df(f"""
            SELECT
              COUNT(*)::bigint AS trips,
              AVG(tripduration_seconds)::numeric(10,1) AS avg_secs,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tripduration_seconds) AS p50_secs,
              COUNT(DISTINCT start_station_id) AS start_stations,
              COUNT(DISTINCT end_station_id)   AS end_stations
            FROM {SCHEMA}.{TABLE}
            {where_sql};
        """, params)
    if not kpis.empty:
        ca, cb, cc, cd, ce = st.columns(5)
        ca.metric("Viajes", f"{int(kpis.at[0,'trips']):,}")
        cb.metric("Duración promedio (s)", f"{float(kpis.at[0,'avg_secs'] or 0):.0f}")
        cc.metric("Duración mediana (s)", f"{float(kpis.at[0,'p50_secs'] or 0):.0f}")
        cd.metric("Est. origen", f"{int(kpis.at[0,'start_stations']):,}")
        ce.metric("Est. destino", f"{int(kpis.at[0,'end_stations']):,}")
    else:
        st.info("Sin datos para los filtros.")

with tab_series:
    st.subheader("📈 Viajes por día")
    with st.spinner("Generando serie…"):
        daily = query_df(f"""
            SELECT date_trunc('day', started_at)::date AS day,
                   COUNT(*)::bigint AS trips
            FROM {SCHEMA}.{TABLE}
            {where_sql}
            GROUP BY 1 ORDER BY 1;
        """, params)
    if daily.empty:
        st.info("Sin datos en el rango/segmento seleccionado.")
    else:
        daily = daily.astype({"trips":"int64"})
        st.line_chart(daily.set_index("day")["trips"])

with tab_stations:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🏁 Top estaciones de origen")
        top_start = query_df(f"""
            SELECT COALESCE(start_station_name, start_station_id::text) AS station,
                   COUNT(*)::bigint AS trips
            FROM {SCHEMA}.{TABLE}
            {where_sql}
            GROUP BY 1 ORDER BY trips DESC LIMIT 20;
        """, params)
        if top_start.empty:
            st.info("Sin datos.")
        else:
            top_start = top_start.astype({"trips":"int64"})
            st.bar_chart(top_start.set_index("station")["trips"])
    with c2:
        st.subheader("🎯 Top estaciones de destino")
        top_end = query_df(f"""
            SELECT COALESCE(end_station_name, end_station_id::text) AS station,
                   COUNT(*)::bigint AS trips
            FROM {SCHEMA}.{TABLE}
            {where_sql}
            GROUP BY 1 ORDER BY trips DESC LIMIT 20;
        """, params)
        if top_end.empty:
            st.info("Sin datos.")
        else:
            top_end = top_end.astype({"trips":"int64"})
            st.bar_chart(top_end.set_index("station")["trips"])

with tab_dur:
    st.subheader("⏱️ Duración por tipo de usuario")
    dur = query_df(f"""
        SELECT
          COALESCE(member_casual,'(desconocido)') AS member_casual,
          AVG(tripduration_seconds)::numeric(10,1) AS avg_secs,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tripduration_seconds) AS p50_secs,
          COUNT(*)::bigint AS trips
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        GROUP BY 1 ORDER BY trips DESC;
    """, params)
    if dur.empty:
        st.info("Sin datos.")
    else:
        st.dataframe(dur, use_container_width=True)

with tab_map:
    st.subheader("🗺️ Mapa de inicios")
    sample_n = st.slider("Muestra (puntos)", 1000, 20000, 5000, step=1000)
    with st.spinner("Descargando coordenadas…"):
        pts = query_df(f"""
            SELECT start_lat, start_lng
            FROM {SCHEMA}.{TABLE}
            {where_sql}
            AND start_lat IS NOT NULL AND start_lng IS NOT NULL
            LIMIT %(n)s;
        """, {**params, "n": sample_n})
    if pts.empty:
        st.info("No hay coordenadas en el rango seleccionado.")
    else:
        st.map(pts.rename(columns={"start_lat":"lat","start_lng":"lon"}))

# --------- Nuevo Tab: Análisis general ---------
tab_general, = st.tabs(["Análisis general"])

with tab_general:
    st.subheader("🔝 Bicis más usadas")
    top_bikes = query_df(f"""
        SELECT bikeid, COUNT(*)::bigint AS viajes
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        GROUP BY bikeid
        ORDER BY viajes DESC
        LIMIT 10;
    """, params)
    if not top_bikes.empty:
        top_bikes = top_bikes.astype({"viajes":"int64"})
        st.bar_chart(top_bikes.set_index("bikeid")["viajes"])
    else:
        st.info("No hay datos para el rango seleccionado.")

    st.divider()
    st.subheader("⏰ Horas pico de uso")
    hours = query_df(f"""
        SELECT EXTRACT(HOUR FROM started_at)::int AS hora, COUNT(*)::bigint AS viajes
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        GROUP BY hora ORDER BY hora;
    """, params)
    if not hours.empty:
        hours = hours.astype({"viajes":"int64"})
        st.bar_chart(hours.set_index("hora")["viajes"])
    else:
        st.info("No hay datos.")

    st.divider()
    st.subheader("📅 Viajes por día de la semana")
    dow = query_df(f"""
        SELECT TRIM(TO_CHAR(started_at, 'Day')) AS dia_semana, COUNT(*)::bigint AS viajes
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        GROUP BY dia_semana
        ORDER BY viajes DESC;
    """, params)
    if not dow.empty:
        st.dataframe(dow, use_container_width=True)
    else:
        st.info("No hay datos.")

    st.divider()
    st.subheader("🧓 Distribución por año de nacimiento (Top 20)")
    births = query_df(f"""
        SELECT birth_year, COUNT(*)::bigint AS viajes
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        AND birth_year IS NOT NULL
        GROUP BY birth_year
        ORDER BY viajes DESC
        LIMIT 20;
    """, params)
    if not births.empty:
        births = births.astype({"viajes":"int64"})
        st.bar_chart(births.set_index("birth_year")["viajes"])
    else:
        st.info("No hay datos de birth_year.")

    st.divider()
    st.subheader("🗺️ Viajes más largos (> 2h)")
    long_trips = query_df(f"""
        SELECT ride_id, started_at, ended_at, tripduration_seconds
        FROM {SCHEMA}.{TABLE}
        {where_sql}
        AND tripduration_seconds > 7200
        ORDER BY tripduration_seconds DESC
        LIMIT 20;
    """, params)
    if not long_trips.empty:
        st.dataframe(long_trips, use_container_width=True)
    else:
        st.info("No hay viajes largos en este rango.")
