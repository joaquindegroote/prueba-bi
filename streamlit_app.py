import streamlit as st
import duckdb
from pathlib import Path
import yaml

st.set_page_config(page_title="Prueba BI Demo", layout="wide")

# 1. Load CSVs into DuckDB in-memory
# ---------- Carga de datos en DuckDB ----------
@st.cache_resource          # ← en vez de st.cache_data
def get_duck():
    con = duckdb.connect()
    for csv in Path("data").glob("*.csv"):
        con.execute(
            f"CREATE TABLE {csv.stem} AS "
            f"SELECT * FROM read_csv_auto('{csv}', header=True)"
        )
    return con

con = get_duck()

# 2. Question catalogue (add/modify freely)
QUESTIONS = {
    "Top 10 clientes por monto de siniestros": {
        "sql": """            SELECT insured_full_name,
               COUNT(*)             AS n_claims,
               SUM(claim_amount)    AS total_amount
        FROM claims_sample
        GROUP BY insured_full_name
        ORDER BY total_amount DESC
        LIMIT 10;
        """,
        "brief": "Ranking de clientes (asegurado) según el monto total reclamado.",
        "detail": """            Calculamos el valor reclamado total por asegurado para detectar             posibles concentraciones de riesgo o fraude. Se limita a los 10             mayores para facilitar la inspección manual por el equipo de siniestros."""
    },
    "Mora promedio de cuotas": {
        "sql": """            WITH diff AS (
          SELECT fee_id,
                 julianday(paid_at) - julianday(due_date) AS days_late
          FROM fees_populated
          WHERE paid_at IS NOT NULL
        )
        SELECT ROUND(AVG(days_late),2) AS avg_days_late
        FROM diff;
        """,
        "brief": "Medimos la puntualidad de pago de las cuotas.",
        "detail": "El KPI sirve para el área de cobranzas y proyecciones de flujo."
    },
    "Pólizas activas vs canceladas": {
        "sql": """            SELECT status, COUNT(*) AS qty
        FROM policies_populated
        GROUP BY status
        ORDER BY qty DESC;
        """,
        "brief": "Estado portfolio de pólizas.",
        "detail": "Permite monitorear retención y churn."
    },
    "Duración media de conversación del bot": {
        "sql": """            SELECT ROUND(AVG(
          julianday(end_time) - julianday(start_time)
        )*24*60,1) AS avg_minutes
        FROM bot_conversations_sample
        WHERE end_time IS NOT NULL;
        """,
        "brief": "Eficiencia del bot de IA (tiempo en minutos).",
        "detail": "Tiempo prolongado puede indicar complejidad o problemas de UX."
    }
}

# 3. UI
sel = st.sidebar.radio("Selecciona consulta", list(QUESTIONS.keys()))
meta = QUESTIONS[sel]

st.header(sel)
st.code(meta["sql"], language="sql")

if st.button("Ejecutar consulta"):
    df = con.execute(meta["sql"]).df()
    st.dataframe(df, use_container_width=True)

with st.expander("Explicación breve"):
    st.markdown(meta["brief"])
with st.expander("Explicación detallada"):
    st.markdown(meta["detail"])
