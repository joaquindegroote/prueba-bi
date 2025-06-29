import streamlit as st
import pandas as pd
import sqlite3

st.set_page_config(page_title="Prueba Técnica SQL", layout="wide")
st.title("Prueba Técnica – SQL & Casos de Uso")

# --------  Utils & Data -------------------------------------------------------------------

def load_data():
    """Carga los CSV sintetizados y retorna DataFrames."""
    policies = pd.read_csv('data/policies.csv', dtype={'status': str, 'vehicle': str, 'coverages': str})
    fees     = pd.read_csv('data/fees.csv', dtype={'status': str}, parse_dates=['due_date'])
    return policies, fees

@st.cache_resource(show_spinner="Inicializando base en memoria…")
def get_connection() -> sqlite3.Connection:
    """Carga los CSV en una BD SQLite en memoria (multi‑thread)."""
    policies, fees = load_data()
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    policies.to_sql('policies', conn, index=False, if_exists='replace')
    fees.to_sql('fees', conn, index=False, if_exists='replace')
    return conn

conn = get_connection()

# --------  Tabs ---------------------------------------------------------------------------

tab1, tab2 = st.tabs(["Sección 1: Consultas SQL", "Sección 2: Casos de Uso"])

# ==========================================================================================
# TAB 1 – CONSULTAS SQL
# ==========================================================================================
with tab1:
    # -------------------- Consulta 1 ------------------------------------------------------
    st.header("1. Conteo de vehículos activos")
    query1 = """
SELECT
    json_extract(vehicle, '$.brand')  AS marca,
    json_extract(vehicle, '$.model')  AS modelo,
    CAST(json_extract(vehicle, '$.year') AS INTEGER) AS año,
    COUNT(*) AS cantidad
FROM policies
WHERE status = 'active'
  AND json_extract(vehicle, '$.brand') IS NOT NULL
  AND json_extract(vehicle, '$.model') IS NOT NULL
  AND json_extract(vehicle, '$.year')  IS NOT NULL
GROUP BY marca, modelo, año
ORDER BY cantidad DESC;
"""
    st.code(query1, language='sql')
    if st.button("🔍 Ejecutar Consulta 1"):
        df1 = pd.read_sql(query1, conn)
        st.dataframe(df1, use_container_width=True)
        if not df1.empty:
            st.caption("Distribución de vehículos activos por marca (top‑10)")
            top_brand = df1.groupby('marca', as_index=False)['cantidad'].sum().nlargest(10, 'cantidad')
            st.bar_chart(top_brand.set_index('marca'))

    st.subheader("¿Por qué es valioso este conteo?")
    st.markdown(
        """
- **Underwriting** identifica concentraciones de riesgo y ajusta tarifas por segmento.
- **Marketing** diseña campañas específicas (cross‑sell de coberturas premium, upsell de asistencia) para los segmentos de mayor flota.
- **Operaciones** negocia descuentos de repuestos/talleres donde la aseguradora tiene mayor volumen.
        """)

    st.subheader("Mejoras técnicas (Postgres)")
    st.markdown(
        """
- **Columnas generadas** (`brand_g`, `model_g`, `year_g`) → permiten índices B‑tree clásicos, evitando `json_extract` en tiempo de ejecución.
- **Índices GIN parciales** sobre `(vehicle -> 'brand')` y `(vehicle -> 'model')` **dentro** del `WHERE status = 'active'`.
- **Particionamiento** por rango de `created_at` para data warehouse con >100 M pólizas.
        """)

    st.divider()

    # -------------------- Consulta 2 ------------------------------------------------------
    st.header("2. Prima total por póliza")
    query2 = """
SELECT
    policy_id,
    SUM(CAST(json_extract(cov.value, '$.premium') AS REAL)) AS prima_total
FROM policies
CROSS JOIN json_each(policies.coverages) AS cov
GROUP BY policy_id
ORDER BY prima_total DESC;
"""
    st.code(query2, language='sql')
    if st.button("💰 Ejecutar Consulta 2"):
        df2 = pd.read_sql(query2, conn)
        st.dataframe(df2.head(15), use_container_width=True)
        if not df2.empty:
            st.caption("Top‑15 pólizas por prima total")

    st.subheader("¿Por qué sumar las primas?")
    st.markdown(
        """
- **Finanzas**: proyección de ingresos y detección de pólizas con sobreprecio/subprecio.
- **Auditoría**: comparar prima escrita vs. prima facturada para reducir fuga de ingresos.
- **Actuaría**: calcular métrica *Average Written Premium* y alimentar modelos de severidad.
        """)

    st.subheader("Mejoras técnicas (Postgres)")
    st.markdown(
        """
- **Vista materializada `mv_primas`** con refresh nocturno, reduciendo latencia de dashboards.
- **Índice funcional** sobre la suma de `premium` → acelera filtros `ORDER BY prima_total DESC LIMIT 20`.
- Migrar el tipo a **`NUMERIC(12,2)`** para evitar pérdida de precisión cuando `premium` excede 2^24.
        """)

    st.divider()

    # -------------------- Consulta 3 ------------------------------------------------------
    st.header("3. Próxima cuota pendiente")
    query3 = """
SELECT
    f.policy_id,
    f.fee_id,
    f.due_date,
    f.amount
FROM fees AS f
WHERE f.status IN ('pending','overdue')
  AND f.due_date >= date('now')
  AND f.due_date = (
      SELECT MIN(due_date)
      FROM fees
      WHERE policy_id = f.policy_id
        AND status IN ('pending','overdue')
        AND due_date >= date('now')
  )
ORDER BY f.policy_id;
"""
    st.code(query3, language='sql')
    if st.button("⏰ Ejecutar Consulta 3"):
        df3 = pd.read_sql(query3, conn)
        # convertir due_date a datetime para evitar error .date()
        if not df3.empty:
            df3['due_date'] = pd.to_datetime(df3['due_date'])
        st.dataframe(df3, use_container_width=True)
        if not df3.empty:
            txt = (
                f"Ejemplo de recordatorio automático: próxima cuota para la póliza #{df3.iloc[0]['policy_id']} "
                f"vence el {df3.iloc[0]['due_date'].date()} ✉️"
            )
            st.caption(txt)

    st.subheader("¿Por qué necesitamos la próxima cuota?")
    st.markdown(
        """
- **Cobranza**: enfocar recordatorios y evitar mora antes de la fecha crítica.
- **Customer Success**: prevenir cancelaciones enviando ofertas de refinanciamiento.
- **Tesorería**: pronosticar flujo de caja diario/semanal.
        """)

    st.subheader("Mejoras técnicas (Postgres)")
    st.markdown(
        """
- **Índice parcial** `(policy_id, due_date)` **WHERE status IN ('pending','overdue')`.
- Calendario de pagos como **tabla de hechos** en un modelo estrella → facilita análisis por periodo.
- Automatizar recordatorios vía **cron + webhook** (SMS/WhatsApp) usando la consulta como fuente.
        """)

    st.divider()

    # -------------------- Sección 4 --------------------------------------------------------
    st.header("4. Limitaciones y roadmap de mejora")

    roadmap_df = pd.DataFrame({
        'Limitación actual': [
            'Datos anidados en JSONB',
            'Sin historización de cambios',
            'Escalabilidad OLAP',
            'Seguridad PII/PCI',
            'Rendimiento en consultas temporales'
        ],
        'Mejora propuesta': [
            'Normalizar tablas vehicle / coverages',
            'SCD‑Type 2 o snapshots diarios',
            'Data Lake bronze‑silver‑gold (Parquet/Iceberg)',
            'RLS + cifrado transparente',
            'Particionamiento por rango (created_at) + índices parciales'
        ],
        'Justificación': [
            'Índices simples, FKs, JOINs eficientes',
            'Auditoría, GDPR/CCPA compliance',
            'Coste/GB bajo + compute elástico',
            'Minimizar riesgo de fuga de datos',
            'Menor I/O y tiempos < 200 ms'
        ]
    })
    st.dataframe(roadmap_df, use_container_width=True, hide_index=True)
