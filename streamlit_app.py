import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta

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
    """Carga los CSV en una BD SQLite en memoria (multi-thread)."""
    policies, fees = load_data()
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    policies.to_sql('policies', conn, index=False, if_exists='replace')
    fees.to_sql('fees', conn, index=False, if_exists='replace')
    return conn

conn = get_connection()

# --------  Tabs ---------------------------------------------------------------------------

tab1, tab2, tab3 = st.tabs([
    "Sección 1: Consultas SQL",
    "Sección 2: Casos de Uso",
    "Sección 3: Pregunta Abierta"
])

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
El conteo de vehículos activos permite detectar tendencias de riesgo por marca, modelo y año y ajustar tarifas, límites y retenciones según la experiencia de siniestralidad; provee a **Actuaría** la base empírica para estimar frecuencia y severidad y calcular el *loss ratio* segmentado, indispensable para reservas técnicas y negociaciones de reaseguro; ofrece a **Marketing & Growth** una segmentación granular desde la cual diseñar campañas de *cross‑sell* y *upsell* y programas de fidelización basados en el LTV; satisface los requerimientos de **Cumplimiento Regulatorio** en informes de concentración de cartera; y entrega a **Operaciones** la masa crítica necesaria para negociar mejores SLA y precios con talleres y proveedores, reduciendo tanto costo como *turn‑around‑time*.
        """
    )

    st.subheader("Mejoras técnicas (Postgres)")
    st.markdown(
        """
- **Columnas generadas** + índices B‑tree → habilitan *index‑only scans* y evitan `json_extract` en tiempo de ejecución.
- **Índices GIN parciales** (`WHERE status='active'`) sobre JSONB → aceleran exploraciones ad‑hoc sin penalizar escrituras de pólizas inactivas.
- **Particionamiento por rango** (`created_at` mes) → facilita archivado (*DETACH*) y *partition‑pruning* en dashboards.
        """
    )

    st.divider()

    # -------------------- Consulta 2 ------------------------------------------------------
    st.header("2. Prima total por póliza")
    query2 = """
SELECT policy_id,
       SUM(CAST(json_extract(cov.value, '$.premium') AS REAL)) AS prima_total
FROM   policies
CROSS  JOIN json_each(policies.coverages) AS cov
GROUP  BY policy_id
ORDER  BY prima_total DESC;
"""
    st.code(query2, language='sql')
    if st.button("💰 Ejecutar Consulta 2"):
        df2 = pd.read_sql(query2, conn)
        st.dataframe(df2.head(15), use_container_width=True)

    st.subheader("¿Por qué sumar las primas?")
    st.markdown(
        """
La prima total por póliza constituye el insumo central para **Finanzas** al proyectar ingresos escritos frente a ganados y alimentar el *forecast* trimestral; permite a **Auditoría** cotejar lo escrito con lo efectivamente facturado y detectar fugas en el proceso de cobranza; entrega a **Actuaría** el *Average Written Premium* utilizado en pricing y en el cálculo de reservas IBNR; ayuda a **Producto & Pricing** a experimentar con bundles y medir la elasticidad precio‑demanda para optimizar el margen técnico; y da a **Riesgo & Reaseguro** visibilidad sobre la exposición agregada necesaria para definir capas de retención y coberturas facultativas.
        """
    )

    st.subheader("Mejoras técnicas (Postgres)")
    st.markdown(
        """
- **Vista materializada** `mv_primas` (*REFRESH nocturno*) → SLA sub‑segundo en reportes.
- **Índice funcional** sobre `SUM(premium)` → acelera *TOP‑N queries*.
- **Tipo `NUMERIC(12,2)`** → evita errores de redondeo y cumple normativas SOX.
- **CHECK de moneda** + tabla `exchange_rates` → garantiza consistencia multimoneda.
        """
    )

    st.divider()

    # -------------------- Consulta 3 ------------------------------------------------------
    st.header("3. Próxima cuota pendiente")
    query3 = """
SELECT f.policy_id, f.fee_id, f.due_date, f.amount
FROM   fees AS f
WHERE  f.status IN ('pending','overdue')
  AND  f.due_date >= date('now')
  AND  f.due_date = (
        SELECT MIN(due_date)
        FROM   fees
        WHERE  policy_id = f.policy_id
          AND  status IN ('pending','overdue')
          AND  due_date >= date('now'))
ORDER  BY f.policy_id;
"""
    st.code(query3, language='sql')
    if st.button("⏰ Ejecutar Consulta 3"):
        df3 = pd.read_sql(query3, conn)
        if not df3.empty:
            df3['due_date'] = pd.to_datetime(df3['due_date'])
        st.dataframe(df3, use_container_width=True)
        if not df3.empty:
            st.caption(f"Recordatorio: la próxima cuota de la póliza #{df3.iloc[0]['policy_id']} vence el {df3.iloc[0]['due_date'].date()} ✉️")

    st.subheader("¿Por qué necesitamos la próxima cuota?")
    st.markdown(
        """
Conocer la próxima cuota pendiente habilita a **Cobranza** a programar recordatorios multicanal y ofrecer planes de pago antes de que se materialice la mora; permite a **Tesorería** refinar el flujo de caja proyectado y asegurar liquidez frente a obligaciones regulatorias y pagos de siniestros; brinda a **Customer Success** la oportunidad de intervenir proactivamente para evitar cancelaciones y reducir el *churn*; ayuda a **Riesgo & Cumplimiento** a prevenir contingencias legales asociadas a pólizas sin cobertura por impago; y mejora la experiencia de cliente al desencadenar notificaciones *in‑app* o *push* que protegen el NPS.
        """
    )

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

# ==========================================================================================
# TAB 2 – CASOS DE USO 
# ==========================================================================================
with tab2:
    st.header("Caso 1 – Integración mensual de siniestros (CSV → DW)")

    with st.expander("1️⃣  Proceso propuesto", expanded=True):
        st.markdown("""
**Seis pasos ETL (versión enriquecida)**
1. **Recepción** ▸ bucket *landing* inmutable con versionado + *object lock*.
2. **Validación** ▸ *Great Expectations* ➜ métricas en DataDog, corte si `unexpected_percent > 1%`.
3. **Staging** ▸ ingesta append‑only con `source_file`, `ingested_at` y hash MD5.
4. **Transformación** ▸ normaliza unidades, deduplica vía *window function* y flag para MERGE.
5. **Historización** ▸ Delta Lake SCD‑2 idempotente (*exactly‑once*).
6. **Monitoreo** ▸ eventos OpenLineage + SLAs Airflow → alertas Slack/PagerDuty.
        """)

    with st.expander("2️⃣  Problemas potenciales", expanded=True):
        st.markdown("""
| Problema | Ejemplo | Impacto |
|---|---|---|
| **Drift de esquema** | Nueva columna `currency` | Carga falla o columnas desalineadas |
| **Codificación/delimitadores** | UTF‑16, separador `;` | Valores mal parseados |
| **Datos inconsistentes** | `claim_amount` negativo | Cálculos erróneos |
| **Inconsistencia referencial** | `claim` sin `policy_number` | Siniestros huérfanos |
| **Duplicados** | Misma clave `policy+claim` | Sobreconteo de siniestros |
| **Volumen creciente** | CSV > 2 GB | Riesgo *OOM* y ventana rota |
| **Seguridad & PII** | Nombres sin cifrar | Riesgo de fuga de datos |
| **Retrasos de entrega** | Archivo fuera SLA | Brechas en reporting |
        """, unsafe_allow_html=True)

    with st.expander("3️⃣  Solución escalable", expanded=True):
        st.markdown("""
Arquitectura **lakehouse** (Parquet/Delta Bronze→Silver→Gold) + orquestador declarativo (Airflow/Prefect).  
Contratos de datos (JSON Schema) garantizan calidad; transformaciones idempotentes; MERGE SCD‑2 preserva histórico.  
Linaje OpenLineage, código en Git CI/CD, cómputo serverless auto‑escalable (Glue, Databricks Jobs, Dataflow).  
Cifrado end‑to‑end + IAM de mínimo privilegio.
        """)

    st.divider()

    # -------------------- Bot IA ----------------------------------------------------------
    st.header("Caso 2 – Evaluación de desempeño del Bot de IA")

    with st.expander("1️⃣  Métricas prioritarias", expanded=True):
        st.markdown("""
**Matriz de KPIs por dimensión**

| # | Métrica | Dimensión | SQL conceptual | Insight clave |
|---|---|---|---|---|
| 1 | **Tasa de Contención** | Eficiencia operativa | `1 - AVG(transferred_to_agent)` | ¿Cuánta carga desviamos del call‑center? |
| 2 | **Goal Completion Rate (GCR)** | Valor de negocio | `AVG(conversation_successful)` | ¿Resolvemos realmente la intención del usuario? |
| 3 | **CSAT** | Experiencia de cliente | `AVG(customer_feedback_score)` | ¿Les gusta a los usuarios interactuar con el bot? |
-  **Tiempo promedio de respuesta** Como analisis transversal, cada una de las metricas anteriores se puede complementar analizando el aspecto temporal, por ejemplo si la duracion de la conversacion o tiempo de respuesta afecta el feedback recibido, o la cantidad de consultas derivadas a personas, indicando por ejemplo un problema en solicitudes de mayor duracion o complejidad por perdida de contexto/memoria del LLM.     
                    """, unsafe_allow_html=True)
        st.markdown("> Las tres primeras son las **North‑Star Metrics**; las secundarias se vigilan para detectar la raíz de un cambio inesperado.")

    with st.expander("2️⃣  Patrones / alertas", expanded=True):
        st.markdown("""
- ↑ **Fallback Rate** para un intent → entrenamiento NLU insuficiente.
- ↑ **Escalaciones** en intent específico (`cambiar_vehiculo`) → flujo demasiado complejo.
- **Alta Contención pero bajo GCR** → fallo silencioso: el bot "cree" que resuelve, el usuario no.
- **Drop‑off analysis**: abandono recurrente tras mensaje X → copy confuso o paso engorroso.
- **Cluster de CSAT ≤ 2** concentrado en ciertos intents → priorizar rediseño de esos flows.
        """)

    with st.expander("3️⃣  Visualizaciones para no técnicos", expanded=True):
        st.markdown("""
# Diseño Propuesto del Dashboard

## 1. KPIs Principales (La Vista de 30 Segundos)

- En la parte superior del dashboard, se mostrarán las tres métricas prioritarias:  
  **Tasa de Contención**, **GCR**, **CSAT**, en formato de tarjetas de puntuación (scorecards) grandes.  
- Cada tarjeta mostrará:
  - El valor actual
  - Un indicador de color (**verde/amarillo/rojo**) comparado con el objetivo
  - Una pequeña línea de tendencia que muestre el cambio respecto al período anterior

---

## 2. Rendimiento a lo Largo del Tiempo (La Vista de Tendencia Semanal)

- Un **gráfico de líneas** que muestre la tendencia de los tres KPIs principales durante los últimos **30 o 90 días**.  
  Esto ayuda al equipo a entender los ritmos de rendimiento y el impacto de los cambios implementados.

- Un **gráfico de barras apiladas** que muestre el **volumen total de conversaciones diarias**, desglosado por resultado:
  - `conversation_successful`
  - `transferred_to_agent`
  - `fallo/abandono`  
  Esto proporciona una visión clara del volumen de trabajo del bot y su eficacia.

---

## 3. Diagnóstico Detallado (La Vista de "Dónde Enfocarse Hoy")

- Un **gráfico de barras horizontales** titulado:  
  **"Top 5 Intents que Generan Más Escalados"**  
  Esto informa inmediatamente al equipo de operaciones sobre qué flujos de conversación están causando la mayor carga de trabajo humano.

- Una **tabla o gráfico de barras horizontales** titulado:  
  **"Intents con la Tasa de Éxito Más Baja"**  
  Esto resalta las partes más "rotas" o ineficaces de la lógica del bot, que requieren atención inmediata.
.  

Filtros simples (rango fechas, canal) permiten segmentar sin abrumar al equipo de operaciones.
        """)

    st.divider()

# ==========================================================================================
# TAB 3 – PREGUNTA ABIERTA
# ==========================================================================================
with tab3:
    st.header("Sección 3 – Pregunta Abierta")

    st.subheader("Problema:")
    st.write(
        "En el marco de un servicio de consultoría en RedSalud, trabajé "
        "analizando datos clínicos en BigQuery con tablas que superaban "
        "los 100 millones de registros. Los datos eran crudos, poco "
        "estructurados y altamente voluminosos, lo que dificultaba el "
        "análisis y generaba altos tiempos de consulta."
    )

    st.subheader("Solución:")
    st.write(
        "Estandarizamos y limpiamos los datos, optimizamos las consultas "
        "mediante particiones, índices y transformaciones, y creamos una "
        "capa semántica para facilitar el análisis. Luego aplicamos "
        "modelos de clustering para segmentación interpretable."
    )

    st.subheader("Impacto:")
    st.write(
        "La segmentación permitió diseñar un plan de beneficios con lógica "
        "familiar en lugar de individual, mejorando la eficiencia y la "
        "atención al paciente con campañas preventivas más efectivas. "
        "Se implementaron dashboards funcionales y consultas optimizadas "
        "reutilizables."
    )

    st.markdown(
        "**Adicionalmente**, trabajé en Outlier realizando RLHF, entrenando "
        "modelos de IA conversacional mediante retroalimentación humana en tareas "
        "de codificación y atención general en español e inglés. Esta experiencia "
        "me familiarizó con los criterios de éxito, fallos comunes y métricas clave "
        "para evaluar el desempeño de bots y prompts."
    )

st.sidebar.success("Para abordar este problema opte por generar una base ficticia de datos con las indicaciones, permitiendo verificar el funcionamiento de las consultas, y hacer ejemplos mas concretos, ademas opte por montarlo en streamlit cloud desde git para incorporar herramientas de interes para el puesto✅")




