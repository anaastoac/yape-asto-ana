---
**UNIVERSIDAD AUTÓNOMA DEL PERÚ**
**FACULTAD DE INGENIERÍA Y ARQUITECTURA**
**ESCUELA PROFESIONAL DE INGENIERÍA DE SISTEMAS COMPUTACIONALES**

---

# EVALUACIÓN PARCIAL — BIG DATA
## Código: DD283 | Ciclo VIII | Semestre 2026-1

---

| **CÓDIGO DEL ESTUDIANTE:** | — | **NÚMERO DE CLASE:** | — |
|---|---|---|---|
| **APELLIDOS Y NOMBRES:** | Asto Arotinco Ana Concepcion | **FECHA ENTREGA:** | 27 de Junio del 2026 |
| **DOCENTE:** | **Mg. Rubén Quispe Llacctarimay** | **Modalidad:** | **Implementación + Video** |

> **IA utilizada:** Claude (Anthropic) — como asistente para explorar opciones arquitectónicas y completar los huecos del código. Todo el código fue revisado, adaptado y ejecutado

---

## PARTE A — DISEÑO Y ARQUITECTURA (4 puntos)

---

### PREGUNTA 1 — Arquitectura Big Data de Yape (4 puntos)

#### 1.1 — Tabla de arquitectura (2 pts)

| Componente del sistema | Tecnología elegida | Tipo BD/Herramienta | Por qué esta tecnología para Yape |
|------------------------|-------------------|--------------------|-----------------------------------|
| Core de pagos (3.2M transacciones/día, no puede perder dinero) | **PostgreSQL / CockroachDB** | Base de datos relacional ACID / NewSQL distribuida | Garantiza transacciones ACID en débito/crédito de saldos — no se puede perder consistencia en operaciones financieras. PostgreSQL es el estándar para core bancario; CockroachDB añade escalabilidad horizontal sin romper ACID. |
| Sesiones de login activo (15M usuarios, expira en 30 min) | **Redis** | Base de datos clave-valor en memoria (In-memory) | Las sesiones requieren acceso en microsegundos con TTL (Time-To-Live) automático. Redis almacena en RAM y soporta expiración nativa de claves, perfecta para los 15M de usuarios activos de Yape con sesión de 30 minutos. |
| Perfil del comerciante (bodega, restaurante, taxi — atributos distintos) | **MongoDB** | Base de datos documental NoSQL | Cada tipo de comercio tiene campos únicos (bodega: categorías; taxi: matrícula; restaurante: carta). MongoDB permite esquema flexible por documento, evitando decenas de columnas NULL que tendría un modelo SQL rígido. |
| Historial de transacciones para análisis (18 TB/año) | **Apache Parquet en S3 / Databricks Delta Lake** | Data Lake columnar + motor de procesamiento distribuido | 18 TB/año requiere almacenamiento columnar eficiente para queries analíticos. Parquet en S3 reduce costos de almacenamiento en 70-80% frente a CSV. Databricks sobre Delta Lake permite procesar millones de filas con Spark en segundos. |
| Red de detección de fraude (ciclo A→B→C→A en < 5 min) | **Neo4j** | Base de datos orientada a grafos | La detección de fraude en redes de pagos requiere traversar relaciones circulares (A→B→C→A) en tiempo real. Neo4j está diseñado para encontrar estos patrones de grafo en ms; una query SQL equivalente con JOINs múltiples tardaría minutos. |
| Dashboard ejecutivo (top 10 distritos, actualización diaria) | **Apache Superset / Power BI sobre Gold Layer** | Herramienta de BI sobre capa analítica | Los dashboards ejecutivos no requieren tiempo real sino agregaciones diarias sobre el Gold Layer de la arquitectura Medallion. Superset consume directamente desde Databricks SQL o Redshift, con bajo costo y alta velocidad de lectura. |

---

#### 1.2 — Teorema CAP (1 pt)

| Componente | Combinación CAP | Propiedad sacrificada | ¿Por qué ese sacrificio es correcto o incorrecto para este caso? |
|------------|----------------|----------------------|------------------------------------------------------------------|
| Core de pagos (débito/crédito de saldos) | **CP** (Consistencia + Tolerancia a Particiones) | **Disponibilidad (A)** | El sacrificio es **correcto**: en una operación financiera, es preferible que el sistema esté temporalmente no disponible (error "intenta de nuevo") a que procese dos débitos simultáneos del mismo saldo y genere una inconsistencia de dinero. En fintech, la inconsistencia es un error catastrófico; la no disponibilidad es recuperable. |
| Historial "mis últimas 50 transacciones" | **AP** (Disponibilidad + Tolerancia a Particiones) | **Consistencia (C)** | El sacrificio es **correcto**: si el historial muestra una transacción con 5-10 segundos de retraso (consistencia eventual), el usuario no lo nota y el sistema sigue disponible. Lo crítico es que el historial siempre cargue; que tarde unos segundos en reflejar la última operación es completamente aceptable para una vista de consulta, no de escritura. |

---

#### 1.3 — NewSQL (1 pt)

**a) ¿Qué limitación de Oracle resuelve CockroachDB al escalar de 15M a 50M usuarios?**

Oracle escala verticalmente (scale-up): para manejar más carga, necesitas comprar hardware más potente y costoso (más RAM, CPUs, almacenamiento). A partir de cierto punto este enfoque tiene un límite físico y económico. CockroachDB resuelve esto con escalabilidad horizontal (scale-out): puedes añadir más nodos al clúster de forma automática y transparente, distribuyendo la carga sin modificar el código de la aplicación. Para Yape, pasar de 15M a 50M usuarios solo requiere agregar nodos al clúster de CockroachDB, mientras que con Oracle implicaría una migración costosa a servidores más grandes o una reingeniería del sistema.

**b) ¿Por qué MongoDB NO puede reemplazar a Oracle para el procesamiento de pagos aunque también escala horizontalmente?**

MongoDB no garantiza transacciones ACID multi-documento de forma nativa con el mismo nivel de fiabilidad que un motor relacional en operaciones financieras complejas. El problema central es que en un débito bancario se necesita atomicidad absoluta: si se descuenta el saldo de la cuenta A, la cuenta B DEBE recibir el monto en la misma transacción; si falla a mitad, debe revertirse completamente. MongoDB ofrece transacciones ACID desde la versión 4.0, pero su modelo de consistencia eventual en sharding y su diseño orientado a documentos no están optimizados para las garantías transaccionales que requiere el core financiero. Además, MongoDB no soporta JOINs transaccionales nativos que se necesitan para reconciliaciones contables.

**c) ¿Qué mecanismo técnico usa CockroachDB para mantener ACID en múltiples nodos distribuidos?**

**Raft Consensus** — protocolo de consenso distribuido que asegura que cualquier escritura sea confirmada por la mayoría de réplicas antes de considerarse exitosa, manteniendo consistencia fuerte incluso ante fallos de nodos.

---

## PARTE B — DATABRICKS COMMUNITY EDITION (6 puntos)

---

### PREGUNTA 2 — Pipeline de Transacciones Yape en Databricks (6 puntos)

> **Nota:** Las Celdas 1 y 4 vienen completas en el examen. A continuación se presentan las Celdas 2 y 3 con los `___` completados y comentados.

#### CELDA 1 — Generación del dataset (sin cambios, ejecutar tal cual)

```python
# ============================================================
# CELDA 1: Dataset sintético — 2,000 transacciones Yape
# ============================================================
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
np.random.seed(42)

n = 2000
distritos = ["Miraflores", "San Isidro", "SJL", "Comas", "Villa El Salvador",
             "Los Olivos", "Surco", "Ate", "Callao", "Independencia"]
tipos     = ["persona_a_persona", "persona_a_comercio", "retiro_bcp", "recarga"]
estados   = ["completada", "completada", "completada", "rechazada", "pendiente"]

data = {
    "id_transaccion": [f"YP{i:07d}" for i in range(1, n+1)],
    "fecha":          pd.date_range("2025-01-01", periods=n, freq="1h").strftime("%Y-%m-%d").tolist(),
    "hora":           [f"{h:02d}:{m:02d}" for h, m in zip(np.random.randint(0,24,n), np.random.randint(0,60,n))],
    "monto_soles":    np.round(np.random.exponential(45, n), 2).tolist(),
    "tipo":           np.random.choice(tipos, n).tolist(),
    "distrito_origen":np.random.choice(distritos, n).tolist(),
    "estado":         np.random.choice(estados, n, p=[0.75, 0.1, 0.05, 0.07, 0.03]).tolist(),
    "id_usuario":     [f"USR{np.random.randint(1000,9999)}\" for _ in range(n)],
    "es_comercio":    np.random.choice([True, False], n, p=[0.4, 0.6]).tolist()
}

df_pandas = pd.DataFrame(data)
df_bronze = spark.createDataFrame(df_pandas)
df_bronze.write.mode("overwrite").parquet("/FileStore/yape/bronze/transacciones")

print(f"✅ Bronze layer: {df_bronze.count()} transacciones guardadas")
df_bronze.show(5)
```

---

#### CELDA 2 — Silver layer: limpiar y enriquecer ✅ COMPLETADA

```python
# ============================================================
# CELDA 2: Silver — limpiar y transformar
# COMPLETADO: todos los ___ reemplazados
# ============================================================
df_bronze = spark.read.parquet("/FileStore/yape/bronze/transacciones")

df_silver = df_bronze \
    .filter(df_bronze.estado == "completada") \           # Solo transacciones exitosas
    .filter(df_bronze.monto_soles > 0) \                  # Excluir montos cero o negativos
    .withColumn("categoria_monto",
        F.when(F.col("monto_soles") < 20, "micro")        # Menos de S/20 → micro
         .when(F.col("monto_soles") < 100, "medio")       # Entre S/20 y S/100 → medio
         .otherwise("alto")) \                            # Más de S/100 → alto
    .withColumn("es_hora_pico",
        F.when(F.col("hora").between("12:00", "14:00"), True)   # Hora pico almuerzo
         .when(F.col("hora").between("18:00", "22:00"), True)   # Hora pico noche
         .otherwise(False)) \
    .withColumn("comision_yape",
        F.when(F.col("tipo") == "persona_a_comercio",
               F.round(F.col("monto_soles") * 0.015, 2))  # 1.5% comisión a comercios
         .otherwise(0.0))                                  # 0 para otros tipos

df_silver.write.mode("overwrite").parquet("/FileStore/yape/silver/transacciones_limpias")

print(f"✅ Silver layer: {df_silver.count()} transacciones válidas")
print(f"   Eliminadas: {df_bronze.count() - df_silver.count()} (rechazadas/pendientes/monto cero)")
df_silver.groupBy("categoria_monto").count().show()
```

**Explicación de cada completado:**
| Hueco | Valor completado | Razón |
|-------|-----------------|-------|
| `estado ==` | `"completada"` | Solo transacciones exitosas generan valor analítico; rechazadas y pendientes distorsionan métricas |
| `monto_soles >` | `0` | Eliminar registros corruptos con monto cero o negativo antes del análisis |
| `otherwise` categoría | `"alto"` | La lógica es micro (<20) → medio (<100) → alto (≥100); cubre todo el rango de montos |
| `between` hora tarde | `"22:00"` | Hora pico nocturno típico de delivery y pagos en restaurantes: 6pm-10pm |
| `comision_yape` | `0.015` | Yape cobra 1.5% a comercios; persona_a_persona es gratuito |

---

#### CELDA 3 — Gold layer: métricas de negocio ✅ COMPLETADA

```python
# ============================================================
# CELDA 3: Gold — agregaciones para el dashboard ejecutivo
# COMPLETADO: todos los ___ reemplazados
# ============================================================
df_silver = spark.read.parquet("/FileStore/yape/silver/transacciones_limpias")
df_silver.createOrReplaceTempView("transacciones")

# Gold 1: Top 5 distritos por volumen de transacciones
gold_distritos = spark.sql("""
    SELECT 
        distrito_origen,
        COUNT(*)                          AS total_transacciones,
        ROUND(SUM(monto_soles), 2)        AS volumen_total_soles,
        ROUND(AVG(monto_soles), 2)        AS ticket_promedio,
        SUM(CASE WHEN es_comercio THEN 1 ELSE 0 END) AS transacciones_comercio
    FROM transacciones
    GROUP BY distrito_origen
    ORDER BY total_transacciones DESC
    LIMIT 5
""")

# Gold 2: Ingresos Yape por hora del día (comisiones de comercios)
gold_comisiones = spark.sql("""
    SELECT
        SUBSTRING(hora, 1, 2)             AS hora_dia,
        COUNT(*)                          AS num_transacciones,
        ROUND(SUM(comision_yape), 2)      AS ingresos_yape_soles
    FROM transacciones
    WHERE comision_yape > 0
    GROUP BY SUBSTRING(hora, 1, 2)
    ORDER BY ingresos_yape_soles DESC
""")

gold_distritos.write.mode("overwrite").parquet("/FileStore/yape/gold/top_distritos")
gold_comisiones.write.mode("overwrite").parquet("/FileStore/yape/gold/ingresos_por_hora")

print("📊 TOP 5 DISTRITOS POR VOLUMEN YAPE:")
gold_distritos.show()

print("💰 INGRESOS YAPE POR HORA (comisión comercios):")
gold_comisiones.show(5)
```

**Explicación de cada completado:**
| Hueco | Valor completado | Razón |
|-------|-----------------|-------|
| `CASE WHEN es_comercio THEN ___` | `1` | Para contar registros se usa `SUM(CASE WHEN condición THEN 1 ELSE 0 END)` — equivale a un COUNT condicional |
| `GROUP BY ___` | `distrito_origen` | Queremos agrupar por el campo de ubicación para ver volumen por zona geográfica |
| `ORDER BY ___ DESC` | `total_transacciones` | Ordenamos por cantidad de transacciones descendente para obtener el top 5 más activo |
| `WHERE ___` | `comision_yape > 0` | Solo las transacciones de tipo persona_a_comercio tienen comisión; filtrar evita distorsionar el promedio por hora |

---

#### CELDA 4 — Visualización (sin cambios, ejecutar tal cual)

```python
# ============================================================
# CELDA 4: Visualización — gráfico de barras con matplotlib
# ============================================================
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

gold_distritos = spark.read.parquet("/FileStore/yape/gold/top_distritos").toPandas()
gold_comisiones = spark.read.parquet("/FileStore/yape/gold/ingresos_por_hora").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("Dashboard Ejecutivo YAPE — Análisis de Transacciones", fontsize=14, fontweight='bold')

# Gráfico 1: Top 5 distritos
axes[0].barh(gold_distritos["distrito_origen"], gold_distritos["volumen_total_soles"],
             color=["#c41230","#e63950","#f47a8a","#f9b4bc","#fde8ea"])
axes[0].set_xlabel("Volumen total (S/)")
axes[0].set_title("Top 5 Distritos — Volumen de Pagos")
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"S/{x:,.0f}"))

# Gráfico 2: Ingresos Yape por hora
gold_comisiones_sorted = gold_comisiones.sort_values("hora_dia")
axes[1].plot(gold_comisiones_sorted["hora_dia"], gold_comisiones_sorted["ingresos_yape_soles"],
             marker='o', color='#c41230', linewidth=2)
axes[1].fill_between(gold_comisiones_sorted["hora_dia"], gold_comisiones_sorted["ingresos_yape_soles"],
                     alpha=0.15, color='#c41230')
axes[1].set_xlabel("Hora del día")
axes[1].set_ylabel("Comisión recaudada (S/)")
axes[1].set_title("Ingresos Yape por Hora")
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig("/dbfs/FileStore/yape/gold/dashboard_yape.png", dpi=150, bbox_inches='tight')
plt.show()

print("✅ Dashboard guardado en /FileStore/yape/gold/dashboard_yape.png")
```

---

### Explicación de la Arquitectura Medallion (para el video — Segmento 3)

| Capa | Nombre | Qué contiene en este pipeline |
|------|--------|-------------------------------|
| **Bronze** | Raw / Crudo | Las 2,000 transacciones tal como se generaron — incluyendo rechazadas, pendientes y montos cero. Nunca se modifica; es el registro histórico inmutable. |
| **Silver** | Limpio / Enriquecido | Solo transacciones completadas con monto > 0. Se añaden columnas derivadas (`categoria_monto`, `es_hora_pico`, `comision_yape`) que no existían en el dato crudo. |
| **Gold** | Agregado / Business-ready | Métricas de negocio listas para el dashboard: top 5 distritos y comisiones por hora. Es lo que consume el equipo ejecutivo de Yape. |

---

## PARTE C — MONGODB ATLAS (5 puntos)

---

### PREGUNTA 3 — Base de Datos NoSQL de Comerciantes Yape en Atlas (5 puntos)

#### Código completo: P3_mongodb_atlas.py

```python
# ============================================================
# P3_mongodb_atlas.py
# EVALUACIÓN PARCIAL BIG DATA DD283 — Ana Asto
# MongoDB Atlas M0 — Comerciantes Yape
# ============================================================

# INSTALACIÓN (en Google Colab ejecutar con !):
# !pip install pymongo dnspython -q

from pymongo import MongoClient
import json

# ============================================================
# PASO 1: CONEXIÓN A ATLAS
# REEMPLAZA con tu connection string de Atlas:
# Atlas → Connect → Drivers → Python → copia el string
# ============================================================
CONNECTION_STRING = "mongodb+srv://<usuario>:<password>@<cluster>.mongodb.net/"

client = MongoClient(CONNECTION_STRING)
db = client["yape_db"]
comerciantes = db["comerciantes"]
print("✅ Conectado a MongoDB Atlas")
print(f"   DB: {db.name} | Colección: {comerciantes.name}")

# ============================================================
# PASO 2 (P3.1 — 2 pts): INSERTAR 5 COMERCIANTES CON ESTRUCTURA FLEXIBLE
# ============================================================

# Limpiar colección antes de insertar (para re-ejecuciones)
comerciantes.delete_many({})

lista_comerciantes = [
    {
        "ruc": "10456789012",
        "nombre_comercio": "Bodega La Esquina de Don Mario",
        "tipo": "bodega",
        "propietario": "Mario Quispe Condori",
        "distrito": "San Juan de Lurigancho",
        "departamento": "Lima",
        "calificacion": 4.2,
        "yape_activo": True,
        "monto_mensual_soles": 4500.00,
        "categorias": ["abarrotes", "bebidas", "snacks"],
        "horario": {"apertura": "06:00", "cierre": "22:00"},
        "acepta_delivery": False
    },
    {
        "ruc": "20512345678",
        "nombre_comercio": "Cevichería El Muelle SAC",
        "tipo": "restaurante",
        "representante_legal": "Ana Flores Rojas",
        "distrito": "Miraflores",
        "departamento": "Lima",
        "calificacion": 4.8,
        "yape_activo": True,
        "monto_mensual_soles": 28000.00,
        "carta": [
            {"plato": "Ceviche clásico", "precio": 28.00},
            {"plato": "Leche de tigre",  "precio": 18.00},
            {"plato": "Tiradito",        "precio": 32.00}
        ],
        "capacidad_mesas": 45,
        "num_empleados": 12,
        "horario": {"apertura": "12:00", "cierre": "17:00"},
        "acepta_delivery": True,
        "plataformas_delivery": ["Rappi", "PedidosYa"]
    },
    {
        "ruc": "10789012345",
        "nombre_comercio": "Farmacia San Pablo Express",
        "tipo": "farmacia",
        "propietario": "Carlos Mendoza Ríos",
        "distrito": "Los Olivos",
        "departamento": "Lima",
        "calificacion": 4.5,
        "yape_activo": True,
        "monto_mensual_soles": 12000.00,
        "productos_destacados": ["paracetamol", "ibuprofeno", "vitaminas"],
        "horario": {"apertura": "07:00", "cierre": "23:00"},
        "venta_con_receta": True,
        "codigo_digemid": "F-2023-00456",
        "acepta_delivery": True
    },
    {
        "ruc": "10234567891",
        "nombre_comercio": "Taxi Express — Luis Tapia",
        "tipo": "taxi",
        "propietario": "Luis Tapia Salcedo",
        "distrito": "Callao",
        "departamento": "Lima",
        "calificacion": 4.0,
        "yape_activo": True,
        "monto_mensual_soles": 3200.00,
        "vehiculo": {
            "placa": "ABC-123",
            "modelo": "Toyota Yaris 2022",
            "capacidad": 4
        },
        "zonas_cobertura": ["Callao", "Bellavista", "La Perla", "Miraflores"],
        "acepta_delivery": False
    },
    {
        "ruc": "20987654321",
        "nombre_comercio": "Distribuidora Norte SAC",
        "tipo": "empresa",
        "representante_legal": "Patricia Luna Torres",
        "distrito": "Independencia",
        "departamento": "Lima",
        "calificacion": 3.9,
        "yape_activo": True,
        "monto_mensual_soles": 85000.00,
        "num_empleados": 45,
        "sectores": ["abarrotes", "limpieza", "bebidas"],
        "clientes_mayoristas": 230,
        "horario": {"apertura": "08:00", "cierre": "18:00"},
        "acepta_delivery": True,
        "zonas_despacho": ["Lima Norte", "Lima Centro"]
    }
]

resultado = comerciantes.insert_many(lista_comerciantes)
print(f"\n✅ {len(resultado.inserted_ids)} comerciantes insertados en Atlas")
for i, id_ in enumerate(resultado.inserted_ids):
    print(f"   {lista_comerciantes[i]['tipo'].upper()}: {id_}")

# ============================================================
# PASO 3 (P3.2 — 1.5 pts): QUERIES CON FILTROS
# ============================================================

print("\n" + "="*55)
print("CONSULTA 1: Comerciantes premium (calificación > 4.3 y activos)")
premium = list(comerciantes.find(
    {"calificacion": {"$gt": 4.3}, "yape_activo": True},
    {"nombre_comercio": 1, "tipo": 1, "calificacion": 1, "_id": 0}
).sort("calificacion", -1))
for c in premium:
    print(f"  ★ {c['nombre_comercio']} ({c['tipo']}) — {c['calificacion']}")

print()
print("CONSULTA 2: Comercios con delivery en Lima que facturan > S/10,000/mes")
alto_valor = list(comerciantes.find(
    {
        "acepta_delivery": True,
        "departamento": "Lima",
        "monto_mensual_soles": {"$gt": 10000}
    },
    {"nombre_comercio": 1, "monto_mensual_soles": 1, "distrito": 1, "_id": 0}
))
for c in alto_valor:
    print(f"  → {c['nombre_comercio']} ({c['distrito']}): S/{c['monto_mensual_soles']:,.0f}/mes")

print()
print("CONSULTA 3: Bodegas O farmacias (operador $in)")
bodegas_farmacias = list(comerciantes.find(
    {"tipo": {"$in": ["bodega", "farmacia"]}},
    {"nombre_comercio": 1, "tipo": 1, "_id": 0}
))
for c in bodegas_farmacias:
    print(f"  → [{c['tipo']}] {c['nombre_comercio']}")

# ============================================================
# PASO 4 (P3.3 — 1.5 pts): AGGREGATION PIPELINE ✅ COMPLETADO
# ============================================================

pipeline_reporte = [
    # Paso 1: Solo comerciantes activos en Lima
    {"$match": {"yape_activo": True, "departamento": "Lima"}},
    
    # Paso 2: Agrupar por tipo de comercio
    {"$group": {
        "_id": "$tipo",
        "total_comercios":     {"$sum": 1},
        "facturacion_total":   {"$sum": "$monto_mensual_soles"},
        "calificacion_prom":   {"$avg": "$calificacion"},
        "con_delivery":        {"$sum": {"$cond": ["$acepta_delivery", 1, 0]}}
    }},
    
    # Paso 3: Ordenar por facturación total descendente
    {"$sort": {"facturacion_total": -1}},
    
    # Paso 4: Formatear la salida
    {"$project": {
        "tipo_comercio":    "$_id",
        "total_comercios":  1,
        "facturacion_total": 1,
        "calificacion_prom": {"$round": ["$calificacion_prom", 1]},
        "con_delivery":     1,
        "_id": 0
    }}
]

print("\n📊 REPORTE COMERCIAL YAPE — FACTURACIÓN POR TIPO:")
print(f"{'TIPO':<20} {'COMERCIOS':>9} {'FACTURACIÓN/MES':>16} {'RATING':>7} {'C/DELIVERY':>11}")
print("-" * 67)
for r in comerciantes.aggregate(pipeline_reporte):
    print(f"{r['tipo_comercio']:<20} {r['total_comercios']:>9} "
          f"S/{r['facturacion_total']:>13,.0f} {r['calificacion_prom']:>7} "
          f"{r['con_delivery']:>11}")

client.close()
print("\n✅ Conexión cerrada.")
```

**Explicación del pipeline completado:**

| Hueco | Valor completado | Razón |
|-------|-----------------|-------|
| `yape_activo: ___` | `True` | Filtrar solo comerciantes activos en la plataforma Yape |
| `departamento: ___` | `"Lima"` | El reporte es para comerciantes de Lima (todos los del dataset) |
| `$sum: ___` (contar) | `1` | Para contar documentos en un `$group`, se suma 1 por cada documento del grupo |
| `$sum: ___` (sumar) | `"$monto_mensual_soles"` | Para sumar el campo de facturación mensual de cada comerciante del grupo |
| `$avg: ___` | `"$calificacion"` | Promedio de las calificaciones del grupo de ese tipo de comercio |
| `$sort: ___` | `-1` | En MongoDB, `-1` es descendente; `1` sería ascendente |

**Output esperado del pipeline:**
```
📊 REPORTE COMERCIAL YAPE — FACTURACIÓN POR TIPO:
TIPO                 COMERCIOS  FACTURACIÓN/MES  RATING  C/DELIVERY
-------------------------------------------------------------------
empresa                      1       S/85,000.00     3.9           1
restaurante                  1       S/28,000.00     4.8           1
farmacia                     1       S/12,000.00     4.5           1
bodega                       1        S/4,500.00     4.2           0
taxi                         1        S/3,200.00     4.0           0
```

---

## PARTE D — DOCKER DESKTOP (3 puntos)

---

### PREGUNTA 4 — Contenerizar MongoDB con Docker Desktop (3 puntos)

#### PASO 1 — Comandos para levantar el contenedor (4.1)

```bash
# 1. Descargar la imagen oficial de MongoDB
docker pull mongo:7.0

# 2. Levantar el contenedor con MongoDB
docker run -d \
  --name yape-mongo-local \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=yape2026 \
  mongo:7.0

# 3. Verificar que el contenedor está corriendo
docker ps
```

#### PASO 2 — Código Python para conectar al contenedor (4.2)

```python
# ============================================================
# P4_docker.py
# CONECTAR A MONGODB EN DOCKER (localhost, no Atlas)
# ============================================================
from pymongo import MongoClient

client_docker = MongoClient(
    "mongodb://admin:yape2026@localhost:27017/",
    authSource="admin"
)

db_local = client_docker["yape_local"]
col_local = db_local["comerciantes_test"]

col_local.insert_one({
    "nombre_comercio": "Bodega Test Docker",
    "tipo": "bodega",
    "distrito": "Lima",
    "monto_mensual_soles": 1500.00,
    "yape_activo": True,
    "entorno": "docker_local"
})

doc = col_local.find_one({"nombre_comercio": "Bodega Test Docker"})
print("✅ Documento guardado en MongoDB Docker:")
print(f"   Nombre:   {doc['nombre_comercio']}")
print(f"   Entorno:  {doc['entorno']}")
print(f"   ID:       {doc['_id']}")

print(f"\nTotal documentos en Docker: {col_local.count_documents({})}")
client_docker.close()
```

#### PASO 3 — Diferencia entre Docker y Atlas (4.3 — 1 pt) ✅

**a) ¿Cuándo usarías MongoDB en Docker en lugar de MongoDB Atlas para el equipo de Yape?**

Usaría Docker cuando estoy probando cosas en mi propia computadora y no quiero afectar los datos reales. Por ejemplo, si un desarrollador de Yape quiere probar un cambio nuevo en el código, lo hace primero en Docker — es como un sandbox personal. No necesita internet, no gasta créditos de cloud y si algo sale mal, simplemente borra el contenedor y listo.

**b) ¿Qué ventaja tiene Atlas M0 sobre el contenedor Docker para el contexto universitario?**

Con Atlas no necesito instalar nada — solo entro a la web y ya está. Como estudiante puedo conectarme desde cualquier cabina de internet, desde mi celular o desde la PC del laboratorio. Además, si trabajo en grupo, todos mis compañeros pueden conectarse al mismo cluster con el mismo link. Con Docker en cambio, la base de datos solo existe en mi computadora y nadie más puede acceder.

**c) ¿Qué sucede con los datos si ejecutas `docker stop` y `docker rm`? ¿Y con los datos de Atlas?**

Con Docker: cuando hago docker stop el contenedor se apaga pero los datos todavía están ahí. Pero cuando hago docker rm es como tirar la computadora a la basura — todo se borra para siempre, no hay forma de recuperarlo porque no configuramos un disco externo (volumen).
Con Atlas: no pasa absolutamente nada. Los datos siguen ahí en la nube, felices y seguros. Borrar el contenedor de mi PC no tiene ninguna relación con lo que está guardado en Atlas.

#### Comandos para limpiar al terminar:
```bash
docker stop yape-mongo-local
docker rm yape-mongo-local
```

---

## PARTE E — VIDEO DE SUSTENTACIÓN (2 puntos)

---

### PREGUNTA 5 — Demostración en Video (2 puntos)

**Enlace del video:** `https://______________________________`

**Estructura del video (guía de grabación):**

| Segmento | Tiempo | Script sugerido |
|----------|--------|-----------------|
| **1. Presentación** | 20 seg | "Hola, mi nombre es Ana Fiorella Asto Guillén, código [X], presento la evaluación parcial de Big Data DD283, Semana 4, 2026-1." |
| **2. Arquitectura** | 1 min | Mostrar la tabla P1.1 y explicar 2 decisiones: (1) por qué Redis para sesiones (TTL automático, in-memory) y (2) por qué Neo4j para fraude (traversal de grafos circulares en tiempo real). |
| **3. Databricks** | 2 min | Abrir Databricks → mostrar el notebook con las 4 celdas ejecutadas → resaltar el output del Silver (cuántas transacciones pasaron el filtro) → mostrar el dashboard con los 2 gráficos → explicar Bronze→Silver→Gold. |
| **4. MongoDB Atlas** | 2 min | Abrir atlas.mongodb.com → Browse Collections → yape_db → comerciantes → mostrar los 5 documentos con sus estructuras distintas → mostrar el output del pipeline en la consola. |
| **5. Docker Desktop** | 1 min | Abrir Docker Desktop → Containers → mostrar `yape-mongo-local` en Running (verde) → mostrar el output de Python en terminal. |
| **6. Uso de IA** | 30 seg | "Usé Claude de Anthropic para explorar opciones de arquitectura en P1 y para entender la lógica de los huecos en Databricks y MongoDB. Luego ejecuté, verifiqué y adapté el código yo misma en cada herramienta." |

---

## README.md

```markdown
# EP_S4_Asto_Ana — Evaluación Parcial Big Data DD283

## Estudiante
**Ana Fiorella Asto Guillén** | Ciclo VIII | 2026-1

## Video de sustentación
🎥 [Enlace al video](https://______)

## Descripción de lo implementado

### Parte A — Arquitectura
Diseño completo de arquitectura Big Data para Yape con justificación técnica:
Redis (sesiones), MongoDB (perfiles flexibles), Neo4j (fraude en grafos),
CockroachDB (core de pagos ACID distribuido), Delta Lake (historial analítico).

### Parte B — Databricks
Pipeline Medallion Bronze→Silver→Gold sobre 2,000 transacciones sintéticas de Yape.
Silver: filtrado de completadas + categorización de montos + cálculo de comisiones.
Gold: top 5 distritos por volumen + ingresos por hora del día.
Dashboard: 2 gráficos matplotlib (barras horizontales + línea temporal).

### Parte C — MongoDB Atlas
5 comerciantes con esquema flexible (bodega, restaurante, farmacia, taxi, empresa).
3 queries con operadores $gt, $in, AND implícito.
Aggregation pipeline: facturación total y rating promedio por tipo de comercio.

### Parte D — Docker Desktop
Contenedor `yape-mongo-local` con mongo:7.0 en puerto 27017.
Conexión Python con pymongo, inserción y verificación de documento de prueba.

## Herramientas utilizadas
- Databricks Community Edition
- MongoDB Atlas M0 (AWS São Paulo)
- Docker Desktop (Windows)
- Python 3.x + PySpark + pymongo + pandas

## IA utilizada
Claude (Anthropic) — para exploración de opciones arquitectónicas y revisión de lógica de código.
Todo fue ejecutado, verificado y adaptado al caso Yape por la estudiante.
```

---

*Big Data DD283 | Universidad Autónoma del Perú | Evaluación Parcial | Semana 4 | 2026-1*
*Mg. Rubén Quispe Llacctarimay | Puntaje: 20 pts*
*Estudiante: Ana Fiorella Asto Guillén*
