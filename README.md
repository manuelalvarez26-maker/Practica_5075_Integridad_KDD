# Practica_5075_Integridad_KDD

Pipeline automatizado en Python para la validación de integridad criptográfica por lotes mediante **SHA-256**, saneamiento de anomalías, detección estadística de valores atípicos (*outliers*) y perfilado de calidad de datos bajo la norma **ISO/IEC 25012**, orientado a entornos de Business Intelligence (BI) y minería de datos (**KDD**).

---

## 1. Estructura del Repositorio

```text
Practica_5075_Integridad_KDD/
├── scripts/
│   ├── 01_verificar_integridad.py
│   └── 02_pipeline_saneamiento.py
├── logs/
│   └── ejecucion_pipeline.log
├── reports/
│   └── INFORME_CALIDAD_KDD.md
└── output_samples/
    ├── quarantine_manifest.txt
    └── metricas_perfilado.json
```

*(Nota: Los directorios operativos de trabajo local `raw_batch_202611/`, `staging/`, `quarantine/` y `processed/` se excluyen del control de versiones mediante `.gitignore`, conservando en el repositorio los scripts, registros de auditoría, informes y muestras de salida requeridas).*

---

## 2. Documentación Técnica de los Scripts

### Fase 1: `scripts/01_verificar_integridad.py`
Actúa como pasarela criptográfica de admisión (*data quality gate*) antes de procesar el contenido de los ficheros transaccionales:
- **`configurar_logger()`**: Inicializa el sistema de auditoría dual por consola y archivo en `logs/ejecucion_pipeline.log`.
- **`calcular_sha256(ruta_fichero)`**: Lee cada archivo en bloques binarios de 64 KB (`65536` bytes) para calcular su resumen hexadecimal SHA-256 de forma eficiente en memoria.
- **`ejecutar_verificacion()`**: Contrasta cada fichero listado en `raw_batch_202611/manifest_checksums.sha256` frente a su huella calculada:
  - **Archivos íntegros (`transacciones_tienda_norte.csv` y `transacciones_tienda_este.csv`)**: Se promueven a la zona intermedia `staging/verified/`.
  - **Archivos alterados (`transacciones_tienda_sur.csv`)**: Se aíslan automáticamente en `quarantine/tampered/`, emitiendo una alerta de seguridad en el log y registrando la incidencia en `output_samples/quarantine_manifest.txt`.
  - **Auditoría de fase**: Genera el resumen cuantitativo en `staging/audit_fase1.json` (tasa de integridad criptográfica del **66,67 %**).

### Fases 2 y 3: `scripts/02_pipeline_saneamiento.py`
Ejecuta la limpieza, validación de dominio, detección de *outliers* y generación de métricas ISO/IEC 25012 sobre los ficheros verificados en `staging/verified/`:
- **`ejecutar_pipeline_kdd()`**: Ingesta y concatena los datos verificados (**100 registros** iniciales) aplicando secuencialmente las siguientes reglas sin detener el flujo operativo:
  1. **Completitud en atributos críticos**: Segrega a `quarantine/bad_records.csv` las filas con valores nulos en columnas obligatorias (`id_transaccion`, `timestamp`, `importe_eur`, `unidades`) — **3 registros rechazados**.
  2. **Unicidad**: Detecta y aísla duplicados de clave primaria (`id_transaccion`).
  3. **Coherencia temporal (ISO 8601)**: Convierte `timestamp` al estándar `YYYY-MM-DD HH:MM:SS` y descarta registros con cadenas corruptas, fechas futuras o fuera del ejercicio en curso — **3 registros rechazados**.
  4. **Reglas de dominio de negocio**: Filtra transacciones con `importe_eur <= 0` o `unidades < 1` — **4 registros rechazados**.
  5. **Detección robusta de outliers**: Calcula el rango intercuartílico ($IQR = Q_3 - Q_1$) sobre `importe_eur` y segrega valores extremos fuera del intervalo $[Q_1 - 3 \times IQR,\ Q_3 + 3 \times IQR]$ ($[-44{,}85\text{ €},\ 188{,}65\text{ €}]$) — **3 registros rechazados**.
  6. **Imputación de variables secundarias**: Completa valores ausentes en atributos categóricos no críticos mediante la **moda** (`Electronica` en `categoria` y `Transferencia` en `metodo_pago`) para preservar el tamaño muestral.
  7. **Perfilado KDD y entrega a BI**: Exporta el dataset saneado (`87` transacciones conformes) en `processed/dataset_kdd_listo.csv`, actualiza `output_samples/quarantine_manifest.txt`, guarda las métricas estructuradas en `output_samples/metricas_perfilado.json` y redacta el informe técnico en `reports/INFORME_CALIDAD_KDD.md`.

---

## 3. Requisitos e Instrucciones de Ejecución

### Dependencias
- Python 3.10 o superior
- Librería `pandas` (`pip install pandas`)

### Ejecución paso a paso
Situado en la carpeta raíz del proyecto (donde reside `raw_batch_202611/`):

```bash
# 1. Ejecutar pasarela criptográfica SHA-256 (Fase 1)
python scripts/01_verificar_integridad.py

# 2. Ejecutar saneamiento, control de outliers y perfilado KDD (Fases 2 y 3)
python scripts/02_pipeline_saneamiento.py
```

---

## 4. Resumen de Resultados Obtenidos (ISO/IEC 25012)

- **Integridad criptográfica**: 2 de 3 ficheros validados (**66,67 %**); `transacciones_tienda_sur.csv` bloqueado en cuarentena por discrepancia de firma SHA-256.
- **Completitud post-limpieza**: **100,00 %** (87/87 registros sin valores nulos).
- **Unicidad post-limpieza**: **100,00 %** en `id_transaccion`.
- **Validez de registros**: **87,00 %** (87 registros conformes para BI y 13 registros anómalos aislados en cuarentena).
- **Estabilización de `importe_eur`**: Media de **71,79 €**, mediana de **73,44 €**, rango intercuartílico $[53{,}78\text{ €},\ 86{,}70\text{ €}]$ y desviación estándar reducida de **528,78 €** a **26,82 €**.
