# Informe Técnico de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

**Fecha de ejecución:** 2026-11-12 19:30:49  
**Dataset resultante:** `processed/dataset_kdd_listo.csv` (87 transacciones conformes)  
**Cuarentena de registros:** `quarantine/bad_records.csv` (13 transacciones segregadas)  

---

## 1. Métricas de Calidad ISO/IEC 25012

| Dimensión / Indicador | Valor Previo | Valor Post-Pipeline | Estado |
| :--- | :---: | :---: | :---: |
| **Integridad Criptográfica (SHA-256)** | 2/3 ficheros | **66.67%** | Aprobado (`Sur` bloqueado) |
| **Completitud (Registros sin nulos)** | 95.0% (95/100) | **100.0%** (87/87) | Conforme |
| **Unicidad (`id_transaccion`)** | 98.0% (98/100) | **100.0%** (87/87) | Conforme |
| **Exactitud / Validez de Dominio** | — | **87.0%** (87/100 filas) | Conforme |

---

## 2. Distribución Estadística de `importe_eur` (Post-Limpieza)

| Estadístico | Valor (EUR) |
| :--- | :---: |
| **Media aritmética** | 70.99 € |
| **Mediana (Percentil 50)** | 73.44 € |
| **Desviación estándar ($s$)** | 26.34 € |
| **Percentil 25 ($Q_1$)** | 53.8 € |
| **Percentil 75 ($Q_3$)** | 85.34 € |
| **Rango observado (Mín – Máx)** | 12.5 € – 124.47 € |

---

## 3. Auditoría de Segregación en Cuarentena

- **Ficheros bloqueados en Fase 1 (`quarantine/tampered/`):** `transacciones_tienda_sur.csv` (hash SHA-256 calculado `1c47dff4bf34eff5...` discrepante respecto al manifiesto `0773c2cde0d45d3b...`).
- **Registros bloqueados en Fase 2 (`quarantine/bad_records.csv`):** 13 registros aislados sin interrumpir el pipeline:
- **VIOLACION_REGLA_DOMINIO_IMPORTE_O_UNIDADES**: 4 registro(s)
- **NULO_EN_COLUMNA_OBLIGATORIA**: 3 registro(s)
- **TIMESTAMP_CORRUPTO_O_FUERA_DE_ANIO_EN_CURSO**: 3 registro(s)
- **OUTLIER_ESTADISTICO_IMPORTE (fuera de [-44.85, 188.65] EUR)**: 3 registro(s)

---

## 4. Conclusiones Técnicas para Minería de Datos (Proceso KDD)

1. **Bloqueo de alteraciones en tránsito (Fase 1):** La verificación SHA-256 aisló `transacciones_tienda_sur.csv` antes de su ingesta, impidiendo que registros manipulados (`TXS-20261110-0005` con `9999.99 €` y `TXS-20261110-0020` con método de pago `ALTERADO_NO_AUTORIZADO`) contaminaran los cubos OLAP.
2. **Criterio diferenciado ante valores ausentes (Completitud):** Se descartaron a cuarentena las 3 transacciones con nulos en columnas críticas (`id_transaccion` ausente en 2 filas e `importe_eur` nulo en `TXE-20261110-0034`) al romper la trazabilidad contable, mientras que los 2 nulos en variables categóricas secundarias (`categoria` en `TXE-20261110-0007` y `metodo_pago` en `TXE-20261110-0015`) se imputaron mediante la **moda** (`Textil` y `Transferencia`, respectivamente) para preservar el tamaño muestral sin alterar la distribución marginal.
3. **Estabilización estadística para modelos de Minería de Datos:** La exclusión de fechas corruptas (`2024-02-11`, `2027-08-21`, `2026-99-99 INVALID_TS`), importes no positivos (`-34.50 €`, `0.00 €`), unidades inválidas (`0`, `-3`) y los 3 *outliers* extremos detectados por $3 \times IQR$ (`1480.00 €`, `2750.90 €`, `4120.00 €`) reduce la desviación estándar de `importe_eur` de **528.78 €** a **26.82 €** y evita sesgos graves en algoritmos de *clustering* (*K-Means*), regresión o detección de patrones de compra.
