# Informe TÃ©cnico de Calidad de Datos y PreparaciÃ³n KDD (ISO/IEC 25012)

**Fecha de ejecuciÃ³n:** 2026-11-12 20:07:05  
**Dataset resultante:** `processed/dataset_kdd_listo.csv` (87 transacciones conformes)  
**Manifiesto de cuarentena:** `output_samples/quarantine_manifest.txt` (13 transacciones segregadas)  

---

## 1. MÃ©tricas de Calidad ISO/IEC 25012

| DimensiÃ³n / Indicador | Valor Previo | Valor Post-Pipeline | Estado |
| :--- | :---: | :---: | :---: |
| **Integridad CriptogrÃ¡fica (SHA-256)** | 2/3 ficheros | **66.67%** | Aprobado (`Sur` bloqueado) |
| **Completitud (Registros sin nulos)** | 95.0% (95/100) | **100.0%** (87/87) | Conforme |
| **Unicidad (`id_transaccion`)** | 98.0% (98/100) | **100.0%** (87/87) | Conforme |
| **Exactitud / Validez de Dominio** | â€” | **87.0%** (87/100 filas) | Conforme |

---

## 2. DistribuciÃ³n EstadÃ­stica de `importe_eur` (Post-Limpieza)

| EstadÃ­stico | Valor (EUR) |
| :--- | :---: |
| **Media aritmÃ©tica** | 70.99 â‚¬ |
| **Mediana (Percentil 50)** | 73.44 â‚¬ |
| **DesviaciÃ³n estÃ¡ndar ($s$)** | 26.34 â‚¬ |
| **Percentil 25 ($Q_1$)** | 53.8 â‚¬ |
| **Percentil 75 ($Q_3$)** | 85.34 â‚¬ |
| **Rango observado (MÃ­n â€“ MÃ¡x)** | 12.5 â‚¬ â€“ 124.47 â‚¬ |

---

## 3. AuditorÃ­a de SegregaciÃ³n en Cuarentena

- **Ficheros bloqueados en Fase 1 (`quarantine/tampered/`):** `transacciones_tienda_sur.csv` (hash SHA-256 calculado `1c47dff4bf34eff5...` discrepante respecto al manifiesto `0773c2cde0d45d3b...`).
- **Registros bloqueados en Fase 2 (`quarantine/bad_records.csv`):** 13 registros aislados sin interrumpir el pipeline:
- **VIOLACION_REGLA_DOMINIO_IMPORTE_O_UNIDADES**: 4 registro(s)
- **NULO_EN_COLUMNA_OBLIGATORIA**: 3 registro(s)
- **TIMESTAMP_CORRUPTO_O_FUERA_DE_ANIO_EN_CURSO**: 3 registro(s)
- **OUTLIER_ESTADISTICO_IMPORTE (fuera de [-44.85, 188.65] EUR)**: 3 registro(s)

---

## 4. Conclusiones TÃ©cnicas para MinerÃ­a de Datos (Proceso KDD)

1. **Bloqueo de alteraciones en trÃ¡nsito (Fase 1):** La verificaciÃ³n SHA-256 aislÃ³ `transacciones_tienda_sur.csv` antes de su ingesta, impidiendo que registros manipulados (`TXS-20261110-0005` con `9999.99 â‚¬` y `TXS-20261110-0020` con mÃ©todo de pago `ALTERADO_NO_AUTORIZADO`) contaminaran los cubos OLAP.
2. **Criterio diferenciado ante valores ausentes (Completitud):** Se descartaron a cuarentena las 3 transacciones con nulos en columnas crÃ­ticas (`id_transaccion` ausente en 2 filas e `importe_eur` nulo en `TXE-20261110-0034`) al romper la trazabilidad contable, mientras que los 2 nulos en variables categÃ³ricas secundarias (`categoria` en `TXE-20261110-0007` y `metodo_pago` en `TXE-20261110-0015`) se imputaron mediante la **moda** (`Electronica` y `Transferencia`, respectivamente) para preservar el tamaÃ±o muestral sin alterar la distribuciÃ³n marginal.
3. **EstabilizaciÃ³n estadÃ­stica para modelos de MinerÃ­a de Datos:** La exclusiÃ³n de fechas corruptas (`2024-02-11`, `2027-08-21`, `2026-99-99 INVALID_TS`), importes no positivos (`-34.50 â‚¬`, `0.00 â‚¬`), unidades invÃ¡lidas (`0`, `-3`) y los 3 *outliers* extremos detectados por $3 \times IQR$ (`1480.00 â‚¬`, `2750.90 â‚¬`, `4120.00 â‚¬`) reduce la desviaciÃ³n estÃ¡ndar de `importe_eur` de **528.78 â‚¬** a **26.82 â‚¬** y evita sesgos graves en algoritmos de *clustering* (*K-Means*), regresiÃ³n o detecciÃ³n de patrones de compra.

