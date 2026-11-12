import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_VERIFIED = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED = BASE_DIR / "quarantine" / "tampered"
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_SAMPLES_DIR = BASE_DIR / "output_samples"


def configurar_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("PasarelaSHA256")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
    consola = logging.StreamHandler()
    consola.setFormatter(formato)
    archivo = logging.FileHandler(LOGS_DIR / "ejecucion_pipeline.log", mode="w", encoding="utf-8")
    archivo.setFormatter(formato)

    logger.addHandler(consola)
    logger.addHandler(archivo)
    return logger


def calcular_sha256(ruta_fichero: Path) -> str:
    hasher = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def ejecutar_verificacion() -> None:
    logger = configurar_logger()
    STAGING_VERIFIED.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED.mkdir(parents=True, exist_ok=True)
    OUTPUT_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    if not MANIFEST_PATH.exists():
        logger.critical(f"Manifiesto no encontrado: {MANIFEST_PATH}")
        raise FileNotFoundError(MANIFEST_PATH)

    logger.info("=== INICIO FASE 1: VALIDACIÓN CRIPTOGRÁFICA SHA-256 ===")
    validados, rechazados = 0, 0
    resultados = []
    lineas_cuarentena = [
        "MANIFIESTO DE CUARENTENA - PIPELINE DE INTEGRIDAD Y SANEAMIENTO KDD",
        f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 72,
        "[FASE 1 - FICHEROS RECHAZADOS POR INTEGRIDAD SHA-256]",
    ]

    with open(MANIFEST_PATH, "r", encoding="utf-8") as manifest:
        for linea in manifest:
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue

            hash_esperado, nombre_archivo = linea.split(maxsplit=1)
            hash_esperado = hash_esperado.lower()
            nombre_archivo = nombre_archivo.lstrip("*")
            ruta_origen = RAW_DIR / nombre_archivo

            if not ruta_origen.exists():
                logger.error(f"[AUSENTE] '{nombre_archivo}' no existe en {RAW_DIR}/")
                rechazados += 1
                resultados.append({
                    "archivo": nombre_archivo,
                    "estado": "AUSENTE",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": None,
                })
                lineas_cuarentena.append(f"- {nombre_archivo} | Motivo: ARCHIVO_AUSENTE_EN_LOTE")
                continue

            hash_calculado = calcular_sha256(ruta_origen)

            if hash_calculado == hash_esperado:
                shutil.copy2(ruta_origen, STAGING_VERIFIED / nombre_archivo)
                validados += 1
                logger.info(
                    f"[VERIFICADO] {nombre_archivo} -> Promovido a staging/verified/ "
                    f"(SHA-256: {hash_calculado[:16]}...)"
                )
                resultados.append({
                    "archivo": nombre_archivo,
                    "estado": "VERIFICADO",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": hash_calculado,
                })
            else:
                destino = QUARANTINE_TAMPERED / nombre_archivo
                shutil.move(str(ruta_origen), str(destino))
                rechazados += 1
                logger.error(
                    f"[ALERTA SEGURIDAD] Firma discrepante en '{nombre_archivo}'. "
                    f"Esperado: {hash_esperado[:16]}... | Calculado: {hash_calculado[:16]}... "
                    f"-> Segregado a quarantine/tampered/{nombre_archivo}"
                )
                resultados.append({
                    "archivo": nombre_archivo,
                    "estado": "RECHAZADO_HASH_DISCREPANTE",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": hash_calculado,
                })
                lineas_cuarentena.append(
                    f"- {nombre_archivo} | Motivo: FIRMA_SHA256_DISCREPANTE | "
                    f"Esperado: {hash_esperado} | Calculado: {hash_calculado}"
                )

    total = validados + rechazados
    tasa_integridad = round((validados / total) * 100, 2) if total > 0 else 0.0

    auditoria = {
        "timestamp_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "archivos_evaluados": total,
        "archivos_validados": validados,
        "archivos_rechazados": rechazados,
        "tasa_integridad_criptografica_pct": tasa_integridad,
        "detalle_ficheros": resultados,
    }
    (BASE_DIR / "staging" / "audit_fase1.json").write_text(
        json.dumps(auditoria, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (OUTPUT_SAMPLES_DIR / "quarantine_manifest.txt").write_text(
        "\n".join(lineas_cuarentena) + "\n", encoding="utf-8"
    )
    logger.info(f"=== FIN FASE 1 | Validados: {validados}/{total} ({tasa_integridad}%) | Cuarentena: {rechazados} ===")


if __name__ == "__main__":
    ejecutar_verificacion()
