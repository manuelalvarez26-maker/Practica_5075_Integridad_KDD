#!/usr/bin/env python3
"""
Fase 1: Pasarela de Integridad Criptográfica SHA-256 por Lotes.
Cubre CE 3.a y CE 3.b (Real Decreto 279/2021).
"""

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

RAW_DIR = Path("raw_batch_202611")
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_VERIFIED = Path("staging/verified")
QUARANTINE_TAMPERED = Path("quarantine/tampered")
LOGS_DIR = Path("logs")


def configurar_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("PasarelaSHA256")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
    consola = logging.StreamHandler()
    consola.setFormatter(formato)
    archivo = logging.FileHandler(LOGS_DIR / "auditoria_pipeline.log", mode="w", encoding="utf-8")
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

    if not MANIFEST_PATH.exists():
        logger.critical(f"Manifiesto no encontrado: {MANIFEST_PATH}")
        raise FileNotFoundError(MANIFEST_PATH)

    logger.info("=== INICIO FASE 1: VALIDACIÓN CRIPTOGRÁFICA SHA-256 ===")
    validados, rechazados = 0, 0
    resultados = []

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
                continue

            hash_calculado = calcular_sha256(ruta_origen)

            if hash_calculado == hash_esperado:
                shutil.copy2(ruta_origen, STAGING_VERIFIED / nombre_archivo)
                validados += 1
                logger.info(
                    f"[VERIFICADO] {nombre_archivo} -> Promovido a {STAGING_VERIFIED}/ "
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
                    f"-> Segregado a {destino}"
                )
                resultados.append({
                    "archivo": nombre_archivo,
                    "estado": "RECHAZADO_HASH_DISCREPANTE",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": hash_calculado,
                })

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
    Path("staging/audit_fase1.json").write_text(
        json.dumps(auditoria, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info(f"=== FIN FASE 1 | Validados: {validados}/{total} ({tasa_integridad}%) | Cuarentena: {rechazados} ===")


if __name__ == "__main__":
    ejecutar_verificacion()