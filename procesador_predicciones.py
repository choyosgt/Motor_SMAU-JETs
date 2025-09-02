#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import sys
import re
import pandas as pd

ALLOWED_COLS = ("predicted_label", "label")
HEADER = "HEADER"
PARENT = "PARENT"
CHILD = "CHILD"
DATA = "DATA"


def _pick_label_column(df: pd.DataFrame) -> str:
    for c in ALLOWED_COLS:
        if c in df.columns:
            return c
    raise ValueError("No se encontró ninguna columna de etiqueta. Se esperaba 'predicted_label' o 'label'.")


def _normalize_labels(s: pd.Series) -> pd.Series:
    # Normaliza SOLO etiquetas (quita espacios extremos)
    return s.astype(str).fillna("").str.strip()


def _raw_text(s: pd.Series) -> pd.Series:
    # Conserva exactamente el texto original (incluye espacios/tabs al inicio y fin)
    return s.astype(str).fillna("")


def _detect_mode(labels_upper: pd.Series) -> str:
    has_child = (labels_upper == CHILD).any()
    has_parent = (labels_upper == PARENT).any()
    has_data = (labels_upper == DATA).any()
    if has_child or has_parent:
        return "HPC"
    if has_data:
        return "HD"
    raise ValueError("No se detectó estructura válida (no hay CHILD/PARENT ni DATA).")


def _collect_headers(texts: pd.Series, labels_upper: pd.Series) -> list[str]:
    """
    Devuelve una lista con los textos de HEADER **únicos**, preservando el orden.
    Normalización para unicidad: lower-case + espacios colapsados (solo para la clave),
    pero se devuelve el texto original tal cual.
    """
    mask = labels_upper == HEADER
    headers_raw = [t for t in texts[mask].tolist() if t != ""]

    seen = set()
    uniques = []
    for t in headers_raw:
        key = re.sub(r"\s+", " ", t.strip().lower())
        if key not in seen:
            seen.add(key)
            uniques.append(t)
    return uniques


def _process_hpc(df: pd.DataFrame, text_col: str, label_col: str) -> list[list[str]]:
    # TEXTO sin recortar; ETIQUETAS normalizadas
    texts = _raw_text(df[text_col])
    labels_upper = _normalize_labels(df[label_col]).str.upper()

    headers = _collect_headers(texts, labels_upper)

    rows: list[list[str]] = []
    if headers:
        rows.append(headers)

    current_parent = ""
    for txt, lab in zip(texts, labels_upper):
        if lab == PARENT:
            current_parent = txt
        elif lab == CHILD:
            rows.append([current_parent, txt])

    return rows


def _process_hd(df: pd.DataFrame, text_col: str, label_col: str) -> list[list[str]]:
    # TEXTO sin recortar; ETIQUETAS normalizadas
    texts = _raw_text(df[text_col])
    labels_upper = _normalize_labels(df[label_col]).str.upper()

    headers = _collect_headers(texts, labels_upper)
    rows: list[list[str]] = []
    if headers:
        rows.append(headers)

    for txt, lab in zip(texts, labels_upper):
        if lab == DATA:
            rows.append([txt])

    return rows


def procesar_csv_entrada(ruta_in: str, ruta_out: str):
    if not os.path.exists(ruta_in):
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {ruta_in}")

    # No activar recortes automáticos: mantener espacios/tabs en 'text'
    df = pd.read_csv(ruta_in, dtype=str, keep_default_na=False)
    text_col = "text" if "text" in df.columns else None
    if text_col is None:
        raise ValueError("El CSV de entrada debe contener una columna 'text'.")

    label_col = _pick_label_column(df)
    labels_upper = _normalize_labels(df[label_col]).str.upper()

    modo = _detect_mode(labels_upper)

    if modo == "HPC":
        rows = _process_hpc(df, text_col, label_col)
    else:
        rows = _process_hd(df, text_col, label_col)

    os.makedirs(os.path.dirname(ruta_out) or ".", exist_ok=True)
    with open(ruta_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        for r in rows:
            writer.writerow(r)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Procesa un CSV de predicciones y genera una tabla estructurada.\n"
            "Preserva espacios y tabs al inicio/fin de cada línea en la columna 'text'."
        )
    )
    parser.add_argument("entrada", help="Ruta del CSV de predicciones (por ejemplo, predicciones_ej2.csv).")
    parser.add_argument("--salida", required=True, help="Ruta del CSV de salida (por ejemplo, resultado_procesado.csv).")
    args = parser.parse_args()

    try:
        procesar_csv_entrada(args.entrada, args.salida)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
