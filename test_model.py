import os
import re
import json
import pickle
import argparse
import numpy as np
import pandas as pd
import csv

from typing import Optional, List

# Si el modelo se entrenó con XGBClassifier, no necesitas usar xgboost aquí,
# pero lo importamos por si el pickle hace referencia interna.
try:
    import xgboost as xgb  # noqa: F401
except Exception:
    pass

from features import DocumentFeatureExtractor


class DocumentTester:
    def __init__(self, model_path: str = "modelo"):
        self.model_path = model_path
        self.model = None
        self.label_encoder = None
        self.feature_names: Optional[List[str]] = None
        self.model_info = {}
        self.feature_extractor = DocumentFeatureExtractor()

    # ---------------------------
    # Carga de artefactos
    # ---------------------------
    def load_model(self):
        """Carga el modelo entrenado y sus componentes."""
        print("Cargando modelo entrenado...")

        # Modelo
        model_file = os.path.join(self.model_path, "model.pkl")
        if not os.path.exists(model_file):
            raise FileNotFoundError(f"No se encontró el modelo en {model_file}")
        with open(model_file, "rb") as f:
            self.model = pickle.load(f)

        # LabelEncoder
        encoder_file = os.path.join(self.model_path, "label_encoder.pkl")
        if not os.path.exists(encoder_file):
            raise FileNotFoundError(f"No se encontró el label encoder en {encoder_file}")
        with open(encoder_file, "rb") as f:
            self.label_encoder = pickle.load(f)

        # Nombres de features
        features_file = os.path.join(self.model_path, "feature_names.txt")
        if not os.path.exists(features_file):
            raise FileNotFoundError(f"No se encontró feature_names.txt en {features_file}")
        with open(features_file, "r", encoding="utf-8") as f:
            self.feature_names = [line.strip() for line in f if line.strip()]

        # Info del modelo (opcional pero recomendado)
        info_file = os.path.join(self.model_path, "model_info.json")
        if os.path.exists(info_file):
            with open(info_file, "r", encoding="utf-8") as f:
                self.model_info = json.load(f)
        else:
            # Construimos un mínimo para mostrar
            self.model_info = {
                "model_type": type(self.model).__name__,
                "num_features": len(self.feature_names),
                "classes": list(map(str, getattr(self.label_encoder, "classes_", []))),
            }

        print("Modelo cargado exitosamente:")
        print(f"- Tipo: {self.model_info.get('model_type')}")
        print(f"- Features: {self.model_info.get('num_features')}")
        print(f"- Clases: {self.model_info.get('classes')}")

    # ---------------------------
    # Carga de archivo de test
    # ---------------------------
    def _read_text_file(self, file_path: str, encoding: str = "utf-8") -> pd.DataFrame:
        # Leemos líneas y construimos DF similar al entrenamiento
        with open(file_path, "r", encoding=encoding, errors="replace") as f:
            lines = f.readlines()

        test_data = []
        base = os.path.basename(file_path)
        for i, line in enumerate(lines, 1):
            test_data.append(
                {
                    "file": base,
                    "line_no": i,
                    "text": line.rstrip("\r\n"),
                    "label": "UNKNOWN",
                }
            )
        return pd.DataFrame(test_data)

    def _sniff_encoding(self, file_path: str) -> str | None:
        # Detecta por BOM: utf-16le / utf-16be / utf-8-sig
        with open(file_path, "rb") as fb:
            head = fb.read(4)
        if head.startswith(b"\xff\xfe"):
            return "utf-16le"
        if head.startswith(b"\xfe\xff"):
            return "utf-16be"
        if head.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        return None


    def _read_csv_or_excel(self, file_path: str) -> pd.DataFrame:
        ext = os.path.splitext(file_path)[1].lower()

        if ext in [".xlsx", ".xls"]:
            # Leer Excel y normalizar al esquema estándar esperado por el pipeline
            df = pd.read_excel(file_path, dtype=str, header=None).fillna("")
            if "text" in df.columns:
                texts = df["text"].astype(str).tolist()
            else:
                if df.shape[1] == 1:
                    # Hoja con una sola columna: úsala como texto
                    texts = df.iloc[:, 0].astype(str).tolist()
                else:
                    # Varias columnas: unirlas en una sola línea (sin recortar)
                    texts = df.astype(str).agg(" ".join, axis=1).tolist()
            base = os.path.basename(file_path)
            return pd.DataFrame(
                {
                    "file": base,
                    "line_no": np.arange(1, len(texts) + 1),
                    "text": texts,
                    "label": "UNKNOWN",
                }
            )

        # CSV plano: probamos codificación + separador de forma robusta
        sniff = self._sniff_encoding(file_path)
        enc_candidates = ([sniff] if sniff else []) + ["utf-16", "utf-8", "latin-1", "cp1252"]
        sep_candidates = [None, ",", ";", "\t", "|"]

        last_err = None
        for enc in enc_candidates:
            for sep in sep_candidates:
                try:
                    df = pd.read_csv(
                        file_path,
                        dtype=str,
                        sep=sep,
                        encoding=enc,
                        engine="python",
                    )
                    # Éxito: normalizamos y salimos
                    df = df.fillna("")
                    if "text" in df.columns:
                        texts = df["text"].astype(str).tolist()
                    else:
                        texts = df.astype(str).agg(" ".join, axis=1).tolist()

                    base = os.path.basename(file_path)
                    return pd.DataFrame(
                        {
                            "file": base,
                            "line_no": np.arange(1, len(texts) + 1),
                            "text": texts,
                            "label": "UNKNOWN",
                        }
                    )
                except Exception as e:
                    last_err = e

        # Si ninguna combinación funcionó, propagamos el último error
        raise last_err

    def load_test_file(self, file_path: str, encoding: str = "utf-8") -> pd.DataFrame:
        """Carga el archivo de test y lo prepara para predicción."""
        print(f"Cargando archivo de test: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

        ext = os.path.splitext(file_path)[1].lower()
        if ext in [".txt", ".log"]:
            df = self._read_text_file(file_path, encoding=encoding)
        elif ext in [".csv", ".xlsx", ".xls"]:
            df = self._read_csv_or_excel(file_path)
        else:
            # Como fallback, lo tratamos como texto plano
            df = self._read_text_file(file_path, encoding=encoding)

        print(f"Archivo cargado: {len(df)} líneas")
        return df

    # ---------------------------
    # Predicción
    # ---------------------------
    def _align_features(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Alinea y ordena las columnas de features como en entrenamiento."""
        # Asegura todas las columnas esperadas
        missing = [c for c in self.feature_names if c not in features_df.columns]
        if missing:
            # Estas features faltantes se imputan con 0 (la mayoría son contadores/bools)
            for c in missing:
                features_df[c] = 0

        # Elimina columnas extra no vistas en entrenamiento (por seguridad)
        extra = [c for c in features_df.columns if c not in self.feature_names]
        if extra:
            features_df = features_df.drop(columns=extra)

        # Reordena exactamente como el entrenamiento
        features_df = features_df[self.feature_names]

        # Asegura tipos numéricos (si alguna quedó como object por accidente)
        for col in features_df.columns:
            if features_df[col].dtype == "object":
                features_df[col] = pd.to_numeric(features_df[col], errors="coerce").fillna(0)

        # Evita NaN residuales
        features_df = features_df.fillna(0)
        return features_df

    def predict_file(self, test_df: pd.DataFrame) -> pd.DataFrame:
        """Realiza predicciones en el archivo de test."""
        print("Extrayendo features del archivo de test...")
        features_df = self.feature_extractor.extract_all_features(test_df).copy()
        features_df = features_df.reset_index(drop=True)
        test_df = test_df.reset_index(drop=True)

        # Verificar/forzar compatibilidad con las features del modelo
        if self.feature_names is None:
            raise RuntimeError("feature_names no cargados. Llama primero a load_model().")

        if list(features_df.columns) != self.feature_names:
            print("Advertencia: las features no coinciden exactamente. Realineando columnas...")
        features_df = self._align_features(features_df)

        print("Realizando predicciones...")
        # XGBClassifier (scikit wrapper) soporta predict_proba
        preds = self.model.predict(features_df)
        # A veces los modelos devuelven floats -> convertimos a int para inverse_transform
        if preds.dtype != np.int64 and preds.dtype != np.int32:
            preds = preds.astype(int)

        # Probabilidades (si existen)
        if hasattr(self.model, "predict_proba"):
            probas = self.model.predict_proba(features_df)
        else:
            # Fallback: si no hay predict_proba, construimos algo básico
            # (no ideal, pero evita romper el flujo)
            probas = np.zeros((len(preds), len(self.label_encoder.classes_)))
            for i, p in enumerate(preds):
                probas[i, p] = 1.0

        # Decodificar etiquetas
        predicted_labels = self.label_encoder.inverse_transform(preds)

        # Construir resultados
        results_df = test_df.copy()
        results_df["predicted_label"] = predicted_labels
        results_df["confidence"] = np.max(probas, axis=1)

        # Probabilidad por clase
        for i, class_name in enumerate(self.label_encoder.classes_):
            results_df[f"prob_{class_name}"] = probas[:, i]

        return results_df

    # ---------------------------
    # Análisis
    # ---------------------------
    def analyze_predictions(self, results_df: pd.DataFrame, low_conf: float = 0.7, max_examples: int = 5):
        """Analiza los resultados de las predicciones (estadísticos y ejemplos)."""
        print("\n" + "=" * 60)
        print("ANÁLISIS DE PREDICCIONES")
        print("=" * 60)

        # Distribución
        label_counts = results_df["predicted_label"].value_counts()
        print("\nDistribución de etiquetas predichas:")
        total = len(results_df)
        for label, count in label_counts.items():
            percentage = (count / total) * 100 if total else 0.0
            print(f"  {label}: {count} líneas ({percentage:.1f}%)")

        # Confianza
        conf = results_df["confidence"]
        print("\nEstadísticas de confianza:")
        print(f"  Promedio: {conf.mean():.3f}")
        print(f"  Mínimo:   {conf.min():.3f}")
        print(f"  Máximo:   {conf.max():.3f}")

        # Baja confianza
        low_conf_df = results_df[results_df["confidence"] < low_conf]
        if len(low_conf_df) > 0:
            print(f"\nLíneas con baja confianza (<{low_conf}): {len(low_conf_df)}")
            print("Ejemplos:")
            for _, row in low_conf_df.head(max_examples).iterrows():
                txt = row["text"]
                preview = (txt[:80] + "...") if isinstance(txt, str) and len(txt) > 80 else txt
                print(f"  Línea {row['line_no']}: {row['predicted_label']} ({row['confidence']:.3f}) - {preview}")

        # Ejemplos por categoría
        print("\nEjemplos de predicciones por categoría:")
        for label in label_counts.index[:5]:  # hasta 5 categorías
            examples = results_df[results_df["predicted_label"] == label].head(min(3, label_counts[label]))
            print(f"\n{label}:")
            for _, row in examples.iterrows():
                txt = row["text"]
                preview = (txt[:100] + "...") if isinstance(txt, str) and len(txt) > 100 else txt
                print(f"  Línea {row['line_no']} ({row['confidence']:.3f}): {preview}")

    # ---------------------------
    # Guardado
    # ---------------------------
    def save_results(self, results_df, output_file: str = "resultados_prediccion.csv"):
        """Guarda los resultados en CSV (simple y detallado) con comillas en TODAS las celdas."""
        print(f"\nGuardando resultados en {output_file}...")

        out_dir = os.path.dirname(output_file) or "."
        os.makedirs(out_dir, exist_ok=True)

        # SIEMPRE comillas en todas las celdas
        quoting_kwargs = {"quoting": csv.QUOTE_ALL}

        # Archivo simple
        simple_cols = ["file", "line_no", "text", "predicted_label", "confidence"]
        results_df[simple_cols].to_csv(
            output_file,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            **quoting_kwargs,
        )
        print("Resultados guardados (simple).")

        # Archivo detallado
        detailed_file = output_file.replace(".csv", "_detailed.csv")
        results_df.to_csv(
            detailed_file,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            **quoting_kwargs,
        )
        print(f"Resultados detallados guardados en {detailed_file}")


# ---------------------------
# CLI
# ---------------------------
def build_argparser():
    parser = argparse.ArgumentParser(
        description="Probar un modelo de clasificación de líneas de Libro Diario."
    )
    parser.add_argument("--model-dir", default="modelo", help="Directorio con el modelo entrenado.")
    parser.add_argument("--file", required=True, help="Ruta del archivo de prueba (.txt, .csv, .xlsx).")
    parser.add_argument("--out", default="resultados_prediccion.csv", help="Archivo CSV de salida (simple).")
    parser.add_argument("--encoding", default="utf-8", help="Codificación para archivos .txt (por defecto utf-8).")
    parser.add_argument("--low-conf", type=float, default=0.7, help="Umbral de baja confianza para el análisis.")
    parser.add_argument("--max-examples", type=int, default=5, help="Máx. ejemplos a mostrar en análisis.")
    return parser


def main():
    args = build_argparser().parse_args()

    tester = DocumentTester(model_path=args.model_dir)
    tester.load_model()

    test_df = tester.load_test_file(args.file, encoding=args.encoding)
    results_df = tester.predict_file(test_df)

    tester.analyze_predictions(results_df, low_conf=args.low_conf, max_examples=args.max_examples)
    tester.save_results(results_df, output_file=args.out)


if __name__ == "__main__":
    main()
