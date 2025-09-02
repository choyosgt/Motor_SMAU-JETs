import os
import re
import pickle
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split


MONTHS_ES = r"(ene|feb|mar|abr|may|jun|jul|ago|set|sep|oct|nov|dic)"
MONTHS_EN = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"


class DocumentFeatureExtractor:
    def __init__(self):
        self.label_encoder = LabelEncoder()

    # ---------------------------
    # Helpers
    # ---------------------------

    @staticmethod
    def _ratio(n, d):
        d = max(d, 1)
        return n / d

    # Encuentra líneas que son separadores visuales
    @staticmethod
    def _is_separator(text: str) -> int:
        # Línea tipo "-----" o "=====" o "....."
        return 1 if re.search(r"^\s*[-=\.]{8,}\s*$", text) else 0

    # Detecta si una línea tiene formato tabular
    @staticmethod
    def _is_table_like(text: str) -> int:
        # Lista de separadores a verificar
        separators = ["|", "│", ";", ",", "\t"]
        
        # Verificar separadores explícitos
        for sep in separators:
            if text.count(sep) >= 3:
                return 1
        
        # Verificar patrón de espacios múltiples (columnas con ancho fijo)
        # Encontrar todos los bloques de 2 o más espacios consecutivos
        space_blocks = re.findall(r" {2,}", text)
        if len(space_blocks) >= 3:
            # Verificar si hay un patrón consistente en los espacios
            space_lengths = [len(block) for block in space_blocks]
            
            # Si hay al menos 3 bloques y algunos tienen la misma longitud, es probable que sea una tabla
            unique_lengths = set(space_lengths)
            if len(unique_lengths) <= 3:  # Máximo 3 tipos diferentes de espaciado
                return 1
            
            # También considerar como tabla si hay muchos espacios múltiples
            return 1
        
        return 0

    # Detecta si hay alguna palabra en mayúsculas (≥4 chars, sin dígitos)
    @staticmethod
    def _has_all_caps_word(text: str) -> int:
        # Palabra de ≥4 chars, sin dígitos, toda en mayúsculas
        for w in re.findall(r"\b[^\W\d_]{4,}\b", text, flags=re.UNICODE):
            if w.isupper():
                return 1
        return 0

    # Ratios de mayúsculas, no alfanuméricos y dígitos
    @staticmethod
    def _upper_ratio(text: str) -> float:
        ups = sum(1 for ch in text if ch.isalpha() and ch.isupper())
        letters = sum(1 for ch in text if ch.isalpha())
        return ups / max(letters, 1)

    # No alfanuméricos (símbolos/puntuación)
    @staticmethod
    def _non_alnum_ratio(text: str) -> float:
        non = sum(1 for ch in text if not ch.isalnum() and not ch.isspace())
        return non / max(len(text), 1)

    # Ratios de dígitos
    @staticmethod
    def _digits_ratio(text: str) -> float:
        digs = sum(1 for ch in text if ch.isdigit())
        return digs / max(len(text), 1)

    # ---------------------------
    # Features por línea (texto)
    # ---------------------------
    def extract_structural_features(self, text: str) -> dict:
        """Extrae features estructurales del texto."""
        features = {}

        # Longitud de línea
        features["line_length"] = len(text)
        features["line_length_no_ws"] = len(text.replace(" ", "").replace("\t", ""))
        features["starts_with_space_or_tab"] = int(text.startswith((" ", "\t")))

        # Indentación (espacios/tabs al inicio)
        features["leading_spaces"] = len(text) - len(text.lstrip(" "))
        features["leading_tabs"] = len(text) - len(text.lstrip("\t"))
        features["total_indentation"] = (
            features["leading_spaces"] + features["leading_tabs"]
        )

        # Delimitadores
        features["pipe_count"] = text.count("|")
        features["tab_count"] = text.count("\t")
        features["comma_count"] = text.count(",")
        features["semicolon_count"] = text.count(";")
        features["colon_count"] = text.count(":")
        features["dot_count"] = text.count(".")
        features["dash_count"] = text.count("-")
        features["total_delimiters"] = (
            features["pipe_count"]
            + features["tab_count"]
            + features["comma_count"]
            + features["semicolon_count"]
            + features["colon_count"]
        )

        # Patrones de separadores
        features["has_dashes"] = 1 if re.search(r"-{8,}", text) else 0
        features["has_equals"] = 1 if re.search(r"={8,}", text) else 0
        features["has_dots"]   = 1 if re.search(r"\.{8,}", text) else 0
        features["is_separator_line"] = self._is_separator(text)

        # Ratios útiles
        features["upper_ratio"] = self._upper_ratio(text)
        features["non_alnum_ratio"] = self._non_alnum_ratio(text)
        features["digits_ratio"] = self._digits_ratio(text)
        features["space_ratio"] = self._ratio(text.count(" "), len(text))
        return features

    def extract_content_features(self, text: str) -> dict:
        """Extrae features del contenido."""
        features = {}

        # Números
        numbers = re.findall(r"\d+[.,]\d+|\d+", text)
        features["number_count"] = len(numbers)
        features["has_numbers"] = 1 if len(numbers) > 0 else 0
        features["number_density"] = len(numbers) / max(len(text), 1)

        # Fechas (varios formatos)
        date_patterns = [
            r"\b\d{2}/\d{2}/\d{4}\b",
            r"\b\d{2}\.\d{2}\.\d{4}\b",
            r"\b\d{2}-\d{2}-\d{4}\b",
            r"\b\d{8}\b",           # YYYYMMDD o DDMMYYYY
            r"\b\d{6}\b",           # YYMMDD
        ]
        
        features["has_date"] = 0
        for pattern in date_patterns:
            if re.search(pattern, text):
                features["has_date"] = 1
                break

        features["has_two_dates"] = 0

        for pattern in date_patterns:
            matches = re.findall(pattern, text)
            if len(matches) >= 2:   # encontró dos o más
                features["has_two_dates"] = 1
                break

        # Año explícito y mes (es/en)
        features["has_year"] = 1 if re.search(r"\b20\d{2}\b", text) else 0
        low = text.lower()
        features["has_month_name"] = 1 if re.search(MONTHS_ES, low) or re.search(MONTHS_EN, low) else 0

        # Monedas
        currency_pattern = r"(\bEUR\b|\bUSD\b|€|\$)"
        features["has_currency"] = 1 if re.search(currency_pattern, text, re.IGNORECASE) else 0

        # Tipo de documento
        features["has_doc_type"] = 1 if re.search(r"\b[A-Z][A-Z0-9]\b", text) else 0

        # Números de cuenta (6-12 dígitos)
        account_pattern = r"\b\d{6,12}\b"
        features["has_account_number"] = 1 if re.search(account_pattern, text) else 0

        # Importes (números con decimales y signos)
        amount_pattern = r"[-]?\d{1,3}([.,]\d{3})*[.,]\d{2}"
        features["has_amounts"] = 1 if re.search(amount_pattern, text) else 0
        features["amount_count"] = len(re.findall(amount_pattern, text))

        return features

    def extract_text_features(self, text: str) -> dict:
        """Extrae features específicas de palabras clave y estilo."""
        features = {}

        # Palabras clave (fortalecidas)
        text_lower = text.lower()

        meta_keywords = [
            "hora", "fecha", "pág", "página", "cif", "ledger", "soc", "sociedad", "ejercicio","empresa",
            "usuario", "user", "report", "rj", "rfbelj", "nacc", "fi97map"
        ]

        header_keywords = [
            "nº", "n°", "no.", "numero", "nº doc", "nº docum", "fecont", "fedoc", "fecpu",
            "texto cabecera", "denominación", "cuenta", "importe", "debe", "haber",
            "libro may", "ba", "cc", "md", "mon.", "mon", "ms", "ap.", "i",
            "bukrs", "gjahr", "belnr", "waers", "tcode", "blart", "bldat", "cpudt",
            "usnam", "nktxt", "tcode", "stblg", "blart", "bldat", "updt", "lifnr", "número",
            "date", "documentNo", "period"
        ]

        total_keywords = ["total", "suma", "acumulado", "arrsaldos", "saldo", "total página", "carryfwd"]

        #parent_keywords = ["factura", "cobros", "contab", "provision", "int comp", "valuation"]

        features["meta_keywords"] = sum(1 for kw in meta_keywords if kw in text_lower)
        features["header_keywords"] = sum(1 for kw in header_keywords if kw in text_lower)
        features["total_keywords"] = sum(1 for kw in total_keywords if kw in text_lower)
        #features["parent_keywords"] = sum(1 for kw in parent_keywords if kw in text_lower)

        # Estadísticos de palabras
        stripped = text.strip()
        words = re.findall(r"\S+", stripped)
        word_lengths = [len(w) for w in words] if words else []
        features["num_words"] = len(words)
        features["avg_word_length"] = float(np.mean(word_lengths)) if word_lengths else 0.0
        features["max_word_length"] = max(word_lengths) if word_lengths else 0

        # Señales tipográficas/estructura
        features["starts_with_number"] = 1 if re.match(r"^\s*\d+", stripped) else 0
        features["starts_with_zeros"] = 1 if re.match(r"^\s*0+", stripped) else 0
        features["is_empty"] = 1 if stripped == "" else 0
        features["is_mostly_spaces"] = 1 if len(stripped) < len(text) * 0.1 else 0
        features["has_all_caps_word"] = self._has_all_caps_word(text)
        features["is_table_like"] = self._is_table_like(text)

        # “Candidatos” a meta/header para ayudar a XGBoost
        features["is_meta_candidate"] = 1 if (
            features["meta_keywords"] > 0 or
            features["has_all_caps_word"] or
            features.get("has_year", 0) or
            features.get("has_month_name", 0)
        ) else 0

        # Header suele tener estructura tabular clara
        features["is_header_candidate"] = 1 if (
            features["is_table_like"] or
            features["header_keywords"] > 0 or
            text.count("|") >= 3
        ) else 0

        short_tokens = [w for w in re.findall(r"\b\w+\b", text) if len(w) <= 3]
        features["short_tokens_ratio"] = len(short_tokens) / max(len(text.split()), 1)

        # Detectar rellenos de cabecera tipo "......."
        features["has_dotted_fillers"] = 1 if re.search(r"\.{3,}", text) else 0

        # Meta fuerte: ≥2 palabras clave de metadatos
        meta_strong_kw = ["hora", "fecha", "pág", "pagina", "ledger", "usuario", "empresa", "cif", "libro diario"]
        features["meta_strong_keywords"] = sum(1 for kw in meta_strong_kw if kw in text_lower)
        features["is_meta_strong"] = 1 if features["meta_strong_keywords"] >= 2 else 0

        return features

    # ---------------------------
    # Features contextuales
    # ---------------------------
    def extract_contextual_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae features contextuales basadas en líneas adyacentes."""
        df = df.copy()
        df["text"] = df["text"].fillna("").astype(str)
        texts = df["text"].tolist()
        n = len(texts)

        features_list = []
        for i in range(n):
            prev_text = texts[i - 1] if i > 0 else ""
            this_text = texts[i]
            next_text = texts[i + 1] if i < n - 1 else ""

            f = {}
            # Posición relativa (0..1)
            f["relative_position"] = (i / (n - 1)) if n > 1 else 0.0
            f["is_first_lines"] = 1 if i < 10 else 0      # ↑ Aumentamos ventana: META suele vivir aquí
            f["is_last_lines"] = 1 if i >= n - 8 else 0

            # Línea anterior
            f["prev_line_length"] = len(prev_text)
            f["prev_is_separator"] = self._is_separator(prev_text)
            f["prev_is_table_like"] = self._is_table_like(prev_text)
            f["length_diff_prev"] = len(this_text) - len(prev_text)

            # Esta línea
            f["surrounded_by_separators"] = 1 if (self._is_separator(prev_text) and self._is_separator(next_text)) else 0

            # Línea siguiente
            f["next_line_length"] = len(next_text)
            f["next_is_separator"] = self._is_separator(next_text)
            f["next_is_table_like"] = self._is_table_like(next_text)
            f["length_diff_next"] = len(next_text) - len(this_text)
            f["next_pipe_count"] = next_text.count("|")

            # Heurísticas contextuales útiles:
            # - Un HEADER suele estar pegado a un separador antes/después.
            f["header_context_hint"] = 1 if (f["prev_is_separator"] or f["next_is_separator"]) else 0
            # - META a menudo aparece antes de la primera línea “tabla-like”
            f["meta_context_hint"] = 1 if (i < 10 and not f["prev_is_table_like"]) else 0

            features_list.append(f)

        return pd.DataFrame(features_list)

    # ---------------------------
    # Pipeline de extracción
    # ---------------------------
    def extract_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae todas las features y las combina."""
        df = df.copy()
        df["text"] = df["text"].fillna("").astype(str)

        all_features = []
        for _, row in df.iterrows():
            text = row["text"]
            f = {}
            f.update(self.extract_structural_features(text))
            content_f = self.extract_content_features(text)
            f.update(content_f)
            text_f = self.extract_text_features(text)
            text_f["has_year"] = content_f.get("has_year", 0)
            text_f["has_month_name"] = content_f.get("has_month_name", 0)
            f.update(text_f)
            all_features.append(f)

        features_df = pd.DataFrame(all_features).reset_index(drop=True)
        contextual_df = self.extract_contextual_features(df).reset_index(drop=True)
        features_df = pd.concat([features_df, contextual_df], axis=1)

        return features_df

    # ---------------------------
    # Preparación del dataset
    # ---------------------------
    def prepare_data(self, csv_path: str):
        """Función principal para preparar los datos."""
        print("Cargando datos...")
        # Evita que celdas vacías se conviertan en NaN automáticamente
        df = pd.read_csv(csv_path, keep_default_na=False)

        # Validaciones mínimas
        if "text" not in df.columns:
            raise ValueError("El CSV debe tener una columna 'text'.")
        if "label" not in df.columns:
            raise ValueError("El CSV debe tener una columna 'label'.")

        # Normalizaciones básicas
        df["text"] = df["text"].astype(str).fillna("")
        df["label"] = df["label"].astype(str).str.strip().str.upper()

        print("Analizando distribución de etiquetas...")
        print(df["label"].value_counts())

        print("Extrayendo features...")
        features_df = self.extract_all_features(df)

        # Target
        y = self.label_encoder.fit_transform(df["label"])

        print("Dividiendo datos en train/validation/test...")
        X_temp, X_test, y_temp, y_test = train_test_split(
            features_df, y, test_size=0.2, random_state=42, stratify=y
        )
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp
        )

        # Crear directorio para guardar los datos
        out_dir = "data_pre/processed"
        os.makedirs(out_dir, exist_ok=True)

        # Guardar datasets
        print("Guardando datasets procesados...")
        pd.concat([X_train.reset_index(drop=True),
                   pd.DataFrame(y_train, columns=["label"])],
                  axis=1).to_csv(os.path.join(out_dir, "train.csv"), index=False)

        pd.concat([X_val.reset_index(drop=True),
                   pd.DataFrame(y_val, columns=["label"])],
                  axis=1).to_csv(os.path.join(out_dir, "val.csv"), index=False)

        pd.concat([X_test.reset_index(drop=True),
                   pd.DataFrame(y_test, columns=["label"])],
                  axis=1).to_csv(os.path.join(out_dir, "test.csv"), index=False)

        # Guardar el label encoder
        with open(os.path.join(out_dir, "label_encoder.pkl"), "wb") as f:
            pickle.dump(self.label_encoder, f)

        # Guardar nombres de features
        feature_names = list(features_df.columns)
        with open(os.path.join(out_dir, "feature_names.txt"), "w", encoding="utf-8") as f:
            for name in feature_names:
                f.write(name + "\n")

        print("Datos preparados exitosamente:")
        print(f"- Train: {X_train.shape}")
        print(f"- Validation: {X_val.shape}")
        print(f"- Test: {X_test.shape}")
        print(f"- Features: {len(feature_names)}")
        print(f"- Clases: {list(self.label_encoder.classes_)}")

        return X_train, X_val, X_test, y_train, y_val, y_test


if __name__ == "__main__":
    extractor = DocumentFeatureExtractor()
    # Cambia esta ruta a tu CSV si lo deseas
    extractor.prepare_data("data_pre/training_data.csv")
