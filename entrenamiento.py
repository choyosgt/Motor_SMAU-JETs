import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
    balanced_accuracy_score,
    top_k_accuracy_score
)
from sklearn.model_selection import RandomizedSearchCV, StratifiedShuffleSplit
from sklearn.utils.class_weight import compute_sample_weight
import pickle
import os
import matplotlib.pyplot as plt
import seaborn as sns
import json


class DocumentClassifier:
    def __init__(self):
        self.model = None
        self.label_encoder = None
        self.feature_names = None

    # ----------------------------
    # Utils internos
    # ----------------------------
    @staticmethod
    def _make_class_weights(y):
        """Genera pesos balanceados por clase para combatir el desbalance."""
        return compute_sample_weight(class_weight="balanced", y=y)

    # ----------------------------
    # Carga de datos
    # ----------------------------
    def load_data(self):
        """Carga los datos procesados (train/val/test + encoder y feature names)."""
        print("Cargando datos de entrenamiento...")

        train_df = pd.read_csv('data_pre/processed/train.csv')
        val_df = pd.read_csv('data_pre/processed/val.csv')
        test_df = pd.read_csv('data_pre/processed/test.csv')

        # Separar features y labels
        X_train = train_df.drop('label', axis=1)
        y_train = train_df['label']

        X_val = val_df.drop('label', axis=1)
        y_val = val_df['label']

        X_test = test_df.drop('label', axis=1)
        y_test = test_df['label']

        # Cargar label encoder
        with open('data_pre/processed/label_encoder.pkl', 'rb') as f:
            self.label_encoder = pickle.load(f)

        # Cargar nombres de features
        with open('data_pre/processed/feature_names.txt', 'r', encoding='utf-8') as f:
            self.feature_names = [line.strip() for line in f]

        print("Datos cargados:")
        print(f"- Train: {X_train.shape}")
        print(f"- Validation: {X_val.shape}")
        print(f"- Test: {X_test.shape}")
        print(f"- Clases: {list(self.label_encoder.classes_)}")

        return X_train, X_val, X_test, y_train, y_val, y_test

    # ----------------------------
    # Entrenamiento del modelo
    # ----------------------------
    def train_model(self, X_train, y_train, X_val, y_val):
        """
        Entrena XGBoost con RandomizedSearchCV (f1_macro) y refit final con early stopping
        usando un 'dev set' interno para evitar fuga de información.
        Compatible con versiones de xgboost donde .fit no acepta early_stopping_rounds.
        """
        print("Iniciando entrenamiento del modelo XGBoost...")

        num_classes = len(self.label_encoder.classes_)
        # Usamos 'hist' para estabilidad en CPU; si tienes GPU puedes cambiar a 'gpu_hist'
        base_params = {
            'objective': 'multi:softprob',
            'num_class': num_classes,
            'eval_metric': 'mlogloss',
            'random_state': 30,
            'verbosity': 0,
            'n_jobs': -1,
            'tree_method': 'hist',
            'enable_categorical': False
        }

        # Espacio de hiperparámetros (exploración eficiente)
        param_distributions = {
            'n_estimators': [1200, 1800, 2800, 3000],
            'learning_rate': [0.03, 0.05, 0.07, 0.1],
            'max_depth': [3, 4, 5, 6, 8],
            'min_child_weight': [1, 2, 4, 6],
            'subsample': [0.7, 0.8, 0.9, 1.0],
            'colsample_bytree': [0.7, 0.8, 0.9, 1.0],
            'gamma': [0, 0.5, 1.0],
            'reg_alpha': [0, 0.001, 0.01, 0.1],
            'reg_lambda': [0.5, 1.0, 1.5, 2.0]
        }

        xgb_base = xgb.XGBClassifier(**base_params)

        print("Realizando búsqueda de hiperparámetros (RandomizedSearchCV)...")
        search = RandomizedSearchCV(
            estimator=xgb_base,
            param_distributions=param_distributions,
            n_iter=25,
            scoring='f1_macro',
            cv=3,
            verbose=1,
            random_state=30,
            n_jobs=-1
        )

        # Importante: NO pasar eval_set en la búsqueda para evitar fuga de info
        sw_train = self._make_class_weights(y_train)
        search.fit(X_train, y_train, sample_weight=sw_train)

        print("Mejores parámetros encontrados:")
        for k, v in search.best_params_.items():
            print(f"  {k}: {v}")
        print(f"Mejor score CV (f1_macro): {search.best_score_:.4f}")

        # Refit final con early stopping en train+val
        print("Preparando refit final con early stopping (compat)...")
        X_tv = pd.concat([X_train, X_val], axis=0)
        y_tv = pd.concat([y_train, y_val], axis=0)

        # 'dev set' interno (10%) para early stopping
        sss = StratifiedShuffleSplit(n_splits=1, test_size=0.1, random_state=30)
        (train_idx, dev_idx) = next(sss.split(X_tv, y_tv))
        X_trf, y_trf = X_tv.iloc[train_idx], y_tv.iloc[train_idx]
        X_dev, y_dev = X_tv.iloc[dev_idx], y_tv.iloc[dev_idx]

        final_params = {**base_params, **search.best_params_}
        # Permitimos más árboles y que early stopping elija la mejor iteración
        final_params['n_estimators'] = max(final_params.get('n_estimators', 500), 1000)

        # ❗ Compat: establecer early_stopping_rounds como PARÁMETRO del modelo (no en fit)
        final_params['early_stopping_rounds'] = 500

        self.model = xgb.XGBClassifier(**final_params)

        print("Entrenando modelo final con early stopping (sin pasarlo a fit)...")
        sw_trf = self._make_class_weights(y_trf)

        # Opción A (Compat máxima): NO pasar early_stopping_rounds en fit
        self.model.fit(
            X_trf, y_trf,
            sample_weight=sw_trf,
            eval_set=[(X_dev, y_dev)],
            verbose=False
        )

        # Métricas preliminares en dev set (control de overfit)
        
        y_dev_pred = self.model.predict(X_dev)
        dev_f1 = f1_score(y_dev, y_dev_pred, average='macro')
        dev_bacc = balanced_accuracy_score(y_dev, y_dev_pred)
        print(f"Dev F1-macro: {dev_f1:.4f} | Dev Balanced Acc: {dev_bacc:.4f}")

        return search.best_score_


    # ----------------------------
    # Evaluación
    # ----------------------------
    def evaluate_model(self, X_test, y_test, out_dir='modelo/plots'):
        """Evalúa el modelo en el conjunto de test con métricas ampliadas."""
        print("Evaluando modelo en conjunto de test...")

        y_proba = self.model.predict_proba(X_test)
        y_pred = np.argmax(y_proba, axis=1)

        acc = accuracy_score(y_test, y_pred)
        f1m = f1_score(y_test, y_pred, average='macro')
        bacc = balanced_accuracy_score(y_test, y_pred)
        top2 = top_k_accuracy_score(y_test, y_proba, k=2)

        print(f"Accuracy: {acc:.4f} | F1-macro: {f1m:.4f} | BalancedAcc: {bacc:.4f} | Top-2 Acc: {top2:.4f}")

        target_names = list(self.label_encoder.classes_)
        report = classification_report(y_test, y_pred, target_names=target_names)
        print("\nReporte de clasificación:")
        print(report)

        cm = confusion_matrix(y_test, y_pred)
        cmn = confusion_matrix(y_test, y_pred, normalize='true')

        os.makedirs(out_dir, exist_ok=True)

        # Confusión absoluta
        plt.figure(figsize=(10, 8))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                    xticklabels=target_names, yticklabels=target_names)
        plt.title('Matriz de Confusión (conteos)')
        plt.ylabel('Etiqueta Real')
        plt.xlabel('Etiqueta Predicha')
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, 'confusion_matrix_counts.png'), dpi=300, bbox_inches='tight')
        plt.close()

        # Confusión normalizada
        plt.figure(figsize=(10, 8))
        sns.heatmap(cmn, annot=True, fmt='.2f', cmap='Greens',
                    xticklabels=target_names, yticklabels=target_names)
        plt.title('Matriz de Confusión (normalizada por clase)')
        plt.ylabel('Etiqueta Real')
        plt.xlabel('Etiqueta Predicha')
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, 'confusion_matrix_normalized.png'), dpi=300, bbox_inches='tight')
        plt.close()

        return {
            'accuracy': acc,
            'f1_macro': f1m,
            'balanced_accuracy': bacc,
            'top2_accuracy': top2,
            'report': report
        }

    # ----------------------------
    # Importancia de features
    # ----------------------------
    def analyze_feature_importance(self):
        """Analiza la importancia de las features (gain) y guarda gráfico + CSV."""
        print("Analizando importancia de features...")

        out_dir = 'modelo/plots'
        os.makedirs(out_dir, exist_ok=True)

        # Importancias por gain desde el booster (más informativas que .feature_importances_)
        try:
            booster = self.model.get_booster()
            gain_dict = booster.get_score(importance_type='gain')
            items = sorted(gain_dict.items(), key=lambda kv: kv[1], reverse=True)
            feat_names = [k for k, _ in items]
            gains = [v for _, v in items]
            feature_importance_df = pd.DataFrame({
                'feature': feat_names,
                'gain': gains
            })
        except Exception:
            # Fallback: importancia promedio
            imp = getattr(self.model, "feature_importances_", None)
            feature_importance_df = pd.DataFrame({
                'feature': self.feature_names,
                'gain': imp if imp is not None else np.zeros(len(self.feature_names))
            }).sort_values('gain', ascending=False)

        print("\nTop 15 features más importantes (gain):")
        print(feature_importance_df.head(15))

        # Graficar top-25
        topk = feature_importance_df.head(25)
        plt.figure(figsize=(10, 8))
        plt.barh(range(len(topk)), topk['gain'])
        plt.yticks(range(len(topk)), topk['feature'])
        plt.gca().invert_yaxis()
        plt.title('Importancia de Features (gain) - Top 25')
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, 'feature_importance_gain.png'), dpi=300, bbox_inches='tight')
        plt.close()

        # Guardar CSV
        feature_importance_df.to_csv('modelo/feature_importance_gain.csv', index=False)

        return feature_importance_df

    # ----------------------------
    # Guardado del modelo
    # ----------------------------
    def save_model(self):
        """Guarda el modelo entrenado, encoder y metadatos."""
        print("Guardando modelo...")

        os.makedirs('modelo', exist_ok=True)

        # Modelo XGBoost
        self.model.save_model('modelo/xgboost_model.json')

        # Pickle para compatibilidad
        with open('modelo/model.pkl', 'wb') as f:
            pickle.dump(self.model, f)

        # Label encoder
        with open('modelo/label_encoder.pkl', 'wb') as f:
            pickle.dump(self.label_encoder, f)

        # Nombres de features
        with open('modelo/feature_names.txt', 'w', encoding='utf-8') as f:
            for name in self.feature_names:
                f.write(name + '\n')

        # Info del modelo
        model_info = {
            'model_type': 'XGBoost',
            'num_features': len(self.feature_names),
            'num_classes': len(self.label_encoder.classes_),
            'classes': list(self.label_encoder.classes_),
            'xgboost_params': self.model.get_params()
        }
        with open('modelo/model_info.json', 'w', encoding='utf-8') as f:
            json.dump(model_info, f, indent=2, ensure_ascii=False)

        print("Modelo guardado exitosamente en la carpeta 'modelo/'")

    # ----------------------------
    # Pipeline completo
    # ----------------------------
    def train_complete_pipeline(self):
        """Pipeline completo de entrenamiento y evaluación con guardado."""
        # Cargar datos
        X_train, X_val, X_test, y_train, y_val, y_test = self.load_data()

        # Entrenar
        cv_score = self.train_model(X_train, y_train, X_val, y_val)

        # Evaluar
        metrics = self.evaluate_model(X_test, y_test)

        # Importancias
        feature_importance = self.analyze_feature_importance()

        # Guardar modelo
        self.save_model()

        # Resumen final
        print("\n" + "=" * 50)
        print("RESUMEN DEL ENTRENAMIENTO")
        print("=" * 50)
        print(f"Best CV Score (F1-macro): {cv_score:.4f}")
        print(f"Test Accuracy: {metrics['accuracy']:.4f}")
        print(f"Balanced Acc: {metrics['balanced_accuracy']:.4f}")
        print(f"Top-2 Acc: {metrics['top2_accuracy']:.4f}")
        print(f"Número de features: {len(self.feature_names)}")
        print(f"Clases: {list(self.label_encoder.classes_)}")
        print("Top 3 features importantes (gain):")
        for _, row in feature_importance.head(3).iterrows():
            print(f"  - {row['feature']}: {row.get('gain', 0):.4f}")
        print("=" * 50)

        return {
            'cv_score': cv_score,
            'metrics': metrics,
            'feature_importance': feature_importance
        }


if __name__ == "__main__":
    classifier = DocumentClassifier()
    _ = classifier.train_complete_pipeline()
