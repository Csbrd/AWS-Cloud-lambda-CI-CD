"""
lifesync XGBoost Training Script  (아키텍처구성도 기준)

BigQuery lifesync_curated.ai_feature_table 에서 30개 피처를 읽어
4개 모델을 학습하고 GCS + Vertex AI Model Registry에 등록합니다.

모델:
  vip    — VIP 승급 가능성     (XGBClassifier,  target: vip_label)
  signup — 추가 가입 가능성    (XGBClassifier,  target: churn_label proxy)
  rec    — 상품 구매 가능성    (XGBClassifier,  target: product_purchase_label proxy)
  health — 건강점수 예측       (XGBRegressor,   target: health_risk_score)

실행:
  GCP_PROJECT_ID=<project_id> python train.py
"""

import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import xgboost as xgb
from google.cloud import bigquery, storage, aiplatform
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, mean_absolute_error, precision_score, recall_score

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION     = os.environ.get("REGION", "asia-northeast3")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "lifesync-data-lake")

SOURCE_TABLE = f"{PROJECT_ID}.lifesync_curated.ai_feature_table"

XGBOOST_CLASSIFIER_CONTAINER = "us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest"
XGBOOST_REGRESSOR_CONTAINER  = "us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest"

# predict_runner.py 의 NUMERIC_FEATURES 와 반드시 일치 (컬럼 순서 동일해야 함)
NUMERIC_FEATURES = [
    "balance_30d_avg", "asset_growth_90d", "card_spend_30d",
    "invest_total", "invest_ratio", "etf_ratio", "policy_cnt",
    "avg_steps_30d", "avg_hr_30d", "stress_avg_30d", "avg_sleep_30d",
    "hospital_visit_90d", "health_risk_score", "step_growth_30d",
    "login_cnt_30d", "avg_session_min", "push_click_rate",
    "recommend_click_rate", "last_active_days",
    "affiliate_cnt", "consent_ratio", "membership_days", "cross_product_score",
    "spend_growth_90d", "invest_growth_90d", "wellness_growth_30d",
    "inactive_days", "card_drop_ratio", "asset_drop_ratio", "complaint_flag",
]

CLF_PARAMS = {
    "n_estimators":  300,
    "max_depth":     6,
    "learning_rate": 0.05,
    "subsample":     0.8,
    "eval_metric":   "logloss",
    "random_state":  42,
}

REG_PARAMS = {
    "n_estimators":  300,
    "max_depth":     6,
    "learning_rate": 0.05,
    "subsample":     0.8,
    "objective":     "reg:squarederror",
    "random_state":  42,
}


METRICS_TABLE      = f"{PROJECT_ID}.lifesync_ml.model_metrics"
IMPORTANCE_TABLE   = f"{PROJECT_ID}.lifesync_ml.feature_importance"

METRICS_SCHEMA = [
    bigquery.SchemaField("model_name",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("auc",             "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("accuracy",        "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("mae",             "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("train_size",      "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("test_size",       "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("precision",        "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("recall",          "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("positive_ratio",  "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("resource_name",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("trained_at",      "TIMESTAMP", mode="REQUIRED"),
]


IMPORTANCE_SCHEMA = [
    bigquery.SchemaField("model_name",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("feature_name",  "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("importance",    "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("rank",          "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("trained_at",    "TIMESTAMP", mode="REQUIRED"),
]


def _ensure_metrics_table(bq: bigquery.Client) -> None:
    try:
        bq.get_table(METRICS_TABLE)
    except Exception:
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, "lifesync_ml")
        table = bigquery.Table(METRICS_TABLE, schema=METRICS_SCHEMA)
        bq.create_table(table)
        print(f"  model_metrics 테이블 생성: {METRICS_TABLE}")


def _ensure_importance_table(bq: bigquery.Client) -> None:
    try:
        bq.get_table(IMPORTANCE_TABLE)
    except Exception:
        table = bigquery.Table(IMPORTANCE_TABLE, schema=IMPORTANCE_SCHEMA)
        bq.create_table(table)
        print(f"  feature_importance 테이블 생성: {IMPORTANCE_TABLE}")


def _save_feature_importance(model_name: str, model, feature_names: list) -> None:
    bq = bigquery.Client(project=PROJECT_ID)
    _ensure_importance_table(bq)
    trained_at = datetime.now(timezone.utc).isoformat()
    importances = model.feature_importances_
    ranked = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
    rows = [
        {
            "model_name":   model_name,
            "feature_name": name,
            "importance":   round(float(imp), 6),
            "rank":         rank + 1,
            "trained_at":   trained_at,
        }
        for rank, (name, imp) in enumerate(ranked)
    ]
    errors = bq.insert_rows_json(IMPORTANCE_TABLE, rows)
    if errors:
        print(f"  [경고] feature_importance 저장 실패: {errors}")
    else:
        print(f"  feature_importance 저장 완료 ({len(rows)}개 피처) → {IMPORTANCE_TABLE}")


def _save_metrics(model_name: str, resource_name: str, **metrics) -> None:
    bq = bigquery.Client(project=PROJECT_ID)
    _ensure_metrics_table(bq)
    row = {
        "model_name":     model_name,
        "auc":            metrics.get("auc"),
        "accuracy":       metrics.get("accuracy"),
        "mae":            metrics.get("mae"),
        "precision":      metrics.get("precision"),
        "recall":         metrics.get("recall"),
        "train_size":     metrics["train_size"],
        "test_size":      metrics["test_size"],
        "positive_ratio": metrics.get("positive_ratio"),
        "resource_name":  resource_name,
        "trained_at":     datetime.now(timezone.utc).isoformat(),
    }
    errors = bq.insert_rows_json(METRICS_TABLE, [row])
    if errors:
        print(f"  [경고] 메트릭 저장 실패: {errors}")
    else:
        print(f"  메트릭 저장 완료 → {METRICS_TABLE}")


def _load_table() -> pd.DataFrame:
    print(f"  BigQuery 로드: {SOURCE_TABLE}")
    client = bigquery.Client(project=PROJECT_ID)
    cols   = ", ".join(NUMERIC_FEATURES + ["vip_label", "churn_label", "product_purchase_label"])
    df     = client.query(f"""
        SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, {cols}
        FROM `{SOURCE_TABLE}`
    """).to_dataframe()
    print(f"  행 수: {len(df):,}")
    return df


def _upload_model(local_path: str, model_name: str) -> str:
    gcs_path = f"models/{model_name}/model.bst"
    storage.Client(project=PROJECT_ID).bucket(GCS_BUCKET) \
           .blob(gcs_path).upload_from_filename(local_path)
    uri = f"gs://{GCS_BUCKET}/models/{model_name}/"
    print(f"  GCS 업로드: {uri}")
    return uri


def _register_model(display_name: str, artifact_uri: str) -> str:
    model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri=artifact_uri,
        serving_container_image_uri=XGBOOST_CLASSIFIER_CONTAINER,
    )
    print(f"  Vertex AI 등록: {model.resource_name}")
    return model.resource_name


def _save_and_register(model, model_name: str, display_name: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        # feature_names/types 제거 → 익명 모델로 저장
        # Vertex AI 1.7 컨테이너가 2.x 모델의 feature_names를 읽지 못하므로
        # 익명으로 저장해 컬럼 순서 기반 예측 사용
        booster = model.get_booster()
        booster.feature_names = None
        booster.feature_types = None
        booster.save_model(f.name)
        uri = _upload_model(f.name, model_name)
    return _register_model(display_name, uri)


def train_vip(df: pd.DataFrame) -> str:
    print("\n[1/4] VIP 예측 모델 (XGBClassifier)")
    X = df[["row_id"] + NUMERIC_FEATURES].fillna(0.0)
    y = df["vip_label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"  Train={len(X_tr):,}  Test={len(X_te):,}  VIP 비율={y.mean():.4f}")
    model = xgb.XGBClassifier(**CLF_PARAMS)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=100)
    preds = model.predict(X_te)
    proba = model.predict_proba(X_te)[:, 1]
    acc  = accuracy_score(y_te, preds)
    auc  = roc_auc_score(y_te, proba)
    prec = precision_score(y_te, preds, zero_division=0)
    rec  = recall_score(y_te, preds, zero_division=0)
    print(f"  Accuracy={acc:.4f}  AUC={auc:.4f}  Precision={prec:.4f}  Recall={rec:.4f}")
    resource_name = _save_and_register(model, "vip", "lifesync-vip-model")
    _save_metrics("vip", resource_name,
                  auc=auc, accuracy=acc, precision=prec, recall=rec,
                  train_size=len(X_tr), test_size=len(X_te),
                  positive_ratio=float(y.mean()))
    _save_feature_importance("vip", model, ["row_id"] + NUMERIC_FEATURES)
    return resource_name


def train_signup(df: pd.DataFrame) -> str:
    print("\n[2/4] 가입 예측 모델 (XGBClassifier, proxy=affiliate_cnt>=3)")
    X = df[["row_id"] + NUMERIC_FEATURES].fillna(0.0)
    y = df["churn_label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"  Train={len(X_tr):,}  Test={len(X_te):,}  Signup 비율={y.mean():.4f}")
    model = xgb.XGBClassifier(**CLF_PARAMS)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=100)
    preds = model.predict(X_te)
    proba = model.predict_proba(X_te)[:, 1]
    acc  = accuracy_score(y_te, preds)
    auc  = roc_auc_score(y_te, proba)
    prec = precision_score(y_te, preds, zero_division=0)
    rec  = recall_score(y_te, preds, zero_division=0)
    print(f"  Accuracy={acc:.4f}  AUC={auc:.4f}  Precision={prec:.4f}  Recall={rec:.4f}")
    resource_name = _save_and_register(model, "signup", "lifesync-signup-model")
    _save_metrics("signup", resource_name,
                  auc=auc, accuracy=acc, precision=prec, recall=rec,
                  train_size=len(X_tr), test_size=len(X_te),
                  positive_ratio=float(y.mean()))
    _save_feature_importance("signup", model, ["row_id"] + NUMERIC_FEATURES)
    return resource_name


def train_rec(df: pd.DataFrame) -> str:
    print("\n[3/4] 추천 예측 모델 (XGBClassifier, proxy=invest_total>0)")
    X = df[["row_id"] + NUMERIC_FEATURES].fillna(0.0)
    y = df["product_purchase_label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"  Train={len(X_tr):,}  Test={len(X_te):,}  Rec 비율={y.mean():.4f}")
    model = xgb.XGBClassifier(**CLF_PARAMS)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=100)
    preds = model.predict(X_te)
    proba = model.predict_proba(X_te)[:, 1]
    acc  = accuracy_score(y_te, preds)
    auc  = roc_auc_score(y_te, proba)
    prec = precision_score(y_te, preds, zero_division=0)
    rec  = recall_score(y_te, preds, zero_division=0)
    print(f"  Accuracy={acc:.4f}  AUC={auc:.4f}  Precision={prec:.4f}  Recall={rec:.4f}")
    resource_name = _save_and_register(model, "rec", "lifesync-rec-model")
    _save_metrics("rec", resource_name,
                  auc=auc, accuracy=acc, precision=prec, recall=rec,
                  train_size=len(X_tr), test_size=len(X_te),
                  positive_ratio=float(y.mean()))
    _save_feature_importance("rec", model, ["row_id"] + NUMERIC_FEATURES)
    return resource_name


def train_health(df: pd.DataFrame) -> str:
    print("\n[4/4] 건강점수 예측 모델 (XGBRegressor, target=health_risk_score)")
    # health_risk_score를 피처에서 제외 — target과 동일한 컬럼이므로 데이터 리키지 방지
    health_features = [f for f in NUMERIC_FEATURES if f != "health_risk_score"]
    X = df[["row_id"] + health_features].fillna(0.0)
    y = df["health_risk_score"].fillna(50.0)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"  Train={len(X_tr):,}  Test={len(X_te):,}  score 평균={y.mean():.1f}")
    model = xgb.XGBRegressor(**REG_PARAMS)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=100)
    preds = model.predict(X_te)
    mae = mean_absolute_error(y_te, preds)
    print(f"  MAE={mae:.4f}")
    resource_name = _save_and_register(model, "health", "lifesync-health-model")
    _save_metrics("health", resource_name,
                  mae=mae,
                  train_size=len(X_tr), test_size=len(X_te))
    _save_feature_importance("health", model, ["row_id"] + health_features)
    return resource_name


def main():
    print("=" * 60)
    print("lifesync XGBoost Training  (4-Model Architecture)")
    print("=" * 60)

    aiplatform.init(project=PROJECT_ID, location=REGION)
    df = _load_table()

    results = {
        "vip":    train_vip(df),
        "signup": train_signup(df),
        "rec":    train_rec(df),
        "health": train_health(df),
    }

    print("\n" + "=" * 60)
    print("학습 완료 — Vertex AI Model Resource Names")
    print("=" * 60)
    for name, resource in results.items():
        print(f"  {name:8s}: {resource}")

    print("\n▶ terraform.tfvars 에 아래 값을 업데이트하세요:")
    print(f'  vip_model_resource_name    = "{results["vip"]}"')
    print(f'  signup_model_resource_name = "{results["signup"]}"')
    print(f'  rec_model_resource_name    = "{results["rec"]}"')
    print(f'  health_model_resource_name = "{results["health"]}"')
    print("=" * 60)


if __name__ == "__main__":
    main()
