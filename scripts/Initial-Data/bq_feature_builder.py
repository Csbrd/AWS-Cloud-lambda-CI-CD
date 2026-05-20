"""
lifesync BigQuery Feature Builder  (설계 문서 AI Feature Table 규칙 기준)

입력:
  customer_master.csv          (pre_customer_data.py 출력)
  bank_YYYYMMDD.json 외 7개   (pre_generator_data.py 출력)

출력:
  BigQuery: lifesync_curated.ai_feature_table

실행:
  GCP_PROJECT_ID=<project_id> BATCH_DATE=20250901 python bq_feature_builder.py
"""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-1f8eb19b-1a9a-45cf-ae6")
DATASET    = "lifesync_curated"
TABLE      = "ai_feature_table"
BATCH_DATE = os.environ.get("BATCH_DATE", "20250901")

MASTER_CSV = "customer_master.csv"

JSON_FILES = {
    "bank":             f"bank_{BATCH_DATE}.json",
    "card":             f"card_{BATCH_DATE}.json",
    "insurance":        f"insurance_{BATCH_DATE}.json",
    "online_insurance": f"online_insurance_{BATCH_DATE}.json",
    "hospital":         f"hospital_{BATCH_DATE}.json",
    "healthcare":       f"healthcare_{BATCH_DATE}.json",
    "securities":       f"securities_{BATCH_DATE}.json",
    "wearable":         f"wearable_{BATCH_DATE}.json",
}

SCHEMA = [
    bigquery.SchemaField("global_id",              "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("lifesync_score",         "FLOAT",   mode="NULLABLE"),
    # 금융
    bigquery.SchemaField("balance_30d_avg",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("asset_growth_90d",       "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("card_spend_30d",         "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("invest_total",           "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("invest_ratio",           "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("etf_ratio",              "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("policy_cnt",             "FLOAT",   mode="NULLABLE"),
    # 건강
    bigquery.SchemaField("avg_steps_30d",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_hr_30d",             "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("stress_avg_30d",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_sleep_30d",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("hospital_visit_90d",     "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("health_risk_score",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("step_growth_30d",        "FLOAT",   mode="NULLABLE"),
    # 행동
    bigquery.SchemaField("login_cnt_30d",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_session_min",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("push_click_rate",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("recommend_click_rate",   "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("last_active_days",       "FLOAT",   mode="NULLABLE"),
    # 관계
    bigquery.SchemaField("affiliate_cnt",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("consent_ratio",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("membership_days",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("cross_product_score",    "FLOAT",   mode="NULLABLE"),
    # 성장
    bigquery.SchemaField("spend_growth_90d",       "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("invest_growth_90d",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("wellness_growth_30d",    "FLOAT",   mode="NULLABLE"),
    # Risk
    bigquery.SchemaField("inactive_days",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("card_drop_ratio",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("asset_drop_ratio",       "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("complaint_flag",         "FLOAT",   mode="NULLABLE"),
    # Label
    bigquery.SchemaField("vip_label",              "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("churn_label",            "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("pb_contract_label",      "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("product_purchase_label", "INTEGER", mode="NULLABLE"),
]


def _load_records(filename: str) -> list:
    if not os.path.exists(filename):
        print(f"  [SKIP] {filename} 없음")
        return []
    with open(filename, encoding="utf-8") as f:
        return json.load(f).get("records", [])


def _membership_days(wearable_join_dt_str) -> float:
    if not wearable_join_dt_str or pd.isna(wearable_join_dt_str):
        return 365.0
    try:
        batch_dt = datetime.strptime(BATCH_DATE, "%Y%m%d")
        join_dt  = datetime.strptime(str(wearable_join_dt_str)[:10], "%Y-%m-%d")
        return max(1.0, float((batch_dt - join_dt).days))
    except Exception:
        return 365.0


def main():
    print("=" * 60)
    print("lifesync BigQuery Feature Builder  (설계 문서 기준)")
    print("=" * 60)

    # ── 1. 마스터 로드 ────────────────────────────────────────
    print("[1/7] customer_master.csv 로드...")
    df = pd.read_csv(MASTER_CSV)
    print(f"      고객 수: {len(df):,}")

    # ── 2. 계열사 데이터 집계 ─────────────────────────────────
    print("[2/7] 계열사 데이터 집계...")

    # 은행: balance_30d_avg (최근 잔액 근사), bank_tx_count (affiliate_cnt용)
    recs = _load_records(JSON_FILES["bank"])
    if recs:
        b = pd.DataFrame(recs).groupby("global_id").agg(
            _bank_tx_count=("amount", "count"),
            balance_30d_avg=("balance_after", "last"),
        ).reset_index()
        b["balance_30d_avg"] = b["balance_30d_avg"].astype(float)
        df = df.merge(b, on="global_id", how="left")
        print(f"      bank: {len(recs):,} 건")

    # 카드: card_spend_30d, card_tx_count (affiliate_cnt용)
    recs = _load_records(JSON_FILES["card"])
    if recs:
        c = pd.DataFrame(recs).groupby("global_id").agg(
            _card_tx_count=("amount", "count"),
            card_spend_30d=("amount", "sum"),
        ).reset_index()
        c["card_spend_30d"] = c["card_spend_30d"].astype(float)
        df = df.merge(c, on="global_id", how="left")
        print(f"      card: {len(recs):,} 건")

    # 증권: invest_total, _securities_cnt (cross_product_score용)
    recs = _load_records(JSON_FILES["securities"])
    if recs:
        s = pd.DataFrame(recs)
        s["trade_value"] = s["qty"] * s["price"]
        s = s.groupby("global_id").agg(
            invest_total=("trade_value", "sum"),
            _securities_cnt=("trade_value", "count"),
        ).reset_index()
        s["invest_total"] = s["invest_total"].astype(float)
        df = df.merge(s, on="global_id", how="left")
        print(f"      securities: {len(recs):,} 건")

    # 보험: policy_cnt, _insurance_cnt (cross_product_score용)
    recs = _load_records(JSON_FILES["insurance"])
    if recs:
        i = pd.DataFrame(recs).groupby("global_id").agg(
            _insurance_cnt=("premium_amount", "count"),
        ).reset_index()
        df = df.merge(i, on="global_id", how="left")
        print(f"      insurance: {len(recs):,} 건")

    # 온라인 보험: _online_ins_cnt (policy_cnt, cross_product_score용)
    recs = _load_records(JSON_FILES["online_insurance"])
    if recs:
        oi = pd.DataFrame(recs).groupby("global_id").size().reset_index(name="_online_ins_cnt")
        df = df.merge(oi, on="global_id", how="left")
        print(f"      online_insurance: {len(recs):,} 건")

    # 병원: hospital_visit_90d
    recs = _load_records(JSON_FILES["hospital"])
    if recs:
        h = pd.DataFrame(recs).groupby("global_id").agg(
            hospital_visit_90d=("cost", "count"),
        ).reset_index()
        h["hospital_visit_90d"] = h["hospital_visit_90d"].astype(float)
        df = df.merge(h, on="global_id", how="left")
        print(f"      hospital: {len(recs):,} 건")

    # 헬스케어: health_risk_score (100 - health_score)
    recs = _load_records(JSON_FILES["healthcare"])
    if recs:
        hc = pd.DataFrame(recs).groupby("global_id").agg(
            _health_score=("health_score", "mean"),
        ).reset_index()
        df = df.merge(hc, on="global_id", how="left")
        print(f"      healthcare: {len(recs):,} 건")

    # 웨어러블: avg_steps_30d, avg_hr_30d, stress_avg_30d, avg_sleep_30d
    recs = _load_records(JSON_FILES["wearable"])
    if recs:
        vitals = [
            {"global_id":    r["global_id"],
             "heart_rate":   v["heart_rate"],
             "steps":        v["steps"],
             "stress_score": v.get("stress_score", 50),
             "sleep_hours":  v.get("sleep_hours",  7.0)}
            for r in recs for v in r.get("vitals", [])
        ]
        w = pd.DataFrame(vitals).groupby("global_id").agg(
            avg_hr_30d=    ("heart_rate",   "mean"),
            avg_steps_30d= ("steps",        "mean"),
            stress_avg_30d=("stress_score", "mean"),
            avg_sleep_30d= ("sleep_hours",  "mean"),
        ).reset_index()
        df = df.merge(w, on="global_id", how="left")
        print(f"      wearable: {len(recs):,} 건")

    # ── 3. 결측값 처리 ────────────────────────────────────────
    print("[3/7] 결측값 처리...")

    float_defaults = {
        "balance_30d_avg": 0.0, "card_spend_30d": 0.0, "invest_total": 0.0,
        "hospital_visit_90d": 0.0, "avg_hr_30d": 0.0, "avg_steps_30d": 0.0,
        "stress_avg_30d": 50.0, "avg_sleep_30d": 6.5,
        "_bank_tx_count": 0.0, "_card_tx_count": 0.0, "_securities_cnt": 0.0,
        "_insurance_cnt": 0.0, "_online_ins_cnt": 0.0,
    }
    for col, val in float_defaults.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)
        else:
            df[col] = val

    # 의료 기록 없는 고객(70%): 나이 기반 기본 건강점수 (정규화 스케일 13~100)
    # age 20→~92, age 40→~76, age 60→~60, age 70→~52
    _age = df["age"].clip(lower=20, upper=80) if "age" in df.columns else pd.Series(40.0, index=df.index)
    _health_default = (92.0 - (_age - 20) * 0.8 + pd.Series(
        np.random.normal(0, 6, len(df)), index=df.index
    )).clip(lower=13, upper=92).round(1)
    if "_health_score" in df.columns:
        df["_health_score"] = df["_health_score"].fillna(_health_default)
    else:
        df["_health_score"] = _health_default

    # ── 4. 파생 Feature 계산 ──────────────────────────────────
    print("[4/7] 파생 Feature 계산...")

    # 금융
    df["asset_growth_90d"]  = 0.0
    df["etf_ratio"]         = 0.0
    df["policy_cnt"]        = (df.get("_insurance_cnt", 0) + df.get("_online_ins_cnt", 0)).astype(float)

    total_assets = df["balance_30d_avg"] + df["invest_total"] + df.get("_insurance_cnt", 0) * 500_000
    df["invest_ratio"] = (df["invest_total"] / total_assets.clip(lower=1.0)).round(4)

    # 건강
    df["health_risk_score"] = (100.0 - df.get("_health_score", 50.0)).clip(lower=0.0).round(1)
    df["step_growth_30d"]   = 0.0

    # 행동 (행동 로그 미제공)
    df["login_cnt_30d"]        = 0.0
    df["avg_session_min"]      = 0.0
    df["push_click_rate"]      = 0.0
    df["recommend_click_rate"] = 0.0
    df["last_active_days"]     = 0.0

    # 관계
    affiliate_expr = (
        (df.get("_bank_tx_count",    0) > 0).astype(int) +
        (df.get("_card_tx_count",    0) > 0).astype(int) +
        (df.get("_securities_cnt",   0) > 0).astype(int) +
        (df.get("_insurance_cnt",    0) > 0).astype(int) +
        (df.get("_online_ins_cnt",   0) > 0).astype(int) +
        (df.get("hospital_visit_90d",0) > 0).astype(int)
    )
    df["affiliate_cnt"]     = affiliate_expr.astype(float)
    df["consent_ratio"]     = df["consent_flag"].apply(lambda x: 1.0 if x == "Y" else 0.0)
    df["membership_days"]   = df["wearable_join_dt"].apply(_membership_days)
    product_cnt = (
        df.get("_insurance_cnt",  0) +
        df.get("_online_ins_cnt", 0) +
        (df.get("_securities_cnt", 0) > 0).astype(int)
    )
    df["cross_product_score"] = (product_cnt * df["affiliate_cnt"]).astype(float)

    # lifesync_score (설계 문서 tier 기반, base 없음, max 100)
    # = financial(40) + health(25) + relationship(15) + growth(10) - risk(20)

    bal  = df["balance_30d_avg"].fillna(0.0)
    card = df["card_spend_30d"].fillna(0.0)
    inv  = df["invest_total"].fillna(0.0)

    # 1) financial_score (max 40)
    balance_score = pd.Series(np.select(
        [bal >= 50_000_000, bal >= 30_000_000, bal >= 10_000_000, bal >= 3_000_000],
        [15, 12, 8, 5], default=2
    ), index=df.index, dtype=float)
    card_score = pd.Series(np.select(
        [card >= 5_000_000, card >= 3_000_000, card >= 1_500_000, card >= 500_000],
        [10, 8, 5, 3], default=1
    ), index=df.index, dtype=float)
    invest_score = pd.Series(np.select(
        [inv >= 100_000_000, inv >= 50_000_000, inv >= 30_000_000, inv >= 10_000_000],
        [15, 12, 8, 5], default=1
    ), index=df.index, dtype=float)
    financial_score = (balance_score + card_score + invest_score).clip(upper=40)

    # 2) health_sub_score (max 25)
    hs       = df["_health_score"].fillna(50.0)
    steps    = df.get("avg_steps_30d",  pd.Series(0.0,  index=df.index)).fillna(0.0)
    stress   = df.get("stress_avg_30d", pd.Series(50.0, index=df.index)).fillna(50.0)
    sleep_hr = df.get("avg_sleep_30d",  pd.Series(7.0,  index=df.index)).fillna(7.0)

    steps_score = pd.Series(np.select(
        [steps >= 12000, steps >= 10000, steps >= 7000, steps >= 5000],
        [10, 8, 6, 3], default=1
    ), index=df.index, dtype=float)
    wellness_score = pd.Series(np.select(
        [hs >= 90, hs >= 80, hs >= 70, hs >= 60],
        [10, 8, 6, 3], default=1
    ), index=df.index, dtype=float)
    stress_sleep_score = pd.Series(np.select(
        [(stress <= 30) & (sleep_hr >= 7.0) & (sleep_hr <= 8.0),
         (stress <= 60) | ((sleep_hr >= 6.0) & (sleep_hr < 7.0))],
        [5, 3], default=1
    ), index=df.index, dtype=float)
    health_sub_score = (steps_score + wellness_score + stress_sleep_score).clip(upper=25)

    # 3) relationship_score (max 15)
    aff  = df["affiliate_cnt"].fillna(0.0)
    cons = df["consent_ratio"].fillna(0.0)
    affiliate_score = pd.Series(np.select(
        [aff >= 5, aff >= 3, aff >= 2],
        [10, 7, 4], default=2
    ), index=df.index, dtype=float)
    consent_score = pd.Series(np.select(
        [cons >= 0.8, cons >= 0.5],
        [5, 3], default=0
    ), index=df.index, dtype=float)
    relationship_score = (affiliate_score + consent_score).clip(upper=15)

    # 4) growth_score (max 10) — 3개월 증가 추세 proxy
    growth_score = pd.Series(np.select(
        [(inv > 0) & (hs >= 70),
         (card > 0) & (inv > 0),
         (inv > 0) | (card > 0)],
        [10, 7, 4], default=1
    ), index=df.index, dtype=float)

    # 5) risk_score (max 20)
    hosp = df.get("hospital_visit_90d", pd.Series(0.0, index=df.index)).fillna(0.0)
    visit_risk  = pd.Series(np.where(hosp > 6, 5, 0),
                            index=df.index, dtype=float)
    health_risk = pd.Series(np.where(df["health_risk_score"] > 60, 10, 0),
                            index=df.index, dtype=float)
    risk_score = (visit_risk + health_risk).clip(upper=20)

    df["lifesync_score"] = (
        financial_score + health_sub_score + relationship_score + growth_score - risk_score
    ).clip(lower=0, upper=100).round(1)

    # 성장 (이전 데이터 미제공)
    df["spend_growth_90d"]   = 0.0
    df["invest_growth_90d"]  = 0.0
    df["wellness_growth_30d"]= 0.0

    # Risk (이전 데이터 미제공)
    df["inactive_days"]   = 0.0
    df["card_drop_ratio"] = 0.0
    df["asset_drop_ratio"]= 0.0
    df["complaint_flag"]  = 0.0

    # Label
    df["vip_label"]              = df["vip_flag"].apply(lambda x: 1 if x == "Y" else 0)
    # Signup proxy: 계열사 3개 이상 이용 고객 → 추가 가입 가능성 높음
    df["churn_label"]            = (df["affiliate_cnt"] >= 3).astype(int)
    df["pb_contract_label"]      = 0
    # Rec proxy: 투자 자산 보유 고객 → 상품 구매 가능성 높음
    df["product_purchase_label"] = (df["invest_total"] > 0).astype(int)

    # ── 5. 최종 컬럼 선택 ────────────────────────────────────
    print("[5/7] 최종 컬럼 정리...")
    final_cols = [f.name for f in SCHEMA]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None
    df = df[final_cols]

    # float 컬럼 반올림
    float_cols = [f.name for f in SCHEMA if f.field_type == "FLOAT"]
    df[float_cols] = df[float_cols].round(4)

    print(f"      행 수:       {len(df):,}")
    print(f"      VIP 수:      {(df['vip_label']==1).sum():,}")
    print(f"      평균 계열사: {df['affiliate_cnt'].mean():.2f}")
    print(f"      평균 건강위험: {df['health_risk_score'].mean():.1f}")

    # ── 6. BigQuery 업로드 ───────────────────────────────────
    print("[6/7] BigQuery 업로드...")
    client    = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    tbl = client.get_table(table_ref)
    print(f"      완료 → {table_ref}")
    print(f"      총 {tbl.num_rows:,} 행 업로드됨")

    # ── 7. lifesync_ml 학습 데이터 테이블 생성 ────────────────
    print("[7/7] lifesync_ml 학습 데이터 테이블 생성...")
    label_cols = {"vip_label", "churn_label", "pb_contract_label", "product_purchase_label"}
    all_feature_names = [f.name for f in SCHEMA if f.name not in label_cols]
    health_feature_names = [n for n in all_feature_names if n != "health_risk_score"]

    feat_cols    = ", ".join(all_feature_names)
    h_feat_cols  = ", ".join(health_feature_names)

    client.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.vip_training_data` AS
        SELECT {feat_cols}, vip_label
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """).result()

    client.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.rec_training_data` AS
        SELECT {feat_cols}, product_purchase_label
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """).result()

    client.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.health_training_data` AS
        SELECT {h_feat_cols}, health_risk_score
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """).result()

    print("      완료 → lifesync_ml.vip_training_data / rec_training_data / health_training_data")
    print("=" * 60)


if __name__ == "__main__":
    main()
