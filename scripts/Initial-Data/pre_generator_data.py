# ==========================================================
# LifeSync360 FULL Generator.py (최신 완성본)
#
# 입력:
#   customer_master.csv
#   customer_identity_map.csv
#
# 출력:
#   bank_20250901.json
#   card_20250901.json
#   insurance_20250901.json
#   online_insurance_20250901.json
#   hospital_20250901.json
#   healthcare_20250901.json
#   securities_20250901.json
#   wearable_20250901.json
#
# 핵심 반영사항
# 1. merge key 개선 (global_customer_id)
# 2. 가입자 / 동의 / VIP 정책 반영
# 3. wearable_flag 기반 생성
# 4. VIP 소비/자산 우대 패턴
# 5. 현실형 JSON 구조
# ==========================================================

import pandas as pd
import json
import random
import uuid
from tqdm import tqdm
from datetime import datetime, timedelta

# ==========================================================
# 설정
# ==========================================================

MASTER_FILE = "customer_master.csv"
MAP_FILE = "customer_identity_map.csv"

BATCH_DT = "2025-09-01T10:00:00Z"

# ==========================================================
# CSV 로드
# ==========================================================

print("Load CSV...")

df_master = pd.read_csv(MASTER_FILE)
df_map = pd.read_csv(MAP_FILE)

# 핵심 수정: ls_user_id NULL 문제 방지
df = pd.merge(
    df_master,
    df_map,
    on="global_id",
    how="inner",
    suffixes=("_m", "_map")
)

# master 기준 사용
df["ls_user_id"] = df["ls_user_id_m"]

print("Customer Count:", len(df))

# ==========================================================
# 공통 함수
# ==========================================================

def rand_dt():
    base = datetime(2025, 9, 1, 10, 0, 0)
    sec = random.randint(0, 86400)
    return (base + timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%SZ")

def save_json(path, source, records):
    payload = {
        "source": source,
        "batch_dt": BATCH_DT,
        "record_count": len(records),
        "records": records
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

# ==========================================================
# 리스트
# ==========================================================

bank = []
card = []
insurance = []
online_insurance = []
hospital = []
healthcare = []
securities = []
wearable = []

# ==========================================================
# 메인 생성
# ==========================================================

for _, c in tqdm(df.iterrows(), total=len(df), desc="Generate"):

    vip = c["vip_flag"] == "Y"
    joined = c["join_status"] == "ACTIVE"
    consent = c["consent_flag"] == "Y"

    age = c["age"]
    income = c["income_grade"]

    # ------------------------------------------------------
    # BANK
    # ------------------------------------------------------
    if pd.notna(c["bank_id"]):

        txn_cnt = random.randint(1, 5)
        if vip:
            txn_cnt += 3

        for _ in range(txn_cnt):

            amt = random.randint(10000, 3000000)
            if vip:
                amt *= 3

            bank.append({
                "global_id": c["global_id"],
                "ls_user_id": c["ls_user_id"],
                "bank_id": c["bank_id"],
                "transaction_id": f"TXN-{uuid.uuid4().hex[:12]}",
                "transaction_dt": rand_dt(),
                "transaction_type": random.choice(
                    ["DEPOSIT", "WITHDRAW", "TRANSFER"]
                ),
                "amount": amt,
                "balance_after": random.randint(0, 50000000),
                "channel": random.choice(
                    ["APP", "ATM", "BRANCH"]
                )
            })

    # ------------------------------------------------------
    # CARD
    # ------------------------------------------------------
    if pd.notna(c["card_id"]):

        cnt = random.randint(1, 8)

        for _ in range(cnt):

            amount = random.randint(5000, 800000)

            # 40대 고소득 소비 증가
            if age >= 40 and age < 50 and income == "HIGH":
                amount *= 4

            if vip:
                amount *= 2

            card.append({
                "global_id": c["global_id"],
                "ls_user_id": c["ls_user_id"],
                "card_id": c["card_id"],
                "approval_no": f"APR-{uuid.uuid4().hex[:12]}",
                "approval_dt": rand_dt(),
                "merchant_name": random.choice(
                    ["스타벅스","쿠팡","이마트","병원","CGV"]
                ),
                "merchant_category": random.choice(
                    ["FOOD","SHOPPING","MEDICAL","TRAVEL"]
                ),
                "amount": amount,
                "installment_months": random.choice([0,3,6,12]),
                "payment_status": "APPROVED"
            })

    # ------------------------------------------------------
    # INSURANCE
    # ------------------------------------------------------
    if pd.notna(c["insurance_id"]):

        premium = random.randint(30000, 400000)
        if vip:
            premium *= 3

        insurance.append({
            "global_id": c["global_id"],
            "ls_user_id": c["ls_user_id"],
            "insurance_id": c["insurance_id"],
            "policy_no": f"POL-{random.randint(100000,999999)}",
            "product_name": random.choice(
                ["실손보험","종신보험","자동차보험"]
            ),
            "premium_amount": premium,
            "payment_cycle": random.choice(
                ["MONTHLY","ANNUAL"]
            ),
            "payment_status": "PAID"
        })

    # ------------------------------------------------------
    # ONLINE INSURANCE
    # ------------------------------------------------------
    if pd.notna(c["online_insurance_id"]):

        buy_flag = "Y" if consent else "N"

        online_insurance.append({
            "global_id": c["global_id"],
            "ls_user_id": c["ls_user_id"],
            "online_insurance_id": c["online_insurance_id"],
            "quote_id": f"Q-{uuid.uuid4().hex[:10]}",
            "channel": "MOBILE",
            "product_name": random.choice(
                ["여행자보험","펫보험","미니암보험"]
            ),
            "premium_quote": random.randint(5000,70000),
            "purchase_flag": buy_flag,
            "event_dt": rand_dt()
        })

    # ------------------------------------------------------
    # HOSPITAL
    # ------------------------------------------------------
    # ── 웨어러블 프로필 (HEALTHCARE 점수 계산 + WEARABLE 레코드에 공유) ─────────
    if c["wearable_flag"] == "Y":
        _step_base = max(2000, int(12000 - max(0, age - 20) * 130 + random.gauss(0, 1500)))
        _hr_base   = min(120, int(65 + max(0, age - 25) * 0.5 + random.gauss(0, 5)))
        _spo2_base = round(max(93.0, min(99.9, 98.5 - max(0, age - 40) * 0.05 + random.gauss(0, 0.4))), 1)
        _stress    = max(0, min(100, int(25 + max(0, age - 30) * 0.6 + random.gauss(0, 15))))
        _sleep     = round(max(3.5, min(9.5, 7.5 - max(0, age - 50) * 0.03 + random.gauss(0, 0.7))), 1)
        _workout   = max(0, min(7, int(4 - max(0, age - 30) * 0.05 + random.gauss(0, 1.2))))
        if vip:
            _step_base = int(_step_base * 1.2)
            _workout   = min(7, _workout + 1)
    else:
        _step_base = max(1000, int(4000 - max(0, age - 20) * 50))
        _hr_base   = min(120, int(72 + max(0, age - 25) * 0.7))
        _spo2_base = round(max(92.0, min(98.5, 97.0 - max(0, age - 40) * 0.1)), 1)
        _stress    = max(0, min(100, int(40 + max(0, age - 30) * 0.5)))
        _sleep     = round(max(3.0, min(9.0, 6.5 - max(0, age - 50) * 0.03)), 1)
        _workout   = 0

    hospital_diagnoses = []  # HEALTHCARE 패널티 계산용
    health_score_val   = None  # WEARABLE wellness_score 폴백용

    if pd.notna(c["hospital_id"]):

        visit_cnt = 1

        # 고령일수록 방문 빈도 증가
        if age >= 50:
            visit_cnt += random.randint(1, 3)

        for _ in range(visit_cnt):

            diag = random.choice(["J00","E11","I10","M54"])
            hospital_diagnoses.append(diag)

            hospital.append({
                "global_id": c["global_id"],
                "ls_user_id": c["ls_user_id"],
                "hospital_id": c["hospital_id"],
                "visit_id": f"VIS-{uuid.uuid4().hex[:10]}",
                "visit_dt": rand_dt(),
                "department": random.choice(
                    ["내과","치과","정형외과","피부과"]
                ),
                "diagnosis_code": diag,
                "cost": random.randint(10000,500000)
            })

    # ------------------------------------------------------
    # HEALTHCARE
    # ------------------------------------------------------
    if pd.notna(c["healthcare_id"]):

        # 1) activity_score (max 25) — 걸음수 + 운동빈도
        steps_s  = (15 if _step_base >= 12000 else 12 if _step_base >= 10000
                    else 8 if _step_base >= 7000 else 5 if _step_base >= 5000 else 2)
        workout_s = 10 if _workout >= 5 else 7 if _workout >= 3 else 4 if _workout >= 1 else 1
        activity_score = steps_s + workout_s

        # 2) bio_score (max 25) — 심박수 + 산소포화도 + BMI
        hr_s   = 10 if _hr_base <= 75 else 8 if _hr_base <= 90 else 5 if _hr_base <= 110 else 2
        spo2_s = 10 if _spo2_base >= 97 else 8 if _spo2_base >= 95 else 5 if _spo2_base >= 93 else 2
        bmi_val = round(max(18.0, min(40.0, 22.0 + max(0, age - 30) * 0.1 + random.gauss(0, 2.5))), 1)
        bmi_s  = 5 if 18.5 <= bmi_val <= 24.9 else 3 if bmi_val <= 29.9 else 1
        bio_score = hr_s + spo2_s + bmi_s

        # 3) lifestyle_score (max 15) — 스트레스 + 수면
        stress_s = 8 if _stress <= 30 else 5 if _stress <= 60 else 2
        sleep_s  = 7 if 7 <= _sleep <= 8 else 5 if _sleep >= 6 else 3 if _sleep >= 5 else 1
        lifestyle_score = stress_s + sleep_s

        # 4) prevent_score (max 15) — 건강검진(1년 이내) + 걷기목표 달성
        checkup_s   = 10  # 헬스케어 레코드 있음 = 1년 이내 검진
        habit_bonus = 5 if _step_base >= 10000 else 0
        prevent_score = checkup_s + habit_bonus

        # 5) disease_penalty (max -15) — health_risk 구간 + 만성질환 플래그
        has_chronic   = any(d in ["E11", "I10"] for d in hospital_diagnoses)
        visit_cnt_30d = len(hospital_diagnoses)
        base_risk = min(100, int(max(0, (age - 20) * 0.8) + visit_cnt_30d * 10))
        if has_chronic:
            base_risk = max(base_risk, 80)
        disease_base_p = 15 if base_risk >= 80 else 10 if base_risk >= 60 else 5 if base_risk >= 40 else 0
        chronic_flag_p = 5 if has_chronic else 0
        disease_penalty = min(15, disease_base_p + chronic_flag_p)

        # 6) visit_penalty (max -10) — 병원 방문 과다
        visit_penalty = 10 if visit_cnt_30d >= 6 else 7 if visit_cnt_30d >= 4 else 3 if visit_cnt_30d >= 2 else 0

        # 합산 (원점수 10~80) → 100점 정규화
        health_score_raw = max(10, min(80,
            activity_score + bio_score + lifestyle_score + prevent_score
            - disease_penalty - visit_penalty
        ))
        health_score_val = int(round(health_score_raw / 80 * 100))

        bp = "120/80" if _hr_base <= 75 else ("130/85" if _hr_base <= 90 else "140/90")

        healthcare.append({
            "global_id":      c["global_id"],
            "ls_user_id":     c["ls_user_id"],
            "healthcare_id":  c["healthcare_id"],
            "height_cm":      random.randint(150, 190),
            "weight_kg":      random.randint(45, 110),
            "bmi":            bmi_val,
            "blood_pressure": bp,
            "health_score":   health_score_val,
            "checkup_dt":     rand_dt()
        })

    # ------------------------------------------------------
    # SECURITIES
    # ------------------------------------------------------
    if pd.notna(c["securities_id"]):

        trade_cnt = 1
        if vip:
            trade_cnt = 5

        for _ in range(trade_cnt):

            securities.append({
                "global_id": c["global_id"],
                "ls_user_id": c["ls_user_id"],
                "securities_id": c["securities_id"],
                "account_no": f"SEC-{random.randint(100000,999999)}",
                "trade_dt": rand_dt(),
                "symbol": random.choice(
                    ["005930","000660","035420","AAPL","TSLA"]
                ),
                "trade_type": random.choice(["BUY","SELL"]),
                "qty": random.randint(1,50),
                "price": random.randint(10000,300000)
            })

    # ------------------------------------------------------
    # WEARABLE (핵심)
    # ------------------------------------------------------
    if c["wearable_flag"] == "Y":

        vitals = []
        ts_base = datetime.utcnow()

        _wellness_base = health_score_val if health_score_val is not None \
                         else max(20, int(90 - max(0, age - 20) * 0.5))

        for i in range(10):

            vitals.append({
                "ts":            (ts_base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "heart_rate":    int(max(50, min(130, _hr_base   + random.randint(-6, 6)))),
                "spo2":          round(max(90.0, min(100.0, _spo2_base + random.uniform(-0.5, 0.5))), 1),
                "body_temp":     round(random.uniform(36.0, 37.5), 1),
                "steps":         int(max(0, _step_base // 10 + random.randint(-100, 100))),
                "stress_score":  max(0, min(100, _stress + random.randint(-5, 5))),
                "sleep_hours":   round(max(3.0, min(10.0, _sleep + random.uniform(-0.2, 0.2))), 1),
                "wellness_score": max(0, min(100, _wellness_base + random.randint(-3, 3))),
            })

        wearable.append({
            "global_id": c["global_id"],
            "ls_user_id": c["ls_user_id"],
            "device_id": f"DEV-{uuid.uuid4().hex[:8]}",
            "device_type": c["device_type"],
            "wearable_join_dt": c["wearable_join_dt"],
            "event_dt": rand_dt(),
            "vitals": vitals
        })

# ==========================================================
# 저장
# ==========================================================

save_json("bank_20250901.json", "bank", bank)
save_json("card_20250901.json", "card", card)
save_json("insurance_20250901.json", "insurance", insurance)
save_json("online_insurance_20250901.json", "online_insurance", online_insurance)
save_json("hospital_20250901.json", "hospital", hospital)
save_json("healthcare_20250901.json", "healthcare", healthcare)
save_json("securities_20250901.json", "securities", securities)
save_json("wearable_20250901.json", "wearable", wearable)

# ==========================================================
# 결과
# ==========================================================

print("===================================================")
print("LifeSync360 FULL Generator 완료")
print("===================================================")
print("BANK        :", len(bank))
print("CARD        :", len(card))
print("INSURANCE   :", len(insurance))
print("ONLINE INS  :", len(online_insurance))
print("HOSPITAL    :", len(hospital))
print("HEALTHCARE  :", len(healthcare))
print("SECURITIES  :", len(securities))
print("WEARABLE    :", len(wearable))
print("===================================================")