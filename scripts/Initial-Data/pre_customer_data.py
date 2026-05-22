# ==========================================================
# LifeSync360 고객 100만명 기본 마스터 생성기 (Wearable 반영 최종판)
#
# 반영 기준
# 전체 고객 : 1,000,000
# 가입자 30%
# 미가입자 70%
# 동의 고객 = 가입자 중 20%
# VIP 고객 = 전체 5%
#
# Wearable 정책
# - 가입자 + 동의 고객만 가능
# - VIP 고객 우선 지급 80%
# - 일반 동의 고객 지급 20%
#
# 출력:
# customer_master.csv
# customer_identity_map.csv
# ==========================================================

import pandas as pd
import random
import uuid
from faker import Faker
from tqdm import tqdm
from datetime import datetime, timedelta

fake = Faker("ko_KR")

# ==========================================================
# 설정
# ==========================================================

CUSTOMER_COUNT = 1_000_000

JOIN_RATIO = 0.30
CONSENT_RATIO = 0.20
VIP_RATIO = 0.05

# 계열사 가입률
BANK_RATIO = 1.00
CARD_RATIO = 0.70
INS_RATIO = 0.50
ONLINE_INS_RATIO = 0.25
HOSPITAL_RATIO = 0.20
HEALTH_RATIO = 0.30
SEC_RATIO = 0.25

# ==========================================================
# 함수
# ==========================================================

def make_code(prefix, num):
    return f"{prefix}-{num:08d}"

def rand_join_dt():
    start = datetime(2024, 1, 1)
    days = random.randint(0, 600)
    dt = start + timedelta(days=days)
    return dt.strftime("%Y-%m-%d")

def wearable_policy(vip, joined, consent):

    if not joined:
        return False

    if not consent:
        return False

    # VIP 우선 지급
    if vip:
        return random.random() <= 0.80

    # 일반 고객
    return random.random() <= 0.20

# ==========================================================
# 데이터 생성
# ==========================================================

master_rows = []
map_rows = []

for i in tqdm(range(1, CUSTOMER_COUNT + 1), desc="Generating"):

    global_id = f"G{i:09d}"

    # ------------------------------------------------------
    # 가입 여부
    # ------------------------------------------------------
    joined = random.random() <= JOIN_RATIO

    if joined:
        ls_user_id = f"LS-{uuid.uuid4().hex[:8].upper()}-{i:06d}"
        join_status = "ACTIVE"
    else:
        ls_user_id = None
        join_status = "NOT_JOINED"

    # ------------------------------------------------------
    # 동의 여부
    # ------------------------------------------------------
    if joined:
        consent = random.random() <= CONSENT_RATIO
    else:
        consent = False

    consent_flag = "Y" if consent else "N"

    # ------------------------------------------------------
    # VIP 여부
    # ------------------------------------------------------
    vip = random.random() <= VIP_RATIO
    vip_flag = "Y" if vip else "N"

    # ------------------------------------------------------
    # 기본 속성
    # ------------------------------------------------------
    age = random.randint(20, 79)

    if age < 30:
        income = "LOW"
    elif age < 45:
        income = "MID"
    elif age < 60:
        income = "HIGH"
    else:
        income = "MID"

    if vip:
        income = random.choice(["HIGH","HIGH","MID"])

    asset_grade = random.choice(["A","B","C","D"])

    if vip:
        asset_grade = random.choice(["A","A","B"])

    # ------------------------------------------------------
    # Wearable 지급
    # ------------------------------------------------------
    wearable_flag = "N"
    device_type = None
    wearable_join_dt = None

    if wearable_policy(vip, joined, consent):

        wearable_flag = "Y"

        if vip:
            device_type = random.choice([
                "APPLE_WATCH",
                "GALAXY_WATCH"
            ])
        else:
            device_type = random.choice([
                "FITBIT",
                "GALAXY_BAND",
                "MI_BAND"
            ])

        wearable_join_dt = rand_join_dt()

    # ------------------------------------------------------
    # customer_master
    # ------------------------------------------------------
    customer = {
        "global_id": global_id,
        "ls_user_id": ls_user_id,
        "customer_name": fake.name(),
        "gender": random.choice(["M","F"]),
        "age": age,
        "birth_year": 2025 - age,
        "region": random.choice([
            "SEOUL","BUSAN","DAEJEON",
            "INCHEON","DAEGU","GWANGJU"
        ]),
        "job_group": random.choice([
            "OFFICE","SELF","PUBLIC",
            "STUDENT","RETIRED"
        ]),
        "income_grade": income,
        "asset_grade": asset_grade,
        "join_status": join_status,
        "consent_flag": consent_flag,
        "vip_flag": vip_flag,

        # 핵심 추가 컬럼
        "wearable_flag": wearable_flag,
        "device_type": device_type,
        "wearable_join_dt": wearable_join_dt
    }

    master_rows.append(customer)

    # ------------------------------------------------------
    # 고객 매핑
    # ------------------------------------------------------
    mapping = {
        "global_id": global_id,
        "ls_user_id": ls_user_id,
        "bank_id": None,
        "card_id": None,
        "insurance_id": None,
        "online_insurance_id": None,
        "hospital_id": None,
        "healthcare_id": None,
        "securities_id": None
    }

    if random.random() <= BANK_RATIO:
        mapping["bank_id"] = make_code("BNK", i)

    if random.random() <= CARD_RATIO:
        mapping["card_id"] = make_code("CRD", i)

    if random.random() <= INS_RATIO:
        mapping["insurance_id"] = make_code("INS", i)

    if random.random() <= ONLINE_INS_RATIO:
        mapping["online_insurance_id"] = make_code("OIN", i)

    if random.random() <= HOSPITAL_RATIO:
        mapping["hospital_id"] = make_code("HSP", i)

    if random.random() <= HEALTH_RATIO:
        mapping["healthcare_id"] = make_code("HLT", i)

    if random.random() <= SEC_RATIO:
        mapping["securities_id"] = make_code("SEC", i)

    map_rows.append(mapping)

# ==========================================================
# DataFrame
# ==========================================================

df_master = pd.DataFrame(master_rows)
df_map = pd.DataFrame(map_rows)

# ==========================================================
# 저장
# ==========================================================

df_master.to_csv(
    "customer_master.csv",
    index=False,
    encoding="utf-8-sig"
)

df_map.to_csv(
    "customer_identity_map.csv",
    index=False,
    encoding="utf-8-sig"
)

# ==========================================================
# 통계
# ==========================================================

print("===================================================")
print("LifeSync360 Customer Master 생성 완료")
print("===================================================")

print("총 고객수        :", len(df_master))
print("가입자 수        :", len(df_master[df_master['join_status']=='ACTIVE']))
print("동의 고객 수     :", len(df_master[df_master['consent_flag']=='Y']))
print("VIP 고객 수      :", len(df_master[df_master['vip_flag']=='Y']))
print("Wearable 지급 수 :", len(df_master[df_master['wearable_flag']=='Y']))

print("===================================================")