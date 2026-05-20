# Vertex AI 리소스는 train.py로 모델 학습 후 Model Registry에 직접 등록
# BatchPredictionJob은 predict_runner.py(Cloud Run)에서 직접 호출
# → 별도 Dataset / Endpoint 리소스 불필요
