FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY predict_runner.py .

ENV PORT=8080

CMD ["python", "predict_runner.py"]
