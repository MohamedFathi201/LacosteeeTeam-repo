FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir azure-iot-device

COPY car_simulator.py .
COPY azure_config.py .

CMD ["python", "-u", "car_simulator.py", "--output-path", "/data/car_stream.jsonl", "--real-time"]