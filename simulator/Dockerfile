FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY car_simulator.py .

CMD ["bash", "-c", "mkdir -p /data && touch /data/car_stream.jsonl && chmod 666 /data/car_stream.jsonl && python -u car_simulator.py --output-path /data/car_stream.jsonl --real-time"]