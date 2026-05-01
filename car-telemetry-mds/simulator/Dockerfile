FROM python:3.11-slim

LABEL maintainer="LacosteeeTeam" \
      description="Car sensor telemetry simulator"

WORKDIR /app

# No external dependencies — stdlib only
COPY car_simulator.py .

# Run the simulator writing to /data volume
CMD ["python", "-u", "car_simulator.py"]
