#!/usr/bin/env python3
"""
Continuously generate car telemetry records.

OUTPUT_MODE (env var, via azure_config) chooses WHERE records go:
  "batch"  -> JSONL file (Airflow batch pipeline) — answers "what happened?"
  "stream" -> IoT Hub (Stream Analytics) — answers "what's happening now?"

These are run as separate sessions, not simultaneously from one process.
Batch needs to pause and let Airflow drain the file; stream needs to never
pause. Forcing both into one loop means either the file grows unbounded
or the stream freezes while waiting on Airflow — a real contradiction, not
a bug to route around. (A production fix for true continuous batch+stream
would be file rotation: write car_stream_0001.jsonl, roll to a new file
every N records, let Airflow watch a directory instead of one file. Not
needed here — running batch and stream as separate sessions demonstrates
both pipelines without that complexity.)


"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass, field 
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from azure.iot.device import IoTHubDeviceClient, Message
from azure_config import OUTPUT_MODE, IOTHUB_CONNECTION_STRING


VALID_MODES = {"batch", "stream"}


@dataclass
class SimulatorState:
    tick: int = 0
    speed_kmh: float = 0.0
    rpm: float = 850.0
    battery_voltage: float = 13.8
    tyre_pressure: list = field(default_factory=lambda: [32.5, 32.5, 32.5, 32.5])
    tyre_temp: list = field(default_factory=lambda: [28.0, 28.0, 28.0, 28.0])
    engine_temp: float = 72.0
    oil_temp: float = 65.0
    fuel_level: float = 54.0
    odometer_km: float = 0.0
    trip_km: float = 0.0
    fault_active: bool = False
    failed_tyre: int | None = None
    fault_type: str = "NONE"

    active_failures = {
        "cooling_system": False,
        "oil_leak": False,
        "battery_degradation": False,
        "fuel_leak": False,
        "tyre_puncture": False,
    }

    failure_duration = {
        "cooling_system": 0,
        "oil_leak": 0,
        "battery_degradation": 0,
        "fuel_leak": 0,
        "tyre_puncture": 0,
        }


   

class CarTelemetrySimulator:
    def __init__(
        self,
        output_path: str,
        real_time: bool = True,
        ticks: int | None = None,
        tick_interval_seconds: float = 0.5,
        seed: int = 42,
    ) -> None:
        if OUTPUT_MODE not in VALID_MODES:
            raise ValueError(f"OUTPUT_MODE must be one of {VALID_MODES}, got {OUTPUT_MODE!r}")

        self.output_path = Path(output_path)
        self.real_time = real_time
        self.ticks = ticks
        self.tick_interval_seconds = tick_interval_seconds
        self.random = __import__("random").Random(seed)
        self.state = SimulatorState()

        self.writes_to_file = OUTPUT_MODE == "batch"
        self.writes_to_iothub = OUTPUT_MODE == "stream"

        # Stream mode always paces in real time. There's no real use case
        # for unpaced IoT Hub sends — it just burns through the daily
        # message quota faster with no benefit, since Stream Analytics and
        # the dashboard don't care about burst speed. Override rather than
        # silently ignore, so --no-real-time doesn't appear to do nothing.
        if self.writes_to_iothub and not self.real_time:
            print("Warning: --no-real-time is ignored in stream mode; forcing real-time pacing.")
            self.real_time = True
       

        self.iot_client: IoTHubDeviceClient | None = None
        if self.writes_to_iothub:
            if not IOTHUB_CONNECTION_STRING:
                raise ValueError("IOTHUB_CONNECTION_STRING is required when OUTPUT_MODE includes streaming")
            self.iot_client = IoTHubDeviceClient.create_from_connection_string(IOTHUB_CONNECTION_STRING)
   
   
    #-----------------------------------------------
        # MAIN LOOP
    #-----------------------------------------------
   
    def run(self) -> None:
        if self.writes_to_file:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.touch(exist_ok=True)
            self.output_path.chmod(0o666)
        if self.iot_client is not None:
            self.iot_client.connect()
            print("Connected to IoT Hub.")

        emitted = 0
        # Bulk batches only make sense for the file path when not pacing in
        # real time. Streaming always sends one message at a time, paced,
        # since each send is a blocking network call.
        batch_size = 10000 if self.writes_to_file else 1

        try:
            while self.ticks is None or emitted < self.ticks:
                written = 0
                file_handle = (
                    self.output_path.open("a", encoding="utf-8") if self.writes_to_file else None
                )
                try:
                    for _ in range(batch_size):
                        if self.ticks is not None and emitted >= self.ticks:
                            break

                        record = self._next_record()
                        line = json.dumps(record)

                        if self.writes_to_file:
                            file_handle.write(line + "\n")
                        elif self.writes_to_iothub:
                            message = Message(line)
                            message.content_type = "application/json"
                            message.content_encoding = "utf-8"
                            self.iot_client.send_message(message)

                        emitted += 1
                        written += 1

                        if self.real_time:
                            time.sleep(self.tick_interval_seconds)
                finally:
                    if file_handle is not None:
                        file_handle.close()

                print(f"wrote {written} records (mode={OUTPUT_MODE}, real_time={self.real_time})")

                # Batch mode pauses to let Airflow drain the file before
                # continuing — this is what prevents unbounded file growth.
                if self.writes_to_file:
                    print("Waiting for Airflow to consume the file...")
                    start = time.time()
                    while self.output_path.stat().st_size > 0:
                        if time.time() - start > 120:
                            print("Airflow did not consume the file within 2 minutes. Continuing...")
                            break
                        time.sleep(2)
                    print("Resuming simulation...")
        finally:
            if self.iot_client is not None:
                self.iot_client.shutdown()
                print("Disconnected from IoT Hub.")

   # ------------------------------------------------------------------
    # Record generation
   # ------------------------------------------------------------------ 
   
    def _next_record(self) -> dict[str, Any]:
        dt_seconds = self.tick_interval_seconds
        tick = self.state.tick
        cycle = tick % 240
 
        # ------------------------------------------------------------------
        # Step 1 — Random failure onset
        # Each subsystem fails independently at a low per-tick probability.
        # ------------------------------------------------------------------
        s = self.state
 
        if not s.active_failures["cooling_system"] and self.random.random() < 0.002:
            s.active_failures["cooling_system"] = True
 
        if not s.active_failures["oil_leak"] and self.random.random() < 0.002:
            s.active_failures["oil_leak"] = True
 
        if not s.active_failures["battery_degradation"] and self.random.random() < 0.0015:
            s.active_failures["battery_degradation"] = True
 
        if not s.active_failures["fuel_leak"] and self.random.random() < 0.0015:
            s.active_failures["fuel_leak"] = True
 
        if not s.active_failures["tyre_puncture"] and self.random.random() < 0.003:
            s.active_failures["tyre_puncture"] = True
            s.failed_tyre = self.random.randint(0, 3)
 
        # ------------------------------------------------------------------
        # Step 2 — Failure progression (duration counter)
        # ------------------------------------------------------------------
        for failure, active in s.active_failures.items():
            if active:
                s.failure_duration[failure] += 1
 
        # ------------------------------------------------------------------
        # Step 3 — Fault recovery
        # Each active failure has a small per-tick chance to resolve on its
        # own. Without this, every long run ends with all five systems failed.
        # ------------------------------------------------------------------
        for failure in list(s.active_failures):
            if s.active_failures[failure] and self.random.random() < 0.003:
                s.active_failures[failure] = False
                s.failure_duration[failure] = 0
                if failure == "tyre_puncture":
                    s.failed_tyre = None
 
        # ------------------------------------------------------------------
        # Step 4 — Drive cycle
        # ------------------------------------------------------------------
        if cycle < 30:
            drive_state = "IDLE"
            target_speed = 0.0
        elif cycle < 75:
            drive_state = "ACCELERATING"
            target_speed = min(110.0, (cycle - 30) * 2.5)
        elif cycle < 170:
            drive_state = "CRUISING"
            target_speed = 75.0 + 12.0 * self.random.random()
        elif cycle < 210:
            drive_state = "BRAKING"
            target_speed = max(0.0, 90.0 - (cycle - 170) * 2.5)
        else:
            drive_state = "STOPPED"
            target_speed = 0.0
 
        speed_delta = target_speed - s.speed_kmh
        s.speed_kmh = max(
            0.0, s.speed_kmh + speed_delta * 0.35 + self.random.uniform(-0.6, 0.6)
        )
        throttle = min(100.0, max(0.0, 16.0 + speed_delta * 1.8 + self.random.uniform(-5.0, 5.0)))
 
        if s.speed_kmh < 1.0 and drive_state in {"IDLE", "STOPPED"}:
            gear = 0
        elif s.speed_kmh < 20:
            gear = 1
        elif s.speed_kmh < 40:
            gear = 2
        elif s.speed_kmh < 60:
            gear = 3
        elif s.speed_kmh < 85:
            gear = 4
        elif s.speed_kmh < 115:
            gear = 5
        else:
            gear = 6
 
        target_rpm = 820.0 + s.speed_kmh * (35.0 - min(gear, 5) * 3.0) + throttle * 10.0
        s.rpm = max(700.0, target_rpm + self.random.uniform(-120.0, 120.0))
 
        # ------------------------------------------------------------------
        # Step 5 — Baseline sensor calculations
        # ------------------------------------------------------------------
 
        # Engine temp (normal)
        s.engine_temp = min(
            120.0,
            s.engine_temp
            + 0.03 * (90.0 - s.engine_temp)
            + 0.018 * (s.rpm / 1000.0)
            + self.random.uniform(-0.08, 0.08),
        )
 
        # Cooling system failure: progressive overheat
        if s.active_failures["cooling_system"]:
            d = s.failure_duration["cooling_system"]
            s.engine_temp += min(d * 0.03, 20.0)
 
        s.oil_temp = min(
            128.0,
            s.oil_temp
            + 0.025 * (s.engine_temp - s.oil_temp)
            + self.random.uniform(-0.06, 0.06),
        )
 
        # Oil pressure (normal)
        oil_pressure = max(1.0, 1.1 + s.rpm / 1200.0 + self.random.uniform(-0.2, 0.2))
 
        # Oil leak failure: progressive pressure drop
        if s.active_failures["oil_leak"]:
            d = s.failure_duration["oil_leak"]
            oil_pressure -= min(d * 0.001, 3.5)
        oil_pressure = max(0.0, oil_pressure)
 
        # Fuel (normal consumption)
        fuel_flow = max(0.4, 1.1 + throttle * 0.17 + s.rpm / 1800.0)
 
        # Fuel leak failure: extra drain + elevated flow reading
        if s.active_failures["fuel_leak"]:
            s.fuel_level = max(0.0, s.fuel_level - 0.03)
            fuel_flow *= 1.15
 
        fuel_consumed_l = fuel_flow * (dt_seconds / 3600.0)
        s.fuel_level = max(0.0, s.fuel_level - fuel_consumed_l)
        s.odometer_km += s.speed_kmh * (dt_seconds / 3600.0)
        s.trip_km += s.speed_kmh * (dt_seconds / 3600.0)
 
        # Battery — Normal
        # then apply degradation persistently to s.battery_voltage
        ignition_on = True
        alternator_on = s.rpm > 900.0
        target_voltage = 13.8 if alternator_on else 12.3
        s.battery_voltage += 0.05 * (target_voltage - s.battery_voltage)
        s.battery_voltage += self.random.uniform(-0.05, 0.05)
 
        if s.active_failures["battery_degradation"]:
            s.battery_voltage -= 0.015
            alternator_on = False
        s.battery_voltage = max(0.0, s.battery_voltage)
        battery_voltage = s.battery_voltage
 
        # Brake pressure
        if drive_state == "BRAKING":
            brake_pressure = max(0.0, 30.0 + self.random.uniform(-10.0, 20.0))
        elif drive_state == "STOPPED":
            brake_pressure = max(0.0, self.random.uniform(0.0, 1.0))
        else:
            brake_pressure = 0.0
 
        # Tyres — Normal
        # then apply puncture degradation directly to state
        tyre_temp_base = 28.0 + s.speed_kmh * 0.42 + (s.engine_temp - 70.0) * 0.2
        for i in range(4):
            s.tyre_pressure[i] += self.random.uniform(-0.05, 0.05)
            s.tyre_pressure[i] = max(0.0, s.tyre_pressure[i])
            s.tyre_temp[i] += 0.1 * (tyre_temp_base - s.tyre_temp[i])
            s.tyre_temp[i] += self.random.uniform(-0.3, 0.3)
            s.tyre_temp[i] = max(0.0, s.tyre_temp[i])
 
        if s.active_failures["tyre_puncture"] and s.failed_tyre is not None:
            s.tyre_pressure[s.failed_tyre] = max(0.0, s.tyre_pressure[s.failed_tyre] - 0.015 + s.speed_kmh * 0.0002)
            s.tyre_temp[s.failed_tyre] += 0.04
 
        tyre_pressure = [round(p, 2) for p in s.tyre_pressure]
        tyre_temp = [round(t, 2) for t in s.tyre_temp]
 
        # ------------------------------------------------------------------
        # Step 6 — Fault detection from sensor values
        # This is how a real ECU works: read sensors, cross thresholds,
        # raise fault codes. The fault is a consequence, not a cause.
        # ------------------------------------------------------------------
        fault_active = False
        fault_type = "NONE"
 
        if s.engine_temp > 115.0:
            fault_active = True
            fault_type = "ENGINE_OVERHEAT"
        elif oil_pressure < 0.5:
            fault_active = True
            fault_type = "LOW_OIL_PRESSURE"
        elif battery_voltage < 10.0:
            fault_active = True
            fault_type = "LOW_BATTERY"
        elif s.fuel_level < 8.0:
            fault_active = True
            fault_type = "LOW_FUEL"
        elif min(tyre_pressure) < 28.0:
            fault_active = True
            fault_type = "LOW_TYRE_PRESSURE"
 
        s.fault_active = fault_active
        s.fault_type = fault_type
 
        # Derived warning indicators
        check_engine = (
            s.engine_temp > 112.0
            or oil_pressure < 1.4
            or (self.random.random() < 0.002 and s.rpm > 4500.0)
        )
        low_fuel_warn = s.fuel_level < 10.0
        abs_active = (
            drive_state == "BRAKING"
            and s.speed_kmh > 35
            and self.random.random() < 0.08
        )
 
        # ------------------------------------------------------------------
        # Step 7 — Build record
        # ------------------------------------------------------------------
        record = {
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "tick":            tick,
            "rpm":             round(s.rpm, 2),
            "speed_kmh":       round(s.speed_kmh, 2),
            "engine_temp":     round(s.engine_temp, 2),
            "oil_temp":        round(s.oil_temp, 2),
            "oil_pressure":    round(oil_pressure, 3),
            "throttle":        round(throttle, 2),
            "fuel_level":      round(s.fuel_level, 3),
            "fuel_flow":       round(fuel_flow, 3),
            "battery_voltage": round(battery_voltage, 3),
            "brake_pressure":  round(brake_pressure, 3),
            "odometer_km":     round(s.odometer_km, 4),
            "trip_km":         round(s.trip_km, 4),
            "gear":            gear,
            "ignition_on":     ignition_on,
            "alternator_on":   alternator_on,
            "abs_active":      abs_active,
            "check_engine":    check_engine,
            "low_fuel_warn":   low_fuel_warn,
            "drive_state":     drive_state,
            "tyre_pressure":   tyre_pressure,
            "tyre_temp":       tyre_temp,
            "fault_active":    s.fault_active,
            "fault_type":      s.fault_type,
        }
 
        s.tick += 1
        return record
 
 
# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
 
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Car telemetry simulator")
    parser.add_argument(
        "--output-path",
        default="/data/car_stream.jsonl",
        help="Path for telemetry JSONL output (batch mode)",
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=None,
        help="Optional number of ticks; omit for infinite stream",
    )
    parser.add_argument(
        "--real-time",
        action="store_true",
        default=True,
        help="Sleep between ticks using the configured interval (default: enabled)",
    )
    parser.add_argument(
        "--no-real-time",
        dest="real_time",
        action="store_false",
        help="Emit ticks as fast as possible (batch mode only; stream mode ignores this)",
    )
    parser.add_argument(
        "--tick-interval-seconds",
        type=float,
        default=0.5,
        help="Tick duration in seconds",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for repeatable telemetry patterns",
    )
    return parser.parse_args()
 
 
def main() -> None:
    args = parse_args()
    simulator = CarTelemetrySimulator(
        output_path=args.output_path,
        real_time=args.real_time,
        ticks=args.ticks,
        tick_interval_seconds=args.tick_interval_seconds,
        seed=args.seed,
    )
    simulator.run()
 
 
if __name__ == "__main__":
    main()
 