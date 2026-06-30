#!/usr/bin/env python3
"""
Continuously generate car telemetry records and append them as JSONL.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import os
from azure.eventhub import EventHubProducerClient, EventData




@dataclass
class SimulatorState:
    tick: int = 0
    speed_kmh: float = 0.0
    rpm: float = 850.0
    engine_temp: float = 72.0
    oil_temp: float = 65.0
    fuel_level: float = 54.0
    odometer_km: float = 0.0
    trip_km: float = 0.0
    fault_active: bool = False
    fault_type: str | None = None
    fault_ticks_remaining: int = 0


class CarTelemetrySimulator:
    def __init__(
        self,
        output_path: str,
        real_time: bool = True,
        ticks: int | None = None,
        tick_interval_seconds: float = 0.5,
        seed: int = 42,
    ) -> None:
        self.output_path = Path(output_path)
        self.real_time = real_time
        self.ticks = ticks
        self.tick_interval_seconds = tick_interval_seconds
        self.random = random.Random(seed)
        self.state = SimulatorState()
        
        self.available_faults = {
            "ENGINE_OVERHEAT": (40, 80),
            "LOW_OIL_PRESSURE": (20, 50),
            "LOW_BATTERY": (60, 120),
            "FUEL_LEAK": (50, 100),
            "TYRE_PRESSURE_LOW": (80, 150),
        }

        self.producer = None

    def run(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.touch(exist_ok=True)
        
        emitted = 0       
        batch_size = 10000
        while self.ticks is None or emitted < self.ticks:
            
            written=0
            # Open the file only long enough to write one record
            with self.output_path.open("a", encoding="utf-8") as handle:
                 
                 for _ in range(batch_size):

                    if self.ticks is not None and emitted >= self.ticks:
                       break

                    record = self._next_record()
                    handle.write(json.dumps(record) + "\n")

                    emitted += 1
                    written += 1
            print(f"wrote{written} records")

            print("Waiting for Airflow to consume the file...")

            start = time.time()

            while self.output_path.stat().st_size > 0:

                if time.time() - start > 120:
                    print("Airflow did not consume the file within 2 minutes. Continuing...")
                    break

                time.sleep(2)

            print("Resuming simulation...")



    def _next_record(self) -> dict[str, Any]:
        dt_seconds = self.tick_interval_seconds
        self._start_random_fault()
        self._update_fault()
        tick = self.state.tick
        cycle = tick % 240

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

        speed_delta = target_speed - self.state.speed_kmh
        self.state.speed_kmh = max(
            0.0, self.state.speed_kmh + speed_delta * 0.35 + self.random.uniform(-0.6, 0.6)
        )
        throttle = min(100.0, max(0.0, 16.0 + speed_delta * 1.8 + self.random.uniform(-5.0, 5.0)))

        if self.state.speed_kmh < 1.0 and drive_state in {"IDLE", "STOPPED"}:
            gear = 0
        elif self.state.speed_kmh < 20:
            gear = 1
        elif self.state.speed_kmh < 40:
            gear = 2
        elif self.state.speed_kmh < 60:
            gear = 3
        elif self.state.speed_kmh < 85:
            gear = 4
        elif self.state.speed_kmh < 115:
            gear = 5
        else:
            gear = 6

        target_rpm = 820.0 + self.state.speed_kmh * (35.0 - min(gear, 5) * 3.0) + throttle * 10.0
        self.state.rpm = max(700.0, target_rpm + self.random.uniform(-120.0, 120.0))

        self.state.engine_temp = min(
            120.0,
            self.state.engine_temp
            + 0.03 * (90.0 - self.state.engine_temp)
            + 0.018 * (self.state.rpm / 1000.0)
            + self.random.uniform(-0.08, 0.08),
        )
        self.state.oil_temp = min(
            128.0,
            self.state.oil_temp
            + 0.025 * (self.state.engine_temp - self.state.oil_temp)
            + self.random.uniform(-0.06, 0.06),
        )

        oil_pressure = max(1.0, 1.1 + self.state.rpm / 1200.0 + self.random.uniform(-0.2, 0.2))
        fuel_flow = max(0.4, 1.1 + throttle * 0.17 + self.state.rpm / 1800.0)
        fuel_consumed_l = fuel_flow * (dt_seconds / 3600.0)
        self.state.fuel_level = max(0.0, self.state.fuel_level - fuel_consumed_l)
        self.state.odometer_km += self.state.speed_kmh * (dt_seconds / 3600.0)
        self.state.trip_km += self.state.speed_kmh * (dt_seconds / 3600.0)

        ignition_on = True
        alternator_on = self.state.rpm > 900.0
        base_battery = 13.8 if alternator_on else 12.3
        battery_voltage = max(10.8, base_battery + self.random.uniform(-0.25, 0.15))
        brake_pressure = (
            max(0.0, 25.0 + self.random.uniform(-4.0, 6.0))
            if drive_state == "BRAKING"
            else max(0.0, self.random.uniform(0.0, 3.0))
        )

        tyre_pressure_base = 32.5 + self.random.uniform(-0.3, 0.3)
        tyre_pressure = [
            round(max(28.0, tyre_pressure_base + self.random.uniform(-0.8, 0.8)), 2) for _ in range(4)
        ]
        tyre_temp_base = 28.0 + self.state.speed_kmh * 0.42 + (self.state.engine_temp - 70.0) * 0.2
        tyre_temp = [round(max(20.0, tyre_temp_base + self.random.uniform(-2.2, 2.2)), 2) for _ in range(4)]

        abs_active = drive_state == "BRAKING" and self.state.speed_kmh > 35 and self.random.random() < 0.08
        check_engine = (
            self.state.engine_temp > 112.0
            or oil_pressure < 1.4
            or (self.random.random() < 0.002 and self.state.rpm > 4500.0)
        )
        low_fuel_warn = self.state.fuel_level < 10.0

        if self.state.fault_active:

            if self.state.fault_type == "ENGINE_OVERHEAT":
                self.state.engine_temp += self.random.uniform(15, 25)
                self.state.oil_temp += self.random.uniform(10, 20)
                oil_pressure = max(0.5, oil_pressure - self.random.uniform(1.0, 1.5))
                check_engine = True

            elif self.state.fault_type == "LOW_OIL_PRESSURE":
                oil_pressure = max(0.3, oil_pressure - self.random.uniform(1.5, 2.5))
                check_engine = True

            elif self.state.fault_type == "LOW_BATTERY":
                battery_voltage = max(9.5, battery_voltage - self.random.uniform(2.0, 3.0))
                alternator_on = False
                check_engine = True

            elif self.state.fault_type == "FUEL_LEAK":
                self.state.fuel_level = max(0, self.state.fuel_level - 0.5)
                fuel_flow *= 2

            elif self.state.fault_type == "TYRE_PRESSURE_LOW":
                tyre_pressure[0] = max(20, tyre_pressure[0] - 5)


        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tick": tick,
            "rpm": round(self.state.rpm, 2),
            "speed_kmh": round(self.state.speed_kmh, 2),
            "engine_temp": round(self.state.engine_temp, 2),
            "oil_temp": round(self.state.oil_temp, 2),
            "oil_pressure": round(oil_pressure, 3),
            "throttle": round(throttle, 2),
            "fuel_level": round(self.state.fuel_level, 3),
            "fuel_flow": round(fuel_flow, 3),
            "battery_voltage": round(battery_voltage, 3),
            "brake_pressure": round(brake_pressure, 3),
            "odometer_km": round(self.state.odometer_km, 6),
            "trip_km": round(self.state.trip_km, 6),
            "gear": gear,
            "ignition_on": ignition_on,
            "alternator_on": alternator_on,
            "abs_active": abs_active,
            "check_engine": check_engine,
            "low_fuel_warn": low_fuel_warn,
            "drive_state": drive_state,
            "tyre_pressure": tyre_pressure,
            "tyre_temp": tyre_temp,
            "fault_active": self.state.fault_active,
            "fault_type": self.state.fault_type,
        }

        self.state.tick += 1
        return record

    def _start_random_fault(self):
        if self.state.fault_active:
            return

        if self.random.random() < 0.005:      # 0.5% chance each tick
            self.state.fault_active = True
            self.state.fault_type = self.random.choice(list(self.available_faults.keys()))
            min_ticks, max_ticks = self.available_faults[self.state.fault_type]
            self.state.fault_ticks_remaining = self.random.randint(min_ticks, max_ticks)


    def _update_fault(self):
        if not self.state.fault_active:
            return

        self.state.fault_ticks_remaining -= 1

        if self.state.fault_ticks_remaining <= 0:
            self.state.fault_active = False
            self.state.fault_type = None
            self.state.fault_ticks_remaining = 0



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Car telemetry simulator")
    parser.add_argument("--output-path", default="/data/car_stream.jsonl", help="Path for telemetry JSONL output")
    parser.add_argument("--ticks", type=int, default=None, help="Optional number of ticks; omit for infinite stream")
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
        help="Disable real-time sleeping and emit ticks as fast as possible",
    )
    parser.add_argument("--tick-interval-seconds", type=float, default=0.5, help="Tick duration in seconds")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for repeatable telemetry patterns")
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
