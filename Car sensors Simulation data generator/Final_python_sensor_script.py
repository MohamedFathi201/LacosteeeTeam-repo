import time
from enum import Enum
from dataclasses import dataclass , field , asdict
import math
import random 
import sys
import os
import json
from datetime import datetime, timezone
from pathlib import Path
import threading
 

# Driving State
class DriveState(Enum):
    IDLE = "IDLE"
    ACCELERATING = "ACCELERATING"
    CRUISING = "CRUISING"
    BRAKING = "BRAKING"
    STOPPED = "STOPPED"

# Gear Box
GEAR_RATIOS   = {0: 0, 1: 3.5, 2: 2.1, 3: 1.4, 4: 1.0, 5: 0.75, 6: 0.60}
UPSHIFT_RPM   = 3800
DOWNSHIFT_RPM = 1400
FINAL_DRIVE   = 3.9
WHEEL_CIRC_M  = 1.96

#Car State 
@dataclass
class CarState:
   #Engine
    rpm: float             = 800.0
    engine_temp: float     = 20.0
    oil_temp: float        = 20.0
    oil_pressure: float    = 0.3
    throttle: float        = 0.0
    ignition_on: bool      = True
    #Drive
    speed_kmh: float       = 0.0
    gear: int              = 0
    #Fuel & Battery
    fuel_level: float      = 85.0
    fuel_flow: float       = 0.4
    battery_voltage: float = 12.6
    alternator_on: bool    = False
    #Tyres
    tyre_pressure: list    = field(default_factory=lambda: [2.3, 2.3, 2.2, 2.2])  
    tyre_temp: list        = field(default_factory=lambda: [20.0, 20.0, 20.0, 20.0])
    #Brakes
    brake_pressure: float  = 0.0      
    abs_active: bool       = False
    #Non-Related
    odometer_km: float      = 14820.0
    trip_km: float          = 0.0
    drive_state: DriveState = DriveState.IDLE
    state_timer: float      = 0.0
    _accel: float           = 0.0
    _state_duration: float  = 0.0
    # Warnings
    check_engine: bool      = False
    low_fuel_warn: bool     = False


class CarSimulator:
    def __init__(self, dt: float = 0.5): #self:object , dt: delta time
        self.s      = CarState()
        self.dt     = dt           
        self.tick   = 0
        self._plan_next_state()
   
    def _plan_next_state(self):
        self.s._state_duration = random.uniform(4, 18)  
        self.s.state_timer     = 0    

    # Driver State
    def _transition_state(self):
        cur = self.s.drive_state
        if cur == DriveState.STOPPED:
            self.s.drive_state = DriveState.ACCELERATING   
        elif cur == DriveState.ACCELERATING:
            self.s.drive_state = random.choice([DriveState.CRUISING, DriveState.CRUISING, DriveState.BRAKING])
        elif cur == DriveState.CRUISING:
            self.s.drive_state = random.choice([DriveState.ACCELERATING, DriveState.BRAKING, DriveState.CRUISING])
        elif cur == DriveState.BRAKING:
            if self.s.speed_kmh < 5:
                self.s.drive_state = DriveState.STOPPED
            else:
                self.s.drive_state = random.choice([DriveState.CRUISING, DriveState.ACCELERATING])
        elif cur == DriveState.IDLE:
            self.s.drive_state = DriveState.ACCELERATING
        self._plan_next_state() 

    # Rpm base Calculation
    def _target_rpm_for_speed(self) -> float:
        if self.s.gear == 0 or self.s.speed_kmh < 1:
            return 800.0
        speed_ms  = self.s.speed_kmh / 3.6
        wheel_rps = speed_ms / WHEEL_CIRC_M
        shaft_rps = wheel_rps * FINAL_DRIVE * GEAR_RATIOS[self.s.gear]
        return shaft_rps * 60
    
    #Gear Shifting
    def _shift_gear(self):
        s = self.s
        if s.speed_kmh < 5:
            s.gear = 1
            return
        rpm_est = self._target_rpm_for_speed()
        if rpm_est > UPSHIFT_RPM and s.gear < 6:
            s.gear += 1
        elif rpm_est < DOWNSHIFT_RPM and s.gear > 1:
            s.gear -= 1

    # Throttle and Brake 
    def _update_throttle_brake(self):
        ds = self.s.drive_state
        if ds == DriveState.ACCELERATING:
            self.s.throttle    = min(100, self.s.throttle + random.uniform(3, 8))
            self.s.brake_pressure = 0
        elif ds == DriveState.CRUISING:
            self.s.throttle    = max(10, min(55, self.s.throttle + random.uniform(-3, 3)))
            self.s.brake_pressure = 0
        elif ds == DriveState.BRAKING:
            self.s.throttle    = max(0, self.s.throttle - random.uniform(8, 15))
            self.s.brake_pressure = min(100, self.s.brake_pressure + random.uniform(5, 20))
        elif ds == DriveState.STOPPED:
             self.s.throttle    = 0
             self.s.brake_pressure = 80      


    #Speed Calculation
    def _update_speed(self):
        ds = self.s.drive_state
        if ds == DriveState.ACCELERATING:
            max_spd = random.uniform(60, 130)
            if self.s.speed_kmh < max_spd:
                self.s.speed_kmh += (self.s.throttle / 100) * random.uniform(1.5, 4.0)
        elif ds == DriveState.CRUISING:
            self.s.speed_kmh += random.uniform(-1.5, 1.5)
            self.s.speed_kmh  = max(20, self.s.speed_kmh)
        elif ds == DriveState.BRAKING:
            decel = random.uniform(3, 9)
            self.s.speed_kmh  = max(0, self.s.speed_kmh - decel)
        elif ds == DriveState.STOPPED:
            self.s.speed_kmh  = 0

        self.s.abs_active = (self.s.brake_pressure > 70 and self.s.speed_kmh > 15)



    #Rpm Updates Calculation
    def _update_rpm(self):
        s = self.s
        self._shift_gear()
        target = self._target_rpm_for_speed()

        if s.speed_kmh < 2:
            target = 800 + (s.throttle / 100) * 1200
        else:
            target += (s.throttle / 100) * 800

        lag = 0.25 
        s.rpm = s.rpm * (1 - lag) + target * lag  # new_rpm = 75%*old_rpm + 25%*target_rpm
        s.rpm = max(700, min(7200, s.rpm + random.uniform(-30, 30)))  # noise          

    
    #TEMPS (Engine, Oil, Tyres)
    def _update_temps(self):    
        s = self.s
        
        target_eng = 90 + (s.rpm / 1000) * 3  # coolant keep engine temp around 90
        rate       = 0.008 if s.engine_temp < target_eng else 0.005  # heat up faster than cool down
        s.engine_temp += (target_eng - s.engine_temp) * rate + random.uniform(-0.1, 0.1) #noise
        s.engine_temp  = max(20, min(120, s.engine_temp)) 

        s.oil_temp += (s.engine_temp - s.oil_temp) * 0.004 + random.uniform(-0.05, 0.05)  #oil temp follows engine temp but slower

        for i in range(4):
            heat = (s.speed_kmh / 200) * 0.6 + (s.brake_pressure / 100) * 0.4
            cool = 0.002
            s.tyre_temp[i] += heat * random.uniform(0.5, 1.5) - s.tyre_temp[i] * cool
            s.tyre_temp[i]  = max(15, min(110, s.tyre_temp[i]))

    
    #Oil Pressure 
    def _update_oil_pressure(self):
        s = self.s
        target = 0.3 + (s.rpm / 1000) * 0.55 + (s.oil_temp / 100) * -0.1
        s.oil_pressure += (target - s.oil_pressure) * 0.15 + random.uniform(-0.02, 0.02)
        s.oil_pressure  = max(0.2, min(5.5, s.oil_pressure))        

   
    #Fuel and Battery
    def _update_fuel_and_battery(self):
        s = self.s
    
        s.fuel_flow   = 0.4 + (s.throttle / 100) * 11.6 + (s.rpm / 6000) * 2  # Fuel consumption: idle ~0.4 L/h, highway ~8–12 L/h
        consumption   = (s.fuel_flow / 3600) * self.dt   
        tank_litres   = 55  
        s.fuel_level  = max(0 ,s.fuel_level - (consumption / tank_litres) * 100)
        s.low_fuel_warn = s.fuel_level < 12

        # Battery: 12.4V off, 14.4V alternator on
        s.alternator_on = s.rpm > 900
        target_v = 14.2 + random.uniform(-0.1, 0.1) if s.alternator_on else 12.3
        s.battery_voltage += (target_v - s.battery_voltage) * 0.05
        s.battery_voltage  = max(11.5, min(14.8, s.battery_voltage))
   
    
    #Odometer update
    def _update_odometer(self):
        s = self.s
        dist = (s.speed_kmh / 3600) * self.dt
        s.odometer_km += dist
        s.trip_km     += dist

    #tyre pressure
    def _update_tyre_pressure(self):
        s = self.s
        for i in range(4):
            delta = (s.tyre_temp[i] - 20) * 0.004 + random.uniform(-0.003, 0.003)
            s.tyre_pressure[i] = max(1.6, min(3.2, s.tyre_pressure[i] + delta))

        if random.random() < 0.0005: # Random check-engine (very rare, clears itself)
            s.check_engine = True
        if random.random() < 0.002:
            s.check_engine = False    

    
    def step(self) -> dict:
        s = self.s
        s.state_timer += self.dt
        if s.state_timer >= s._state_duration:
            self._transition_state()
 
        self._update_throttle_brake()
        self._update_speed()
        self._update_rpm()
        self._update_temps()
        self._update_oil_pressure()
        self._update_fuel_and_battery()
        self._update_odometer()
        self._update_tyre_pressure()
        self.tick += 1
 
        
        raw = asdict(s)                                
        raw["drive_state"]  = s.drive_state.value      
        raw["tick"]         = self.tick
        raw["timestamp"]    = datetime.now(timezone.utc).isoformat()  
 
        # Drop data which is not sensors
        raw.pop("_accel", None)
        raw.pop("_state_duration", None)
        raw.pop("state_timer", None)
 
        return raw
 
def listen_for_events(sim):
    while True:
        cmd = input()
        if cmd.strip().lower() == "accident":
            sim.inject_accident()


def stream_to_jsonl(
        output_path: str  = "car_stream.jsonl",
        ticks: int | None = None,
        dt: float         = 0.5,
        real_time: bool   = True,
        flush_every: int  = 10,
    ) -> None:
        sim  = CarSimulator(dt=dt)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
    
        print(f"Streaming to {path.resolve()}", flush=True)
        print(f"dt={dt}s | real_time={real_time} | ticks={'infinite' if ticks is None else ticks}\n", flush=True)
    
        written = 0
        try:
            with open(path, "a", encoding="utf-8") as fh:
                while ticks is None or written < ticks:
                    record = sim.step()
    
                    fh.write(json.dumps(record, default=str))
                    fh.write("\n")
    
                    written += 1
    
                    if written % flush_every == 0:
                        fh.flush()
                        os.fsync(fh.fileno())
    
                    if written % 20 == 0:
                        print(
                            f"tick {record['tick']:>6} | "
                            f"{record['drive_state']:<12} | "
                            f"spd {record['speed_kmh']:5.1f} km/h | "
                            f"rpm {record['rpm']:5.0f} | "
                            f"fuel {record['fuel_level']:4.1f}%"
                        )
    
                    if real_time:
                        time.sleep(dt)
    
        except KeyboardInterrupt:
            print("\nStopped by user.")    
    
        print(f"{written} records written to {path.resolve()}")
if __name__ == "__main__":
        try:
            stream_to_jsonl(
                output_path = "car_stream.jsonl",
                ticks       = None,
                dt          = 0.5,
                real_time   = True,
                flush_every = 1,
        )
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()
            input("Press Enter to exit...")        
    