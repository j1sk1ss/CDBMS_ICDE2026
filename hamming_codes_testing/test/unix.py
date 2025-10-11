import subprocess

CPU_CAPACITANCE: float = 1e-9

def get_cpu_voltage() -> float:
    try:
        result = subprocess.run(
            ["vcgencmd", "measure_volts", "core"],
            capture_output=True, text=True, check=True
        )
        volt_str = result.stdout.strip().split("=")[1].replace("V", "")
        return float(volt_str)
    except Exception:
        return 1.2
    
def get_cpu_ticks() -> int:
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        parts = line.split()[1:]
        return sum(map(int, parts))
    except FileNotFoundError:
        return 0
    
def get_cpu_temp() -> float:
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            return int(f.read()) / 1000.0
    except FileNotFoundError:
        return 0.0

def get_cpu_freq() -> float:
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq") as f:
            return int(f.read().strip()) / 1e6
    except FileNotFoundError:
        return 0.0

def estimate_energy(cpu_time: float, freq_ghz: float) -> float:
    freq_hz = freq_ghz * 1e9
    return CPU_CAPACITANCE * (get_cpu_voltage() ** 2) * freq_hz * cpu_time
