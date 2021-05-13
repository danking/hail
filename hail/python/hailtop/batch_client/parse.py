from typing import Optional, Mapping, Pattern, Union
import re
import math

MEMORY_REGEXPAT: str = r'[+]?((?:[0-9]*[.])?[0-9]+)([KMGTP][i]?)?'
MEMORY_REGEX: Pattern = re.compile(MEMORY_REGEXPAT)

CPU_REGEXPAT: str = r'[+]?((?:[0-9]*[.])?[0-9]+)([m])?'
CPU_REGEX: Pattern = re.compile(CPU_REGEXPAT)

STORAGE_REGEXPAT: str = r'[+]?((?:[0-9]*[.])?[0-9]+)([KMGTP][i]?)?'
STORAGE_REGEX: Pattern = re.compile(STORAGE_REGEXPAT)


def parse_cpu_in_mcpu(cpu_string: str) -> Optional[int]:
    match = CPU_REGEX.fullmatch(cpu_string)
    if match:
        number = float(match.group(1))
        if match.group(2) == 'm':
            number /= 1000
        return int(number * 1000)
    return None


conv_factor: Mapping[str, int] = {
    'K': 1000, 'Ki': 1024,
    'M': 1000**2, 'Mi': 1024**2,
    'G': 1000**3, 'Gi': 1024**3,
    'T': 1000**4, 'Ti': 1024**4,
    'P': 1000**5, 'Pi': 1024**5
}


memory_to_worker_type = {
    'lowmem': 'highcpu',
    'standard': 'standard',
    'highmem': 'highmem',
}
worker_type_to_memory_ratio = {v: k for k, v in memory_to_worker_type.items()}
memory_ratios = memory_to_worker_type.keys()
worker_types = memory_to_worker_type.values()
worker_type_to_core_list = {
    'highcpu': [2, 4, 8, 16, 32, 64, 96],
    'standard': [1, 2, 4, 8, 16, 32, 64, 96],
    'highmem': [2, 4, 8, 16, 32, 64, 96],
}
valid_machine_types = [
    f'n1-{worker_type}-{cores}'
    for worker_type in worker_types
    for cores in worker_type_to_core_list[worker_type]
]


def parse_memory_request(x: str) -> Optional[Union[int, str]]:
    if x in memory_ratios:
        return x
    return parse_memory_in_bytes(x)


def parse_memory_in_bytes(memory_string: str) -> Optional[int]:
    match = MEMORY_REGEX.fullmatch(memory_string)
    if match:
        number = float(match.group(1))
        suffix = match.group(2)
        if suffix:
            return math.ceil(number * conv_factor[suffix])
        return math.ceil(number)
    return None


def parse_storage_in_bytes(storage_string: str) -> Optional[int]:
    match = STORAGE_REGEX.fullmatch(storage_string)
    if match:
        number = float(match.group(1))
        suffix = match.group(2)
        if suffix:
            return math.ceil(number * conv_factor[suffix])
        return math.ceil(number)
    return None
