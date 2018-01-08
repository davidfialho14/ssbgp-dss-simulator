import uuid
from typing import NamedTuple


class Simulation(NamedTuple):
    report_path: str
    topology: str
    destination: int
    repetitions: int
    min_delay: int
    max_delay: int
    threshold: int
    stubs_file: str
    seed: int
    enable_reportnodes: bool = False
    id: str = str(uuid.uuid4())
