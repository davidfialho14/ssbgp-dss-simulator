import uuid
from collections import namedtuple

Simulation = namedtuple("Simulation",
                        "id report_path topology destination repetitions "
                        "min_delay max_delay threshold stubs_file seed")


def simulation_with(report_path: str, topology: str, destination: int,
                    repetitions: int, min_delay: int, max_delay: int,
                    threshold: int, stubs_file: str, seed: int,
                    id: str = uuid.uuid4()):

    return Simulation(id, report_path, topology, destination, repetitions,
                      min_delay, max_delay, threshold, stubs_file, seed)
