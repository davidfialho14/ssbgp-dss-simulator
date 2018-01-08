import logging
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path
from subprocess import check_call, CalledProcessError
from typing import Tuple
from xmlrpc.client import Fault

from dss_simulator.dispatcher_proxy import DispatcherProxy
from dss_simulator.simulation import Simulation

logger = logging.getLogger('simulator')


class Simulator:
    """
    The simulator's main function is to execute simulations and store the data reports they
    generate.

    Simulations are obtained from a dispatcher. When a simulator is available for executing a new
    simulation it connects to the dispatcher and asks for a new simulation. The dispatcher
    responds with a set of simulation parameters, including an ID that the dispatcher uses to
    uniquely identify the simulation. After finishing a simulation the simulator notifies the
    dispatcher, indicating the ID of the finished simulation.

    When a simulation fails for some reason the simulator logs the simulation that failed and
    re-executes the simulation. It tries to include as much information as possible in the error
    log.
    """

    # Period to check for new simulations
    SIMULATION_CHECK_PERIOD = 10  # seconds

    def __init__(self, jar_file: Path, id_file: Path, topologies_dir: Path, data_dir: Path,
                 logs_dir: Path, dispatcher_address: Tuple[str, int]):
        self._jar_file = jar_file
        self._uuid_file = id_file
        self._topologies_dir = topologies_dir
        self._data_dir = data_dir
        self._logs_dir = logs_dir

        self._dispatcher_address = dispatcher_address
        self._dispatcher = DispatcherProxy(self._dispatcher_address)

        # Unique identifier assigned by the dispatcher to this simulator
        # It is kept as None while the simulator is not logged in
        self._id: str = None

        # This event is set to indicate the intention to stop the main loop
        self._to_stop = threading.Event()

    @property
    def _running_dir(self) -> Path:
        """ Directory where data is kept while a simulation is running """
        return self._data_dir / "running"

    @property
    def _complete_dir(self) -> Path:
        """ Directory where data from complete simulations are stored """
        return self._data_dir / "complete"

    @property
    def _failed_dir(self) -> Path:
        """ Directory where data from failed simulations are stored """
        return self._data_dir / "failed"

    @property
    def _simulations_log(self) -> Path:
        """ Path to file where simulations are logged """
        return self._data_dir / "simulations.log"

    def run_forever(self):
        """ Runs the simulator forever """

        if self._id is None:
            self.login()

        # Create storage structure
        self._running_dir.mkdir(exist_ok=True)
        self._complete_dir.mkdir(exist_ok=True)
        self._failed_dir.mkdir(exist_ok=True)

        # Cleanup the 'running' directory.
        # If the 'running' directory is not empty at this point, then certainly the simulator
        # was shutdown unexpectedly before the running simulation finished. Therefore, that
        # simulation was not finished and its data is incomplete.
        if not is_empty_dir(self._running_dir):
            logger.warning("reports directory is not empty: previous simulation execution must "
                           "have failed.")
            clear_directory(self._running_dir)

        while not self._to_stop.is_set():
            try:
                # Ask the dispatcher for a simulation o execute
                simulation = self._dispatcher.next_simulation(self._id)

            except Fault:
                # Faults are not expected to occur. If they do occur, then it is because there was
                # an error with the dispatcher
                logger.error("dispatcher error")
                continue

            if simulation is None:
                # The dispatcher had not simulations to be executed
                logger.warning("simulation queue is empty")
                logger.info(f"will check again in {self.SIMULATION_CHECK_PERIOD} seconds")
                self._sleep(self.SIMULATION_CHECK_PERIOD)
                continue

            self.simulate(simulation)

    def shutdown(self):
        """ Stops the run_forever loop """
        self._to_stop.set()

    def login(self):

        logger.info("logging in to dispatcher at %s:%d..." % self._dispatcher_address)

        try:
            # Read ID from local file
            with open(self._uuid_file) as file:
                self._id = file.read()

        except FileNotFoundError:
            logger.info("simulator is not registered yet")
            logger.info("registering...")

            # Obtain ID from dispatcher
            self._id = self._dispatcher.register()

            # Store the ID on disk
            with open(self._uuid_file, "w") as file:
                file.write(self._id)

            logger.info(f"registered as {self._id}")

        logger.info(f"logged in as {self._id}")

    def simulate(self, simulation: Simulation):
        """ Runs a *simulation* """

        logger.info(f"running simulation {simulation.id}...")
        logger.info(simulation)

        # Data from each simulation is stored in a sub-directory inside the complete directory.
        # The name of that sub-directory corresponds to the ID of that simulation.
        complete_dir = self._complete_dir / simulation.id

        # Directory where data from this simulation will be stored while it is running
        # There is no need for a special sub-directory for each simulation, as with the complete
        # simulations, because it can only be one running simulation
        running_dir = self._running_dir

        # It may happen that a simulation was executed until the end, but the dispatcher was not
        # notified correctly due to some unexpected error. In that case the complete directory
        # already exists. In that case, we consider the simulation to be complete and notify the
        # dispatcher
        if complete_dir.is_dir():
            logging.warning(f"{simulation.id} was already executed")
            self._dispatcher.notify_finished(self._id, simulation.id)
            return

        # Setup the arguments for the simulator
        args = [
            "java", "-jar", self._jar_file,
            "-t", str(self._topologies_dir / simulation.topology),
            "-o", running_dir,
            "-d", str(simulation.destination),
            "-c", str(simulation.repetitions),
            "-mindelay", str(simulation.min_delay),
            "-maxdelay", str(simulation.max_delay),
            "-th", str(simulation.threshold),
            "-stubs", str(self._topologies_dir / simulation.stubs_file)
        ]

        if simulation.enable_reportnodes:
            args.append("-rn")

        # Path to file where output of simulation is logged to
        log_path = self._logs_dir / (simulation.id + '.log')

        try:
            # Execute the simulation
            with open(log_path, "w") as log_file:
                check_call(args, stdout=log_file, stderr=log_file)

            logger.info("finished simulation %s" % simulation.id)

            # Move data to complete directory
            for path in running_dir.iterdir():
                path.rename(complete_dir / path.name)

            logger.info(f"data moved to '{str(complete_dir)}'")

            self._dispatcher.notify_finished(self._id, simulation.id)

        except CalledProcessError:
            logger.error(f"simulator crashed while running {simulation.id}")

            # Create 'failed' directory to contain incomplete data and log from this simulation
            error_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            failed_dir = self._failed_dir / f"{simulation.id}" / f"{error_timestamp}"
            failed_dir.mkdir(parents=True, exist_ok=True)

            # Move simulation data and logs to a failed directory
            log_path.rename(failed_dir / log_path.name)
            for path in running_dir.iterdir():
                path.rename(failed_dir / path.name)

            logger.info(f"incomplete data and log file were stored in '{failed_dir}'")

            # Wait some time for the user to see the error message
            self._sleep(2)

    def _sleep(self, timeout: float):
        self._to_stop.wait(timeout)


def clear_directory(directory: Path):
    """ Clears the contents of a *directory*. It leaves the directory empty. """
    for path in directory.iterdir():
        if path.is_dir():
            shutil.rmtree(str(path))
        else:
            os.remove(str(path))


def is_empty_dir(directory: Path) -> bool:
    """ Checks if the *directory* is empty """
    for path in directory.iterdir():
        return False
    return True
