import logging
import threading

import os
from datetime import datetime
from subprocess import check_call, CalledProcessError

import shutil

from dss_simulator.dispatcher_proxy import DispatcherProxy
from dss_simulator.simulation import Simulation


class Simulator:
    """
    The simulator's main function is to execute simulations and store
    the data reports they generate.

    Simulations are obtained from a dispatcher. When a simulator is available
    for executing a new simulation it connects to the dispatcher and asks for
    a new simulation. The dispatcher responds with a set of simulation
    parameters, including an ID that the dispatcher uses to uniquely identify
    the simulation. After finishing a simulation the simulator notifies the
    dispatcher, indicating the ID of the finished simulation.

    When a simulation fails for some reason the simulator logs the simulation
    that failed and re-executes the simulation. It tries to include as much
    information as possible in the error log.
    """

    _logger = logging.getLogger('simulator')

    SIMULATION_CHECK_PERIOD = 10  # check for new simulations every 10 seconds

    def __init__(self, jar_file: str, uuid_file: str, topologies_dir: str,
                 reports_dir: str, logs_dir: str, dispatcher_address):
        self._jar_file = jar_file
        self._uuid_file = uuid_file
        self._topologies_dir = topologies_dir
        self._reports_dir = reports_dir
        self._logs_dir = logs_dir

        self._dispatcher = DispatcherProxy(dispatcher_address)
        self._uuid = None

        self._to_stop = threading.Event()

    @property
    def _reports_running_dir(self):
        """ Directory to place reports while a simulation is running """
        return os.path.join(self._reports_dir, "running")

    @property
    def _reports_complete_dir(self):
        """ Directory to place reports while a simulation finishes """
        return os.path.join(self._reports_dir, "complete")

    @property
    def _reports_failed_dir(self):
        """ Directory to place reports while a simulation fails """
        return os.path.join(self._reports_dir, "failed")

    def run_forever(self):
        """ Runs the simulator forever """
        try:
            # Read UUID from local file
            with open(self._uuid_file) as file:
                self._uuid = file.read()

        except FileNotFoundError:
            self._logger.info("simulator is not registered yet")
            self._logger.debug("registering")

            # Obtain UUID from dispatcher
            self._uuid = self._dispatcher.register()

            # Store the UUID on disk
            with open(self._uuid_file, "w") as file:
                file.write(self._uuid)

        self._logger.info("registered with UUID: %s" % self._uuid)

        # Create storage structure
        makedir(self._reports_dir)
        makedir(self._reports_running_dir)
        makedir(self._reports_complete_dir)
        makedir(self._reports_failed_dir)
        makedir(self._logs_dir)

        while not self._to_stop.is_set():
            self._run()

    def shutdown(self):
        """ Stops the run_forever loop """
        self._to_stop.set()

    def _run(self):
        # Ask the dispatcher for a simulation o execute
        simulation = self._dispatcher.next_simulation(self._uuid)

        if simulation is None:
            self._logger.warning("there are no simulations to execute")
            self._logger.info("will ask again in %d seconds" %
                              self.SIMULATION_CHECK_PERIOD)
            self._sleep(self.SIMULATION_CHECK_PERIOD)
            return

        self._logger.info("running simulation %s" % simulation.id)
        self._logger.info(simulation)

        # Path to log file for this simulation
        log_path = os.path.join(self._logs_dir, simulation.id + '.log')

        # Directory where reports files are stored while the simulation is
        # being executed
        reports_running_dir = os.path.join(self._reports_running_dir,
                                           simulation.id)

        # The reports running directory must be completely clean before each
        # execution. If this directory directory contains files from a
        # previous simulation, that simulation is considered failed and the
        # existing files are stored in the failed directory
        try:
            makedir(reports_running_dir, exist_ok=False)
        except FileExistsError:
            self._logger.warning("reports directory is not empty")
            self._logger.warning("previous execution of simulation %s must "
                                 "have failed" % simulation.id)

            self._handle_failure(simulation, reports_running_dir, log_path)

        # Directory where the report files will be stored after the
        # simulation is finished
        reports_complete_dir = os.path.join(self._reports_complete_dir,
                                            simulation.id)

        # Check if the simulation was already executed
        # We assume the simulation was already executed if the complete
        # directory of this simulation already exists
        if os.path.isdir(reports_complete_dir):
            # This may happen if the simulation finished but the dispatcher
            # was not notified
            logging.warning("%s was already executed" % simulation.id)
            self._dispatcher.notify_finished(self._uuid, simulation.id)
            return

        # Setup the arguments for the simulator
        args = [
            "java", "-jar", self._jar_file,
            "-t", os.path.join(self._topologies_dir, simulation.topology),
            "-o", reports_running_dir,
            "-d", str(simulation.destination),
            "-c", str(simulation.repetitions),
            "-mindelay", str(simulation.min_delay),
            "-maxdelay", str(simulation.max_delay),
            "-th", str(simulation.threshold),
            "-stubs", os.path.join(self._topologies_dir, simulation.stubs_file)
        ]

        try:
            # Run the simulator
            with open(log_path, "w") as log_file:
                check_call(args, stdout=log_file, stderr=log_file)

            # Move report files to the complete directory
            os.rename(src=reports_running_dir, dst=reports_complete_dir)

            self._logger.info("finished simulation %s" % simulation.id)
            self._dispatcher.notify_finished(self._uuid, simulation.id)

        except CalledProcessError:

            self._logger.error("simulator crashed while running %s" %
                               simulation.id)

            self._handle_failure(simulation, reports_running_dir, log_path)

            # Wait some time for the user to see the error message
            self._sleep(2)
            print()

    def _handle_failure(self, simulation: Simulation, reports_dir: str,
                        log_path: str):
        """
        Handles a potential failure.

        It takes care of storing the report files in the appropriate directory
        and to rename the log file to an appropriate name to ensure it is not
        overridden.

        :param simulation:      simulation that failed to execute
        :param reports_dir:     directory containing the report files
        :param log_path:        path to log file used for the simulation
                                the cause of the failure
        """
        # Current timestamp
        timestamp = datetime.now().strftime("%y-%m-%d-%H-%M-%S")

        # Rename the log file to include the timestamp
        log_backup = os.path.join(self._logs_dir, '%s-%s.log' %
                                  (simulation.id, timestamp))
        os.rename(src=log_path, dst=log_backup)

        self._logger.info("log file was placed at `%s`" % log_backup)

        # Move report file to the failed directory
        reports_failed_dir = os.path.join(self._reports_failed_dir,
                                          simulation.id)
        makedir(reports_failed_dir)

        # A simulation may fail multiple times
        # The report files from each failure are stored in a
        # sub-directory with the name corresponding to the timestamp at
        # which the failure occurred
        reports_failed_dir = os.path.join(reports_failed_dir, timestamp)
        for filename in os.listdir(reports_dir):
            os.rename(
                src=os.path.join(reports_dir, filename),
                dst=os.path.join(reports_failed_dir, filename)
            )

        self._logger.info("report files were saved to %s" % reports_failed_dir)

    def _sleep(self, timeout: float):
        self._to_stop.wait(timeout)


def makedir(directory, exist_ok=True):
    """
    Create a directory named *directory*.

    It works as the standard os.mkdir with the slight difference that if
    *exist_ok* is True then it only creates the directory if the directory
    does not already exist.

    :param directory: path to the directory to create
    :param exist_ok:  if True an error is not raised if the directory
                      already exists.
    """
    if exist_ok:
        if not os.path.isdir(directory):
            os.mkdir(directory)
    else:
        os.mkdir(directory)
