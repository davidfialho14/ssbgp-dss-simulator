import logging
import threading

import os
from datetime import datetime
from subprocess import check_call, CalledProcessError

import shutil

from dss_simulator.dispatcher_proxy import DispatcherProxy


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
    information as possible in the error log. The simulation is only
    re-executed once. If the simulation fails a second time, then the
    simulator asks the dispatcher to mark the simulation as failed and asks
    for a new simulation.
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

        while not self._to_stop.is_set():
            self._run()

    def shutdown(self):
        """ Stops the run_forever loop """
        self._to_stop.set()

    def _run(self):
        simulation = self._dispatcher.next_simulation(self._uuid)

        if simulation is None:
            self._logger.warning("there are no simulations to execute")
            self._logger.info("will ask again in %d seconds" %
                              self.SIMULATION_CHECK_PERIOD)
            self._sleep(self.SIMULATION_CHECK_PERIOD)
            return

        self._logger.info("running simulation %s" % simulation.id)
        self._logger.info(simulation)

        report_dir = os.path.join(self._reports_dir, simulation.id)
        os.makedirs(report_dir, exist_ok=True)
        log_path = os.path.join(self._logs_dir, simulation.id + '.log')

        args = [
            "java", "-jar", self._jar_file,
            "-t", os.path.join(self._topologies_dir, simulation.topology),
            "-o", report_dir,
            "-d", str(simulation.destination),
            "-c", str(simulation.repetitions),
            "-mindelay", str(simulation.min_delay),
            "-maxdelay", str(simulation.max_delay),
            "-th", str(simulation.threshold),
            "-stubs", os.path.join(self._topologies_dir, simulation.stubs_file)
        ]

        try:
            # Run the simulator to execute the simulation
            with open(log_path, "w") as log_file:
                check_call(args, stdout=log_file, stderr=log_file)

            self._logger.info("finished simulation %s" % simulation.id)
            self._dispatcher.notify_finished(self._uuid, simulation.id)

        except CalledProcessError:
            self._logger.error("simulator crashed while running %s" %
                               simulation.id)

            datetime_now = datetime.now().strftime("%y-%m-%d-%H-%M-%S")

            # Rename the log file to include the current datetime in the name
            log_backup = os.path.join(self._logs_dir, '%s-%s.log' %
                                      (simulation.id, datetime_now))
            os.rename(src=log_path, dst=log_backup)

            self._logger.info("see more details in `%s`" % log_backup)

            # Move reports to an error directory inside the simulation's
            # report directory
            error_folder = "Fail-" + datetime_now
            error_dir = os.path.join(report_dir, error_folder)
            os.makedirs(error_dir)
            for filename in os.listdir(report_dir):
                # Ignore the error folders
                if not filename.startswith("Fail-"):
                    shutil.move(
                        src=os.path.join(report_dir, filename),
                        dst=os.path.join(error_dir, filename)
                    )
            self._logger.info("generated reports were saved to: %s" % error_dir)

            # Wait some time for the user to see
            self._sleep(2)
            print()

    def _sleep(self, timeout: float):
        self._to_stop.wait(timeout)
