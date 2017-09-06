import logging
import threading
from xmlrpc.client import ServerProxy


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

    logger = logging.getLogger('simulator')

    def __init__(self, uuid_file: str, topologies_dir: str,
                 reports_dir: str, dispatcher_address):
        self._uuid_file = uuid_file
        self._topologies_dir = topologies_dir
        self._reports_dir = reports_dir
        self._dispatcher = ServerProxy("http://%s:%d" % dispatcher_address,
                                       allow_none=True)
        self._uuid = None

        self._to_stop = threading.Event()

    def run_forever(self):
        """ Runs the simulator forever """
        try:
            # Read UUID from local file
            with open(self._uuid_file) as file:
                self._uuid = file.read()

        except FileNotFoundError:
            self.logger.info("simulator is not registered yet")
            self.logger.debug("registering")

            # Obtain UUID from dispatcher
            self._uuid = self._dispatcher.register()
            # Store the UUID on disk
            with open(self._uuid_file, "w") as file:
                file.write(self._uuid)

        self.logger.info("registered with UUID: %s" % self._uuid)

        while not self._to_stop.is_set():
            self._run()

    def shutdown(self):
        """ Stops the run_forever loop """
        self._to_stop.set()

    def _run(self):
        pass
