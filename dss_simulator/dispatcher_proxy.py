import logging
import socket
from time import sleep
from xmlrpc.client import ServerProxy, Fault

from dss_simulator.simulation import Simulation

logger = logging.getLogger('simulator')


class DispatcherProxy:
    """ A proxy to access the dispatcher """

    # Amount of time to wait before re-trying to connect
    RECONNECT_PERIOD = 10  # seconds

    def __init__(self, address):
        self._address = address
        self._proxy = ServerProxy("http://%s:%d" % self._address, allow_none=True)

    def register(self) -> str:
        return self._wait_for_connection(self._proxy.register)

    def next_simulation(self, simulator_id: str) -> Simulation:
        simulation = self._wait_for_connection(self._proxy.next_simulation, simulator_id)
        return Simulation(**simulation) if simulation else None

    def notify_finished(self, simulator_id: str, simulation_id: str):
        self._wait_for_connection(self._proxy.notify_finished, simulator_id, simulation_id)

    def _wait_for_connection(self, method, *args):
        """
        Calls the specified method with the given arguments. To call the method the proxy must
        connect to the dispatcher first. If this connection fails it tries waits some period of
        time and tries to call the same method later.

        This method only returns when the method is called successfully.

        :param method: method to call
        :param args:   arguments to call the method with
        :return: the value return by the specified method
        """
        while True:
            try:
                return method(*args)

            except Fault:
                logger.error(f"error occurred at the dispatcher")

            except socket.gaierror:
                ip_address, port = self._address
                logger.warning(f"failed to translate domain name for '{ip_address}'")
                logger.info("internet connection may be down")
                logger.info("domain name may be incorrect")

            except ConnectionError:
                logger.warning("failed to connect to dispatcher")
                ip_address, port = self._address
                logger.info(f"check if the address is correct: '{ip_address}:{port}'")

            except OSError:
                logger.warning("internet connection is down")

            logger.info(f"will try to connect again in {self.RECONNECT_PERIOD} seconds")
            sleep(self.RECONNECT_PERIOD)
