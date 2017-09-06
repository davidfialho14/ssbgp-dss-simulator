import logging
from time import sleep
from xmlrpc.client import ServerProxy


class DispatcherProxy:
    """ A proxy to access the dispatcher """

    _CONNECTION_WAIT_PERIOD = 10  # wait 10 seconds

    _logger = logging.getLogger('simulator')

    def __init__(self, address):
        self._address = address
        self._proxy = ServerProxy("http://%s:%d" % address, allow_none=True)

    def register(self) -> str:
        return self._wait_connection(self._proxy.register)

    def _wait_connection(self, method, *args):
        """
        Calls the specified method with the given arguments. To call the
        method the proxy must connect to the dispatcher first. If this
        connection fails it tries waits some period of time and tries to call
        the same method later.

        This method only returns when the method is called successfully.

        :param method: method to call
        :param args:   arguments to call the method with
        :return: the value return by the specified method
        """
        while True:
            try:
                return method(*args)
            except ConnectionError:
                self._logger.warning("failed to connect to dispatcher")
                self._logger.info("check if the address %s:%d is correct" %
                                  self._address)
                self._logger.info("will try to connect again in %d seconds" %
                                  self._CONNECTION_WAIT_PERIOD)
                sleep(self._CONNECTION_WAIT_PERIOD)
