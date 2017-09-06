"""
SSBGP-DSS Simulator

Usage:
  ssbgp-dss-simulator <install_dir> [--addr=<dispatcher>] [--port=<dispatcher>]
  ssbgp-dss-simulator (-h | --help)

Options:
  -h --help            Show this screen.
  --version            Show version.
  --addr=<dispatcher>  Dispatcher IP address or domain [default: localhost].
  --port=<dispatcher>  Dispatcher listening port [default: 32014].

"""
import logging
import os
import sys
from logging.config import fileConfig

from docopt import docopt
from pkg_resources import resource_filename, Requirement

from dss_simulator.__version__ import version
from dss_simulator.simulator import Simulator


def main():
    # The input args can be parsed before setting up the loggers because the
    # loggers are not used for it
    args = docopt(__doc__, version=version)

    # Setup the loggers
    logs_config = resource_filename(
        Requirement.parse("ssbgp-dss-simulator"), 'dss_simulator/logs.ini')
    fileConfig(logs_config)

    # Use root logger
    logger = logging.getLogger('')

    install_dir = args['<install_dir>']
    if not os.path.isdir(install_dir):
        logger.error("install directory does not exist: %s" % install_dir)
        sys.exit(1)

    address = args['--addr']
    port = int(args['--port'])

    topologies_dir = os.path.join(install_dir, "topologies")
    reports_dir = os.path.join(install_dir, "reports")
    logs_dir = os.path.join(install_dir, "logs")
    uuid_file = os.path.join(install_dir, "uuid.txt")
    jar_file = os.path.join(install_dir, "ssbgp-simulator.jar")

    os.makedirs(topologies_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    simulator = Simulator(jar_file, uuid_file, topologies_dir, reports_dir,
                          logs_dir, dispatcher_address=(address, port))

    try:
        logger.info("connecting to %s:%d" % (address, port))
        logger.info("running...")
        simulator.run_forever()

    except KeyboardInterrupt:
        print()
        logger.info("shutting down the simulator...")
        simulator.shutdown()
        logger.info("shutdown successful")


if __name__ == '__main__':
    main()
