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
from pathlib import Path

from docopt import docopt
from pkg_resources import resource_filename

from dss_simulator.__version__ import version
from dss_simulator.simulator import Simulator

logger = logging.getLogger('')


def main():
    args = docopt(__doc__, version=version)

    # Setup the loggers
    logs_config = resource_filename(__name__, 'logs.ini')
    fileConfig(logs_config)

    install_dir = Path(args['<install_dir>'])
    if not install_dir.is_dir():
        logger.error(f"install directory does not exist: {str(install_dir)}")
        sys.exit(1)

    address = args['--addr'], int(args['--port'])

    jar_file = install_dir / "ssbgp-simulator.jar"
    topologies_dir = install_dir / "topologies"
    reports_dir = install_dir / "reports"
    logs_dir = install_dir / "logs"
    uuid_file = install_dir / "uuid.txt"

    # Create install structure
    topologies_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Check if the routing simulator is installed
    if not jar_file.exists():
        logger.error("routing simulator is not installed yet")
        logger.info(f"expected to find '{jar_file.name}' inside the install directory")
        sys.exit(2)

    simulator = Simulator(jar_file, uuid_file, topologies_dir, reports_dir, logs_dir, address)

    try:
        logger.info("simulator running...")
        simulator.run_forever()

    except KeyboardInterrupt:
        logger.info("shutting down the simulator...")
        simulator.shutdown()
        logger.info("shutdown successful")


if __name__ == '__main__':
    main()
