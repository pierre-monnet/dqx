import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.installer.install import WorkspaceInstallation

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.setLevel("INFO")
    ws = WorkspaceClient(product="dqx", product_version=__version__)
    installer = WorkspaceInstallation.current(ws)
    installer.uninstall()
