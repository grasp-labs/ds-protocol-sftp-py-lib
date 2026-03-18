"""
**File**: `01_linked_service_connect.py`
**Region**: `examples/01_linked_service_connect`

Example 01: Connect to Sftp with Paramiko through Linked Service.
"""

import logging
from uuid import uuid4

from ds_common_logger_py_lib import Logger
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main():
    """Main function to demonstrate connecting to an SFTP server using a linked service."""
    linked_service_settings = SftpLinkedServiceSettings(
        host="",
        port=22,
        username="",
        password="",
        host_key_validation=False,
        host_key_fingerprint=None,
    )
    linked_service = SftpLinkedService(
        id=uuid4(),
        name="SFTP Linked Service",
        version="1.0.0",
        settings=linked_service_settings,
    )

    try:
        linked_service.connect()
        success, message = linked_service.test_connection()
        if success:
            logger.info("Connection successful: %s", message)
        else:
            logger.error("Connection test failed: %s", message)
    except Exception as exc:
        logger.error("An error occurred while connecting to the SFTP server: %s", exc.__dict__)
        raise


if __name__ == "__main__":
    main()
