"""
**File**: `01_linked_service_connect.py`
**Region**: `examples/01_linked_service_connect`

Example 01: Connect to Sftp with Paramiko through Linked Service.
"""
from uuid import uuid4

from ds_common_logger_py_lib import Logger
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings

logger = Logger.get_logger(__name__, package=True)

def main():
    """Main function to demonstrate connecting to an SFTP server using a linked service."""
    linked_service_settings = SftpLinkedServiceSettings(
        host="sftp.example.com",
        port=22,
        username="your_username",
        password="your_password",
        host_key_validation=True,
        host_key_fingerprint="your_host_key_fingerprint",
    )
    linked_service = SftpLinkedService(
        id=str(uuid4()),
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
    except Exception as e:
        logger.error("An error occurred while connecting to the SFTP server: %s", e)
        raise


if __name__ == "__main__":
    main()
