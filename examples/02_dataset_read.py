"""
**File**: `02_dataset_read.py`
**Region**: `examples/02_dataset_read`

Example 02: Read data from Sftp dataset.
"""

import logging
from uuid import uuid4

from ds_common_logger_py_lib import Logger
from ds_protocol_sftp_py_lib.dataset.sftp import SftpDataset, SftpDatasetSettings
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main():
    """Main function to demonstrate reading data from an SFTP dataset."""
    dataset = SftpDataset(
        id=uuid4(),
        name="SFTP Dataset",
        version="1.0.0",
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
        linked_service=SftpLinkedService(
            id=uuid4(),
            name="SFTP Linked Service",
            version="1.0.0",
            settings=SftpLinkedServiceSettings(
                host="",
                port=22,
                username="",
                password="",
                host_key_validation=True,
                host_key_fingerprint="",
            ),
        ),
        settings=SftpDatasetSettings(
            folder_path="test-folder",
            file_name="test.csv",
        ),
    )

    dataset.linked_service.connect()
    dataset.read()
    logger.info("Read dataset at %s/%s", dataset.settings.folder_path, dataset.settings.file_name)
    logger.info("Output:\n%s", dataset.output)


if __name__ == "__main__":
    main()
