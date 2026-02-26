"""
**File:** ``test_config.py``
**Region:** ``tests/utils/sftp/test_config``

SFTP configuration tests.

Covers:
- RetryConfig default policy and immutability.
- SftpConfig default construction and embedded RetryConfig initialization.
"""

from paramiko import AutoAddPolicy, MissingHostKeyPolicy

from ds_protocol_sftp_py_lib.utils.sftp.config import SftpConfig


def test_sftp_config_defaults() -> None:
    """Verify default values of SftpConfig."""
    config = SftpConfig()
    assert config.pkey is None
    assert config.host_key_validation is True
    assert isinstance(config.policy, MissingHostKeyPolicy)
    assert isinstance(config.policy, AutoAddPolicy)
    assert config.timeout is None


def test_sftp_config_custom_values() -> None:
    """Verify SftpConfig can be initialized with custom values."""
    config = SftpConfig(pkey="mykey", host_key_validation=False, timeout=10.0)
    assert config.pkey == "mykey"
    assert config.host_key_validation is False
    assert config.timeout == 10.0


def test_policy_is_immutable() -> None:
    """Verify that each SftpConfig instance gets its own policy object."""
    config1 = SftpConfig()
    config2 = SftpConfig()
    assert config1.policy is not config2.policy  # Each instance gets its own policy object
    assert isinstance(config1.policy, AutoAddPolicy)
    assert isinstance(config2.policy, AutoAddPolicy)
