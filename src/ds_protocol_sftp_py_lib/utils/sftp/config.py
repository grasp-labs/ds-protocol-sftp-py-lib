"""
**File**: `config.py`
**Region**: `src/ds_protocol_sftp_py_lib/utils/sftp/config`

SFTP configuration dataclass.

Covers:
- SftpConfig dataclass definition with default values and type annotations.
"""

from dataclasses import dataclass, field

from paramiko import AutoAddPolicy, MissingHostKeyPolicy


@dataclass(kw_only=True)
class SftpConfig:
    """
    Configuration for the SFTP client.
    """

    # PKey and Host Key Validation (optional fields)
    pkey: str | None = None
    host_key_validation: bool = True

    # Policy configuration
    policy: MissingHostKeyPolicy = field(default_factory=AutoAddPolicy)

    # Timeout configuration
    timeout: float | None = None
