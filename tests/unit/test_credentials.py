"""Unit tests for credentials."""

import os
from unittest.mock import patch

import pytest

from award_archive.storage.credentials import get_storage_options


class TestGetStorageOptions:
    """Tests for get_storage_options."""

    @patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "key_id",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-west-2",
    }, clear=True)
    def test_with_access_key_id(self):
        """Uses AWS_ACCESS_KEY_ID."""
        opts = get_storage_options()

        assert opts["AWS_ACCESS_KEY_ID"] == "key_id"
        assert opts["AWS_SECRET_ACCESS_KEY"] == "secret"
        assert opts["AWS_REGION"] == "us-west-2"

    @patch.dict(os.environ, {
        "AWS_ACCESS_KEY": "access_key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "eu-west-1",
    }, clear=True)
    def test_with_access_key(self):
        """Uses AWS_ACCESS_KEY as fallback."""
        opts = get_storage_options()

        assert opts["AWS_ACCESS_KEY_ID"] == "access_key"
        assert opts["AWS_SECRET_ACCESS_KEY"] == "secret"

    @patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "key_id",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }, clear=True)
    def test_default_region(self):
        """Uses default region."""
        opts = get_storage_options()
        assert opts["AWS_REGION"] == "us-east-1"

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_raises(self):
        """Raises on missing credentials."""
        with pytest.raises(KeyError):
            get_storage_options()
