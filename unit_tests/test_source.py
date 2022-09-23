#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_robin_odata.source import SourceRobinOdata, RobinAuthenticator
import json

config = {
    "username": "username",
    "password": "password",
    "start_date": "2022-01-01",
}


def test_check_connection(mocker):
    source = SourceRobinOdata()
    RobinAuthenticator.login = MagicMock()
    logger_mock = MagicMock()
    assert source.check_connection(logger_mock, config) == (True, None)


with open('secrets/config.json') as f:
    config = json.load(f)


def test_streams(mocker):
    source = SourceRobinOdata()
    streams = source.streams(config)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
