#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os
import pytest
import logging
import sys

# Ignore async tests for Python < 3.5
collect_ignore = []
if sys.version_info < (3, 5):
    collect_ignore.append("tests/asynctests")
    collect_ignore.append("features")
else:
    from tests.asynctests import MockEventProcessor
    from azure.eventprocessorhost import EventProcessorHost
    from azure.eventprocessorhost import EventHubPartitionPump
    from azure.eventprocessorhost import AzureStorageCheckpointLeaseManager
    from azure.eventprocessorhost import AzureBlobLease
    from azure.eventprocessorhost import EventHubConfig
    from azure.eventprocessorhost.lease import Lease
    from azure.eventprocessorhost.partition_pump import PartitionPump
    from azure.eventprocessorhost.partition_manager import PartitionManager

from tests import get_logger
from azure import eventhub
from azure.eventhub import EventHubClient, Receiver, Offset


log = get_logger(None, logging.DEBUG)

@pytest.fixture()
def live_eventhub_config():
    try:
        config = {}
        config['hostname'] = os.environ['EVENT_HUB_HOSTNAME']
        config['event_hub'] = os.environ['EVENT_HUB_NAME']
        config['key_name'] = os.environ['EVENT_HUB_SAS_POLICY']
        config['access_key'] = os.environ['EVENT_HUB_SAS_KEY']
        config['consumer_group'] = "$Default"
        config['partition'] = "0"
    except KeyError:
        pytest.skip("Live EventHub configuration not found.")
    else:
        return config


@pytest.fixture()
def connection_str():
    try:
        return os.environ['EVENT_HUB_CONNECTION_STR']
    except KeyError:
        pytest.skip("No EventHub connection string found.")


@pytest.fixture()
def invalid_hostname():
    try:
        conn_str = os.environ['EVENT_HUB_CONNECTION_STR']
        return conn_str.replace("Endpoint=sb://", "Endpoint=sb://invalid.")
    except KeyError:
        pytest.skip("No EventHub connection string found.")


@pytest.fixture()
def invalid_key():
    try:
        conn_str = os.environ['EVENT_HUB_CONNECTION_STR']
        return conn_str.replace("SharedAccessKey=", "SharedAccessKey=invalid")
    except KeyError:
        pytest.skip("No EventHub connection string found.")


@pytest.fixture()
def invalid_policy():
    try:
        conn_str = os.environ['EVENT_HUB_CONNECTION_STR']
        return conn_str.replace("SharedAccessKeyName=", "SharedAccessKeyName=invalid")
    except KeyError:
        pytest.skip("No EventHub connection string found.")


@pytest.fixture()
def iot_connection_str():
    try:
        return os.environ['IOTHUB_CONNECTION_STR']
    except KeyError:
        pytest.skip("No IotHub connection string found.")


@pytest.fixture()
def device_id():
    try:
        return os.environ['IOTHUB_DEVICE']
    except KeyError:
        pytest.skip("No Iothub device ID found.")


@pytest.fixture()
def receivers(connection_str):
    client = EventHubClient.from_connection_string(connection_str, debug=False)
    eh_hub_info = client.get_eventhub_info()
    partitions = eh_hub_info["partition_ids"]

    recv_offset = Offset("@latest")
    receivers = []
    for p in partitions:
        receivers.append(client.add_receiver("$default", p, prefetch=500, offset=Offset("@latest")))

    client.run()

    for r in receivers:
        r.receive(timeout=1)
    yield receivers

    client.stop()


@pytest.fixture()
def senders(connection_str):
    client = EventHubClient.from_connection_string(connection_str, debug=True)
    eh_hub_info = client.get_eventhub_info()
    partitions = eh_hub_info["partition_ids"]

    senders = []
    for p in partitions:
        senders.append(client.add_sender(partition=p))

    client.run()
    yield senders
    client.stop()


@pytest.fixture()
def storage_clm():
    try:
        storage_clm = AzureStorageCheckpointLeaseManager(
            os.environ['AZURE_STORAGE_ACCOUNT'],
            os.environ['AZURE_STORAGE_ACCESS_KEY'],
            "lease")
        return storage_clm
    except KeyError:
        pytest.skip("Live Storage configuration not found.")


@pytest.fixture()
def eph():
    try:
        storage_clm = AzureStorageCheckpointLeaseManager(
            os.environ['AZURE_STORAGE_ACCOUNT'],
            os.environ['AZURE_STORAGE_ACCESS_KEY'],
            "lease")
        NAMESPACE = os.environ.get('EVENT_HUB_NAMESPACE')
        EVENTHUB = os.environ.get('EVENT_HUB_NAME')
        USER = os.environ.get('EVENT_HUB_SAS_POLICY')
        KEY = os.environ.get('EVENT_HUB_SAS_KEY')

        eh_config = EventHubConfig(NAMESPACE, EVENTHUB, USER, KEY, consumer_group="$default")
        host = EventProcessorHost(
            MockEventProcessor,
            eh_config,
            storage_clm)
    except KeyError:
        pytest.skip("Live EventHub configuration not found.")
    return host


@pytest.fixture()
def eh_partition_pump(eph):
    lease = AzureBlobLease()
    lease.with_partition_id("1")
    partition_pump = EventHubPartitionPump(eph, lease)
    return partition_pump


@pytest.fixture()
def partition_pump(eph):
    lease = Lease()
    lease.with_partition_id("1")
    partition_pump = PartitionPump(eph, lease)
    return partition_pump


@pytest.fixture()
def partition_manager(eph):
    partition_manager = PartitionManager(eph)
    return partition_manager
