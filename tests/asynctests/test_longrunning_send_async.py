#!/usr/bin/env python

"""
send test
"""

import logging
import argparse
import time
import threading
import os
import asyncio

from azure.eventhub import EventHubClientAsync, EventData

import sys
import logging
from logging.handlers import RotatingFileHandler


def get_logger(filename, level=logging.INFO):
    azure_logger = logging.getLogger("azure")
    azure_logger.setLevel(level)
    uamqp_logger = logging.getLogger("uamqp")
    uamqp_logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    if filename:
        file_handler = RotatingFileHandler(filename, maxBytes=20*1024*1024, backupCount=3)
        file_handler.setFormatter(formatter)
        azure_logger.addHandler(file_handler)
        uamqp_logger.addHandler(file_handler)

    return azure_logger

logger = get_logger("send_test_async.log", logging.INFO)


def check_send_successful(outcome, condition):
    if outcome.value != 0:
        print("Send failed {}".format(condition))


async def get_partitions(args):
    eh_data = await args.get_eventhub_info_async()
    return eh_data["partition_ids"]


async def pump(pid, sender, args, duration):
    deadline = time.time() + duration
    total = 0

    def data_generator():
        for i in range(args.batch):
            yield b"D" * args.payload

    if args.batch > 1:
        logger.info("{}: Sending batched messages".format(pid))
    else:
        logger.info("{}: Sending single messages".format(pid))

    try:
        while time.time() < deadline:
            if args.batch > 1:
                data = EventData(batch=data_generator())
            else:
                data = EventData(body=b"D" * args.payload)
            sender.transfer(data, callback=check_send_successful)
            total += args.batch
            if total % 10 == 0:
               await sender.wait_async()
               logger.info("{}: Send total {}".format(pid, total))
    except Exception as err:
        logger.info("{}: Send failed {}".format(pid, err))
    logger.info("{}: Sent total {}".format(pid, total))


def test_long_running_partition_send_async():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", help="Duration in seconds of the test", type=int, default=30)
    parser.add_argument("--payload", help="payload size", type=int, default=512)
    parser.add_argument("--batch", help="Number of events to send and wait", type=int, default=1)
    parser.add_argument("--partitions", help="Comma seperated partition IDs")
    parser.add_argument("--conn-str", help="EventHub connection string", default=os.environ.get('EVENT_HUB_CONNECTION_STR'))
    parser.add_argument("--eventhub", help="Name of EventHub")
    parser.add_argument("--address", help="Address URI to the EventHub entity")
    parser.add_argument("--sas-policy", help="Name of the shared access policy to authenticate with")
    parser.add_argument("--sas-key", help="Shared access key")

    loop = asyncio.get_event_loop()
    args, _ = parser.parse_known_args()
    if args.conn_str:
        client = EventHubClientAsync.from_connection_string(
            args.conn_str,
            eventhub=args.eventhub, debug=True)
    elif args.address:
        client = EventHubClientAsync(
            args.address,
            username=args.sas_policy,
            password=args.sas_key,
            auth_timeout=500)
    else:
        try:
            import pytest
            pytest.skip("Must specify either '--conn-str' or '--address'")
        except ImportError:
            raise ValueError("Must specify either '--conn-str' or '--address'")

    try:
        if not args.partitions:
            partitions = loop.run_until_complete(get_partitions(client))
        else:
            partitions = args.partitions.split(",")
        pumps = []
        for pid in partitions:
            sender = client.add_async_sender(partition=pid, send_timeout=500)
            pumps.append(pump(pid, sender, args, args.duration))
        loop.run_until_complete(client.run_async())
        loop.run_until_complete(asyncio.gather(*pumps, return_exceptions=True))
    finally:
        loop.run_until_complete(client.stop_async())

if __name__ == '__main__':
    test_long_running_partition_send_async()
