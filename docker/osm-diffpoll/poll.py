#!/usr/bin/env python3
import os
import json
import argparse
import time
from dateutil import parser as date_parser
from datetime import datetime, timedelta

import boto3
import requests as r

batch = boto3.client('batch')

# generate changeset URL from replication sequence number
def changeset_url(num):
    if 0 < num <= 999999999:
        hundreds = num % 1000
        thousands = int((num % 1000000) - hundreds)
        millions = int(num - hundreds - thousands)
        decomposed = (str(int(millions / 1000000)).zfill(3), str(int(thousands / 1000)).zfill(3), str(hundreds).zfill(3))
        return "https://planet.openstreetmap.org/replication/minute/{}/{}/{}.osc.gz".format(decomposed[0], decomposed[1], decomposed[2])
    else:
        print("Provided: {} but changeset sequence numbers must be between 1 and 999999999".format(num))
        return None

# parse a state file associated with osm change file
def parse_state(state_text):
    state = { key: value for (key, value) in
        [tuple(description.split("=")) for description in state_text.split("\n")[1:-1]]
    }
    state['txnMaxQueried'] = int(state['txnMaxQueried'])
    state['sequenceNumber'] = int(state['sequenceNumber'])
    state['txnMax'] = int(state['txnMax'])
    state['txnReadyList'] = [int(x) for x in state['txnReadyList'].split(',') if x != '']
    state['txnActiveList'] = [int(x) for x in state['txnActiveList'].split(',') if x != '']
    state['timestamp'] = date_parser.parse(state['timestamp'].replace('\\', ''))
    return state

# submit a job to batch based on the sequence number of a changeset
def submit_index_update(num, dependency = None):
client.submit_job(
    jobName='testJob',
    jobQueue='queueAugDiffDefault',
    jobDefinition='arn:aws:batch:us-east-1:896538046175:job-definition/jobAugDiffAdiUpdate:3',
    parameters={
        'string': 'string'
    },
    containerOverrides={
        'command': [
            'https://planet.openstreetmap.org/replication/minute/002/003/999.osc.gz',
        ]
    },
    dependsOn=[dep for dep in [dependency] if dep is not None]
)


def submit_augdiff(num, dependency)
    return client.submit_job(
        jobName='testJob',
        jobQueue='queueAugDiffDefault',
        jobDefinition='arn:aws:batch:us-east-1:896538046175:job-definition/jobAugDiffAdiUpdate:3',
        parameters={
            'string': 'string'
        },
        containerOverrides={
            'command': [
                'https://planet.openstreetmap.org/replication/minute/002/003/999.osc.gz',
            ]
        },
        dependsOn=[dep for dep in [dependency] if dep is not None]
    )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='.')
    parser.add_argument('-s', '--start', metavar='S3BUCKET', type=int, help='The sequenceNumber from which polling for changes should begin', dest="start")
    args = parser.parse_args()

    # The most current, minutely state
    current_state_request = r.get("https://planet.openstreetmap.org/replication/minute/state.txt")
    current_state = parse_state(current_state_request.text)
    current_number = current_state['sequenceNumber']

    # Initialize this dependency to None
    last_job = None

    # Handle different initialization scenarios
    if args.start is None:
        print('starting at current changeset; replication no. {}'.format(current_number))
    elif args.start > current_number:
        print('start is ahead of current sequence')
    else:
        for seqNum in [x for x in range(args.start, current_number + 1)]:
            indexing_job = submit_index_update(num, last_job)
            submit_changeset(num, indexing_job)
            last_job = indexing_job
            print('running for {}'.format(changeset_url(seqNum)))

    # Poll; submitting jobs upon success
    while True:
        change_request = r.head(changeset_url(current_number))
        if change_request.status_code == 200:
            indexing_job = submit_index_update(num, last_job)
            submit_changeset(num, indexing_job)
            last_job = indexing_job
            print(current_number, 'SUCCESS: {}'.format(change_request.status_code))
            current_number = current_number + 1
        else:
            print(current_number, 'FAILURE: {}'.format(change_request.status_code))

        time.sleep(30)


