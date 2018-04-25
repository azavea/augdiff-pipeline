#!/usr/bin/env python3

import boto3

client = boto3.client('batch')

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
    }
)

