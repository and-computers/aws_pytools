#!/usr/bin/env python3
import csv
import logging
from typing import List, Dict

import boto3
from botocore.exceptions import ClientError
import s3fs

ATHENA_SUCCESS = 'SUCCEEDED'
ATHENA_FAILED = 'FAILED'


class AthenaQueryEngine():

    def __init__(self,
                 database: str,
                 quotechar="`",
                 output_location=None,
                 region='us-east-1',
                 max_attempts=3
                 ) -> None:

        self._client = boto3.client('athena', region_name=region)
        self.db_name = database

        if not output_location:
            self.output_location = f's3://{self.db_name}/tmp'
        else:
            self.output_location = output_location
        if self.output_location.endswith('/'):
            print(f"Removing trailing slash from {self.output_location}")
            self.output_location = self.output_location[:-1]

        self.fs = s3fs.S3FileSystem()
        self.max_attempts = max_attempts
        self.current_attempt = 1
        self.q_id = ''

    def _get_status(self, q_id)->str:
        result = self._client.get_query_execution(QueryExecutionId=q_id)
        query_state = result['QueryExecution']['Status']['State']
        failure_reason = None
        if query_state == ATHENA_FAILED:
            failure_reason = result['QueryExecution']['Status']['StateChangeReason']

        return query_state, failure_reason

    def _retrieve_query_result(self, q_id: str)->List:

        with self.fs.open(f'{self.output_location}/{q_id}.csv', 'r') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader]
        return rows

    def _execute_query(self, query) -> Dict:

        query_resp = self._client.start_query_execution(QueryString=query,
                                                        QueryExecutionContext={
                                                            'Database': self.db_name
                                                        },
                                                        ResultConfiguration={
                                                            'OutputLocation': f'{self.output_location}'
                                                        })

        return query_resp

    def execute_query(self, query: str) -> List:

        status = None

        query_resp = self._execute_query(query)

        self.q_id = query_resp['QueryExecutionId']

        while status != ATHENA_SUCCESS:

            status, failure_reason = self._get_status(self.q_id)

            if status == ATHENA_FAILED:
                if self.current_attempt < self.max_attempts:
                    self.current_attempt += 1
                    self._execute_query(query)
                else:
                    print(f"Failed: {failure_reason}")
                    return []

        data = self._retrieve_query_result(self.q_id)

        self.current_attempt = 1
        return data
if __name__ == "__main__":

    z = AthenaQueryEngine(database="sampledb",
                          output_location="s3://athena-examples-us-east-2/elb/tmp/",
                          region="us-east-2",
                          max_attempts=5)
    ROWS = z.execute_query("SELECT * FROM elb_logs LIMIT 10")
    print(ROWS)
