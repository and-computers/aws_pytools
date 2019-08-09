#!/usr/bin/env python3
import logging
from typing import List

import boto3
from botocore.exceptions import ClientError
import s3fs

ATHENA_SUCCESS = 'SUCCESS'
ATHENA_FAILED = 'FAILED'


class AthenaQueryEngine():

    def __init__(self,
                 database: str,
                 quotechar="`",
                 output_location=None,
                 region='us-east-1'
                 ) -> None:
        self._client = boto3.client('athena', region_name=region)
        self.db_name = database

        if not output_location:
            self.output_location = f's3://{self.db_name}/tmp/'
        else:
            self.output_location = output_location

        self.fs = s3fs.S3FileSystem()
        self.max_retries = 5
        self.current_attempt = 1

    def _get_status(self, q_id)->str:
        result = self._client.get_query_execution(QueryExecutionId=q_id)
        query_state = result['QueryExecution']['Status']['State']
        failure_reason = None
        import pdb
        if query_state == ATHENA_FAILED:
            print(result)
            pdb.set_trace()
            failure_reason = result['QueryExecution']['Status']['StateChangeReason']

        return query_state, failure_reason

    def _retrieve_query_result(self, q_id: str)->List:

        with self.fs.open(f'{self.output_location}/{q_id}.csv', 'r') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader]
        return rows

    def execute_query(self, query: str) -> List:

        status = None
        query_resp = self._client.start_query_execution(QueryString=query,
                                                        QueryExecutionContext={
                                                            'Database': self.db_name
                                                        },
                                                        ResultConfiguration={
                                                            'OutputLocation': f'{self.output_location}'
                                                        })
        q_id = query_resp['QueryExecutionId']

        while status != ATHENA_SUCCESS:

            status, failure_reason = self._get_status(q_id)

            if status == ATHENA_FAILED:
                if self.current_attempt < self.max_retries:
                    self.current_attempt += 1
                    self.execute_query(query)
                else:
                    self.current_attempt = 1
                    return []

        data = self._retrieve_query_result(q_id)

        self.current_attempt = 1
        return []
if __name__ == "__main__":
    import pdb
    pdb.set_trace()
    z = AthenaQueryEngine(database="asodara-data-store",
                          output_location="s3://asodara-data-store/tmp/",
                          region="us-east-2")
    ROWS = z.execute_query("SELECT * FROM asoara WHERE person='OT'")
    print(ROWS)
    pdb.set_trace()
