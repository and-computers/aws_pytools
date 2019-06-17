#!/usr/bin/env python3

import boto3
import s3fs

from typing import List



class AthenaQueryEngine():

    def __init__(self, database: str) -> None:
        self._client = boto3.client('athena', region_name='us-east-1')
        self.db_name = database

    def execute_query(self, query: str) -> List:

        status = None
        while status != ATHENA_SUCCESS:
            status = self._get_status()
        return []
    def get_status(self):
        pass
