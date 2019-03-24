#!/usr/bin/env python3

import boto3


class EmailSender():

    def __init__(self, from_address, session=None):
        self._from_address = from_address
        self._client = boto3.client('ses', region_name='us-east-1')

    def send_email(self, to_address, subject, message, html=False):

        # type checking
        if isinstance(to_address, str):
            # check if commas are contained
            if "," in to_address:
                # if commas contained split into multiple address
                addresses_to_pass = to_address.split(",")
            else:
                # if there is no comma, its one address
                addresses_to_pass = [to_address]
        elif isinstance(to_address, list):
            addresses_to_pass = to_address

        else:
            raise ValueError()

        res = self._client.send_email(
            Source=self._from_address,
            Destination={
                'ToAddresses': addresses_to_pass,
                'CcAddresses': [],
                'BccAddresses': []
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'utf-8'
                },
                'Body': {
                    'Text': {
                        'Data': message,
                        'Charset': 'utf-8'
                    },
                    # TODO: check if you can have both
                    'Html': {
                        'Data': message,
                        'Charset': 'utf-8'
                    }
                }
            }
        )

        return res
