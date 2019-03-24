#!/usr/bin/env python3


import boto3
import pytest
from moto import mock_ses
from aws_toolbox.emails.email_sender import EmailSender


@mock_ses
def test_email_send_single():
    FROM = 'SOM@uhkkMAIL.COM'
    conn = boto3.client('ses')
    conn.verify_email_identity(EmailAddress=FROM)
    es = EmailSender(from_address=FROM)
    res = es.send_email(
        to_address='test@gmail.com',
        subject='test email',
        message='hey did you get this?'
    )

    statuscode = res['ResponseMetadata']['HTTPStatusCode']
    assert 200 == statuscode


@mock_ses
def test_email_send_comma_seperated():
    FROM = 'SOM@GMfdafAIL.COM'
    conn = boto3.client('ses')
    conn.verify_email_identity(EmailAddress=FROM)
    es = EmailSender(from_address=FROM)
    res = es.send_email(
        to_address='test@gmail.com,som@amail.ndcomputers.io,new@hotmail.com',
        subject='test email',
        message='hey did you get this?'
    )

    statuscode = res['ResponseMetadata']['HTTPStatusCode']
    assert 200 == statuscode


@mock_ses
def test_email_send_list():
    FROM = 'SOM@GMAfadsIL.COM'
    conn = boto3.client('ses')
    conn.verify_email_identity(EmailAddress=FROM)
    es = EmailSender(from_address=FROM)
    res = es.send_email(
        to_address=['test@gmail.com', 'som@amail.ndcomputers.io', 'new@hotmail.com'],
        subject='test email',
        message='hey did you get this?'
    )

    statuscode = res['ResponseMetadata']['HTTPStatusCode']
    assert 200 == statuscode
