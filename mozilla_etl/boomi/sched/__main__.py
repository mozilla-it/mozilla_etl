import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import Service, use, use_no_input, use_context, use_context_processor, use_raw_input
from bonobo.config.functools import transformation_factory
from bonobo.constants import NOT_MODIFIED

from dateutil import parser as dateparser

import re
import io
import csv

import requests
from requests.auth import HTTPBasicAuth

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1Xl0ELlLmiCh4V0uwWv-qe28kN0N0BwK9dUBq6xNLZx8'
RANGE_NAME = 'A4:X'

SCHED_CONFERENCE = 'whistlerallhandsjune2019'
SCHED_API_KEY = os.getenv('SCHED_API_KEY')

_cache = {}


def cache(self, context):
    yield _cache


@use('sched')
def get_sched(sched):
    params = {
        'api_key': SCHED_API_KEY,
        'format': 'json',
        'custom_data': 'Y',
    }
    for event in sched.get(
            "https://{conference}.sched.com/api/session/list".format(
                conference=SCHED_CONFERENCE, api_key=SCHED_API_KEY),
            params=params).json():
        yield event


def get_sheet():
    store = file.Storage('token.json')
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)

    service = build('sheets', 'v4', http=creds.authorize(Http()))

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME).execute()

    for value in result.get('values'):
        if len(value) < 24:
            value.extend([""] * (24 - len(value)))
        yield tuple(value)


@use_raw_input
@use_context_processor(cache)
def cache_sched(cache, row):
    event_key = row.get('event_key')
    if event_key:
        event_key = row.get('event_key')
        cache[event_key] = row._asdict()
        yield NOT_MODIFIED


@use_raw_input
@use_context_processor(cache)
def modified_events(cache, row):
    event_key = row.get('event_key')
    name = row.get('name')
    active = row.get('active')
    last_modified = row.get('last_modified', 'N')

    changed = False

    if name != "" and active != 'N' and last_modified == "Y":
        if event_key in cache:
            sched_event = cache[event_key]
            changed = "modified"
        else:
            changed = "new"
            sched_event = {}

        if changed:
            event = row._asdict()
            event['sched'] = sched_event
            event['sched']['change'] = changed
            yield event


@use('sched')
def add_event(event, sched):
    if event['sched']['change'] != "new":
        return NOT_MODIFIED

    add_url = "https://{conference}.sched.com/api/session/add".format(
        conference=SCHED_CONFERENCE)
    params = {
        'api_key': SCHED_API_KEY,
        'format': 'json',
    }
    res = sched.get(add_url, params=params)

    this = dict(event)

    this['sched']['res'] = res.content
    this['sched']['code'] = res.status_code

    error = False
    if res.content.startswith(b'ERR: '):
        this['sched']['error'] = res.content
        error = True

    if res.status_code == 200 and not error:
        this['sched']['change'] = "created"

    return this


@use('sched')
def modify_event(event, sched):
    if event['sched']['change'] != "changed":
        return NOT_MODIFIED

    mod_url = "https://{conference}.sched.com/api/session/mod".format(
        conference=SCHED_CONFERENCE)
    params = {
        'api_key': SCHED_API_KEY,
        'format': 'json',
    }
    res = sched.get(mod_url, params=params)

    this = dict(event)

    this['sched']['res'] = res.content
    this['sched']['code'] = res.status_code

    error = False
    if res.content.startswith(b'ERR: '):
        this['sched']['error'] = res.content
        error = True

    if res.status_code == 200 and not error:
        this['sched']['change'] = "modified"

    return this


def get_services(**options):
    return {}


def get_sched_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph(
        get_sched,
        bonobo.PrettyPrinter(),
        bonobo.UnpackItems(0),
        cache_sched,
        bonobo.count,
    )

    return graph


def get_sheet_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph(
        get_sheet,
        bonobo.SetFields(fields=[
            "last_modified",
            "event_key",
            "name",
            "active",
            "eventstarttime",
            "eventendtime",
            "event_start",
            "event_end",
            "event_type",
            "event_subtype",
            "seats",
            "description",
            "speakers",
            "vmoderators",
            "vartists",
            "sponsors",
            "exhibitors",
            "volunteers",
            "venue",
            "address",
            "media_url",
            "custom3",
            "audience1",
            "audience2",
        ]),
        modified_events,
        add_event,
        modify_event,
        bonobo.PrettyPrinter(),
        bonobo.count,
    )

    return graph


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    if not __package__:
        from os import sys, path
        top = path.dirname(
            path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
        sys.path.append(top)

        me = []
        me.append(path.split(path.dirname(path.abspath(__file__)))[1])
        me.insert(
            0,
            path.split(path.dirname(path.dirname(path.abspath(__file__))))[1])
        me.insert(
            0,
            path.split(
                path.dirname(
                    path.dirname(path.dirname(path.abspath(__file__)))))[1])

        __package__ = '.'.join(me)

    from .. import add_default_arguments, add_default_services

    parser = bonobo.get_argument_parser()

    add_default_arguments(parser)

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        bonobo.run(get_sched_graph(**options), services=services)
        bonobo.run(get_sheet_graph(**options), services=services)
