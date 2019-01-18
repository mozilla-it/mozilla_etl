import bonobo
import requests

import io
import csv
import fs

from bonobo.config import Configurable, Service, ContextProcessor, use, use_context
from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from requests.auth import HTTPBasicAuth

WORKDAY_BASE_URL = 'https://services1.myworkday.com'
GET_USERS_QUERY = '/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Service_Bus?format=csv&bom=true'


@use('workday')
def get_workday_users(workday):
    """Retrieve employees list from WorkDay"""

    resp = workday.get(WORKDAY_BASE_URL + GET_USERS_QUERY)

    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)

    for row in data:
        yield dict(zip(headers, row))


import collections


def workday_centerstone_employee_remap(row):
    dict = collections.OrderedDict()
    dict['EmployeeNo'] = row['Employee_ID']
    dict['Last_Name'] = row['Preffered_Last_Name']
    dict['First_Name'] = row['Preferred_First_Name']
    dict['Hire_Date'] = row['Hire_Date']
    dict['Email)'] = row['Email']
    dict['Employee_Type'] = row['Employee_Type']
    dict['Employee_Status'] = row['Employee_Status']
    dict['Long_Title'] = row['Business_Title']
    dict['Work_Location'] = row['Work_Location']
    dict['Manager'] = row['Manager']
    dict['Team'] = row['Supervisory_Organization']
    dict['termination_date'] = row['termination_date']

    yield dict


def split_termed_employee(row):
    if row['Employee_Status'] == 'Terminated':
        yield row


def split_active_employee(row):
    if row['Employee_Status'] != 'Terminated':
        yield row


def get_workday_employee_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        get_workday_users, workday_centerstone_employee_remap,
        bonobo.UnpackItems(0),
        bonobo.CsvWriter(
            '/etl/centerstone/downloads/workday-users.csv' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="brickftp"),
        bonobo.CsvWriter(
            'workday-users.csv' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="centerstone"))

    graph.add_chain(
        split_active_employee,
        bonobo.UnpackItems(0),
        HeaderlessCsvWriter(
            '/etl/centerstone/downloads/Mozilla_Active_Users.txt' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="brickftp"),
        HeaderlessCsvWriter(
            'Mozilla_Active_Users.txt' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="centerstone"),
        _input=workday_centerstone_employee_remap)

    graph.add_chain(
        split_termed_employee,
        bonobo.UnpackItems(0),
        HeaderlessCsvWriter(
            '/etl/centerstone/downloads/Mozilla_Termed_Users.txt' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="brickftp"),
        HeaderlessCsvWriter(
            'Mozilla_Termed_Users.txt' + options['suffix'],
            lineterminator="\n",
            delimiter="\t",
            fs="centerstone"),
        _input=workday_centerstone_employee_remap)

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

    return {}


# The __main__ block actually execute the graph.
import os
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

    from ... import add_default_arguments, add_default_services, HeaderlessCsvWriter

    parser = bonobo.get_argument_parser()
    add_default_arguments(parser)

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        users_g = get_workday_employee_graph(**options)

        # Run Workday GET users process
        print("# Running GET Workday Employee process")
        bonobo.run(users_g, services=services)
