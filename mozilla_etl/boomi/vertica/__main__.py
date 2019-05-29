import bonobo
import bonobo_sqlalchemy
import os
import pendulum

from bonobo.config import Service, use, use_no_input, use_context, use_context_processor
from bonobo.config.functools import transformation_factory
from bonobo.constants import NOT_MODIFIED

from dateutil import parser as dateparser

import re
import io
import csv

import logging

import fs
import datetime

STMT = """
SELECT * from {table}
WHERE {date_field} = '{now}'
"""


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

    return {"output": bonobo.open_fs(options["output"])}


def get_graph(**options):
    graph = bonobo.Graph()

    ds = pendulum.instance(options["now"])
    now = ds.to_date_string()

    params = {
        "now": now,
        "table": options["table_name"],
        "date_field": options["date_field"],
    }

    engine = options["engine"][0]

    options["services"]["output"].makedirs(options["table_name"],
                                           recreate=True)

    graph.add_chain(
        bonobo_sqlalchemy.Select(STMT.format(**params), engine=engine),
        # note when we imported this, in case of discrepancy and to standardize the processed date field
        bonobo.Format(etl_in_date=now),
        # Need to make the path when local, grmbl
        bonobo.CsvWriter("{table}/{now}.csv".format(**params),
                         fs="output",
                         quoting=csv.QUOTE_MINIMAL),
    )

    return graph


# The __main__ block actually execute the graph.
if __name__ == "__main__":
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
                path.dirname(path.dirname(path.dirname(
                    path.abspath(__file__)))))[1],
        )

        __package__ = ".".join(me)

    from .. import add_default_arguments, add_default_services

    parser = bonobo.get_argument_parser()

    add_default_arguments(parser)

    parser.add_argument("--table-name", required=True, type=str)
    parser.add_argument("--date-field", type=str, default="snapshot_date")
    parser.add_argument("--output", type=str, default="file://.")

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        retval = bonobo.run(get_graph(**options, services=services),
                            services=services)

        for node in retval.nodes:
            stats = node.statistics
            if stats['err'] > 0 and stats['err'] == stats['in']:
                print("One step completely failed")
                exit(1)
