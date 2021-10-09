import sys
import json
from functools import wraps

from requests.exceptions import HTTPError

import click

DEBUG = False


def catch_exceptions(func):
    """
    Catches and simplifies expected errors thrown by the cli.

    catch_exceptions should be used as a decorator.

    :param func: The function which may throw exceptions which should be
        simplified.
    :type func: func
    :returns: The decorated function.
    :rtype: func
    """
    @wraps(func)
    def decorated(*args, **kwargs):
        """
        Invokes ``func``, catches errors, prints the error message and
        exits the cli with a non-zero exit code.
        """
        try:
            return func(*args, **kwargs)
        except Exception as error:
            if type(error) is HTTPError:
                print_response(error.response)
            else:
                write(error)
            sys.exit(1)

    return decorated


def enable_debug():
    global DEBUG
    DEBUG = True


def print_response(response):
    if DEBUG:
        write("HTTP {} {}".format(response.status_code, response.reason))
        for header_name, header_value in list(response.headers.items()):
            write("{}: {}".format(header_name, header_value))
        write("\n")
    try:
        write(response.json(), output_format="json")
    except ValueError:
        write(response.text)


def write(var, output_format="str"):
    """
    Writes ``var`` to stdout. If output_format is set to "json",
    write ``var`` as a JSON string.
    :param var: The object to print
    :type var: obj
    :param output_format: The format to print the output as. Allowed values: \
    "str" or "json"
    :type output_format: str
    """
    if output_format == "json":
        stream = json.dumps(var, indent=4)
    if output_format == "str":
        stream = var
    click.echo(stream)
