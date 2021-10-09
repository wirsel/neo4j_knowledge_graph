"""
datalake.cli

BIS Datalake Solution CLI
"""

import click
import json

from . import __version__
from .utils import enable_debug, catch_exceptions, print_response, write
from .api import ApiClient


@click.group()
@click.version_option(version=__version__, prog_name="datalake")
@click.option(
    "--debug", is_flag=True, help="Turn on debug logging."
)
@click.option(
    "--username", type=str, envvar="AD_USERNAME", prompt="Enter your AD username",
    help="Active Directory user name."
)
@click.option(
    "--password", type=str, envvar="AD_PASSWORD", prompt="Enter your AD password", hide_input=True,
    help="Active Directory password."
)
@click.option(
    "--base-url", type=str, envvar="BASE_URL",
    help="Override data lake solution API endpoint (for development purposes)."
)
@click.option(
    "--pool-count", type=str, envvar="POOL_COUNT",
    help="Number of multithreaded processes."
)
@click.pass_context
def cli(ctx, debug, username, password, base_url, pool_count):
    """
    Implements the BIS Datalake Solution CLI.\n
    Set the following environment variables for your Active Directory credentials for the API to read in:\n
      - AD_USERNAME\n
      - AD_PASSWORD\n
    """
    if debug:
        enable_debug()
    ctx.obj = {
        "api_client": ApiClient(username, password,base_url=base_url, pool_count=pool_count)
    }


@cli.command(name="describe-user")
@click.pass_context
@catch_exceptions
def describe_user(ctx):
    """
    Return user information
    """
    api_client = ctx.obj["api_client"]
    response = api_client.get_user()
    print_response(response)
    #write(response.json(), "json")

@cli.command(name="describe-project")
@click.argument("project", type=str)
@click.pass_context
@catch_exceptions
def describe_project(ctx, project):
    """
    [Admin only] Return project information for a specific project
    """
    api_client = ctx.obj["api_client"]
    response = api_client.get_project(project)
    print_response(response)
    #write(response.json(), "json")

@cli.command(name="describe-projects")
@click.pass_context
@catch_exceptions
def describe_projects(ctx):
    """
    [Admin only] Return project information
    """
    api_client = ctx.obj["api_client"]
    response = api_client.get_projects()
    print_response(response)
    #write(response.json(), "json")

@cli.command(name="get-user-authorization")
@click.argument("username", type=str)
@click.argument("project", type=str)
@click.pass_context
@catch_exceptions
def get_user_authorization(ctx, username, project):
    """
    [Admin only] Return assignment information information
    """
    api_client = ctx.obj["api_client"]
    response = api_client.get_user_authorization(username, project)
    print_response(response)
    #write(response.json(), "json")

@cli.command(name="upload-stage")
# @click.argument("project", type=str)
# @click.option(
#    "--region", type=click.Choice(['eu-central-1','ca-central-1']), envvar="AWS_REGION", prompt="Pick the storage region",
#    help="Datalake region can be either eu-central-1 or ca-central-1."
#)
@click.argument("region", type=click.Choice(['eu-central-1','ca-central-1']),envvar="AWS_REGION")
@click.argument("payload", type=click.Path(exists=True))
@click.argument("businessobject", type=str)
@click.pass_context
@catch_exceptions
def upload_to_stage(ctx, region, payload, businessobject):
    """
    [Admin only] Upload payload and and associated metadata
    """
    api_client = ctx.obj["api_client"]
    response = api_client.put_upload_stage(
        region,
        click.format_filename(payload),
        businessobject
    )
    #print(response)
    #print_response(response)
    write(response, "json")

@cli.command(name="upload-core")
# @click.argument("project", type=str)
# @click.option(
#    "--region", type=click.Choice(['eu-central-1','ca-central-1']), envvar="AWS_REGION", prompt="Pick the storage region",
#    help="Datalake region can be either eu-central-1 or ca-central-1."
#)
@click.argument("region", type=click.Choice(['eu-central-1','ca-central-1']),envvar="AWS_REGION")
@click.argument("payload", type=click.Path(exists=True))
@click.argument("metadata", type=click.Path(exists=True))
@click.pass_context
@catch_exceptions
def upload_to_core(ctx, region, payload, metadata):
    """
    [Admin only] Upload payload and and associated metadata
    """
    api_client = ctx.obj["api_client"]
    response = api_client.put_upload_core(
        region,
        click.format_filename(payload),
        click.format_filename(metadata)
    )
    response = api_client.put_file_index(
        "core",
        region,
        "bis",
        response["filesUploaded"]["payload"],
        response["filesUploaded"]["metadata"]
    )
    print_response(response)
    #write(response.json(), "json")

@cli.command(name="upload-consumption")
@click.argument("project", type=str)
@click.argument("payloads", nargs=-1, type=click.Path(exists=True), required=True)
@click.option("--prefix", type=str, required=False)
@click.pass_context
@catch_exceptions
def upload_to_consumption(ctx, project, payloads, prefix):
    """
    [Admin only] Upload to consumption bucket for a project
    """
    api_client = ctx.obj["api_client"]
    responses = []
    for payload in payloads:
        response = api_client.put_upload_consumption(
            project,
            click.format_filename(payload),
            prefix
        )
        responses.append(response)

    # Check uploads were successful, collate failures
    file_count = len(payloads)
    failed_uploads_count = 0
    failed_uploads = []
    for idx, r in enumerate(responses):
        if not r:
            failed_uploads_count += 1
            failed_uploads.append(payloads[idx])

    result =  {
        "uploadsSuccessful" : (file_count - failed_uploads_count),
        "uploadsFailed" : {
            "count": failed_uploads_count,
            "files": failed_uploads
        }
    }

    write(result, "json")

@cli.command(name="upload-manifest")
# @click.argument("region", type=click.Choice(['eu-central-1','ca-central-1']),envvar="AWS_REGION")
@click.argument("manifest", type=click.Path(exists=True))
@click.argument("copy-to-project", default=False, type=bool)
@click.pass_context
@catch_exceptions
def upload_manifest(ctx, manifest, copy_to_project=False):
    """
    [Admin only]  Read in a manifest and upload data and metadata files 
    listed within to the core bucket. Optionally, copy these 
    to a project context in the consumption bucket.
    """
    api_client = ctx.obj["api_client"]

    response_core = api_client.put_upload_manifest(click.format_filename(manifest))

    write(response_core, "json")
    if copy_to_project:
        # print(copy_to_project)
        response_copy = api_client.copy_manifest(click.format_filename(manifest))
        write(response_copy, "json")

@cli.command(name="register-dataobject")
@click.argument("manifest", type=click.Path(exists=True))
@click.argument("update", default=False, type=bool)
@click.pass_context
@catch_exceptions
def register_dataobject(ctx, manifest, update=False):
    """
    [Admin only]  Read in a manifest and register the metadata for the files 
    that will uploaded into datalake. 
    """
    api_client = ctx.obj["api_client"]
    response_core = api_client.register_businessobject(click.format_filename(manifest), update)
    print_response(response_core)
    #write(response_core, "json")        

@cli.command(name="list-dataobject")
@click.argument("dataobjectname",type=str, required=False)
@click.pass_context
@catch_exceptions
def list_dataobject(ctx, dataobjectname):
    """
    [Admin only]  List the registered business object. 
    """
    api_client = ctx.obj["api_client"]
    response_core = api_client.list_businessobject(dataobjectname)
    print_response(response_core)
    #write(response_core, "json")         

@cli.command(name="copy")
@click.argument("project", type=str)
@click.argument("payload", type=str)
@click.pass_context
@catch_exceptions
def copy(ctx, project, payload):
    """
    [Admin only]  Copy payload and its associated metadata to the consumption
    zone for a specific project
    """
    api_client = ctx.obj["api_client"]
    response = api_client.put_copy(
        project,
        click.format_filename(payload)
    )

    if response.status_code is 200:
        write("Successfully copied {} to the {} consumption zone".format(
            payload,
            project
        ))
    else:
        write(json.loads(response.text), output_format="json")


@cli.command(name="download")
@click.argument("project", type=str)
@click.argument("payloads", nargs=-1, type=str, required=True)
@click.pass_context
@catch_exceptions
def download(ctx, project, payloads):
    """
    Download payload
    """
    api_client = ctx.obj["api_client"]
    responses = api_client.get_download_consumption(
        project,
        payloads
    )

    # Check downloads were successful, collate failures
    file_count = len(payloads)
    failed_downloads_count = 0
    failed_downloads = []
    for idx, r in enumerate(responses):
        if not r.status_code == 200:
            failed_downloads_count += 1
            failed_downloads.append(payloads[idx])

    result =  {
        "downloadsSuccessful" : (file_count - failed_downloads_count),
        "downloadsFailed" : {
            "count": failed_downloads_count,
            "files": failed_downloads
        }
    }

    write(result, "json")


@cli.command(name="list-files")
@click.option("--project", type=str, required=False)
@click.option("--day-range", type=int, required=False)
@click.pass_context
@catch_exceptions
def get_list_files(ctx, project, day_range):
    """
    List files available to you. Optionally filter by project or day range
    """
    api_client = ctx.obj["api_client"]
    response = api_client.get_list_files(
        project,
        day_range
    )

    write(response, output_format="json")