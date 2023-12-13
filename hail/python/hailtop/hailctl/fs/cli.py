from typing import Optional, List, Tuple, cast
import asyncio
import click
import os
import sys
import typer

from hailtop.aiotools.plan import plan, PlanError
from hailtop.aiotools.sync import sync as aiotools_sync, SyncError


app_without_click = typer.Typer(
    name='fs',
    no_args_is_help=True,
    help='Use cloud object stores and local filesystems.',
    pretty_exceptions_show_locals=False,
)


@app_without_click.callback()
def callback():
    """
    Typer app, including Click subapp
    """

@click.command()
@click.option(
    '--copy-to',
    help='Pairs of source and destination URL. May be specified multiple times. The destination is always treated as a file. See --copy-into to copy into a directory',
    type=(str, str),
    required=False,
    multiple=True,
    default=(),
)
@click.option(
    '--copy-into',
    help='Copies the source path into the target path. The target must not be a file.',
    type=(str, str),
    required=False,
    multiple=True,
    default=(),
)
@click.option('-v', '--verbose', help='The Google project to which to charge egress costs.', is_flag=True, required=False, default=False)
@click.option('--max-parallelism', help='The maximum number of concurrent requests.', type=int, required=False, default=75)
@click.option('--overwrite-if-size-differs', help='Overwrite a file at the destination if the source file and destination file differ in size.', type=bool, required=False, default=False)
@click.option('--make-plan', help='The folder in which to create a new synchronization plan. Must not exist.', type=str, required=False)
@click.option('--use-plan', help='The plan to execute. Must exist.', type=str, required=False)
@click.option('--gcs-requester-pays-project', help='The Google project to which to charge egress costs.', type=str, required=False)
def sync(
    copy_to: List[Tuple[str, str]],
    copy_into: List[Tuple[str, str]],
    verbose: bool,
    max_parallelism: int,
    overwrite_if_size_differs: bool,
    make_plan: Optional[str] = None,
    use_plan: Optional[str] = None,
    gcs_requester_pays_project: Optional[str] = None,
):
    '''Synchronize files between one or more pairs of locations.

    If a corresponding file already exists at the destination, this command will *not* copy it. If
    you want to replace files, delete the destination files first. THIS COMMAND DOES NOT CHECK MD5s
    OR SHAs! For each pair of matching files present at the source and the destination, if the
    number of bytes matches the files are added to plan/matches otherwise they are added to
    plan/differs.

    If you want to replace all destination files which have a corresponding source file with a
    different size, use --overwrite-if-size-differs.

    First generate a plan with --make-plan, then use the plan with --use-plan.

    Copy all the files under gs://gcs-bucket/a to s3://s3-bucket/b. For example,
    gs://gcs-bucket/a/b/c will appear at s3://s3-bucket/b/b/c:



    $ hailctl fs sync --make-plan plan --copy gs://gcs-bucket/a s3://s3-bucket/b



    $ hailctl fs sync --use-plan plan



    Copy all the files as above, but replace destination files that differ in size from the
    corresponding source file:



    $ hailctl fs sync --make-plan plan --copy gs://gcs-bucket/a s3://s3-bucket/b --overwrite-if-size-differs



    $ hailctl fs sync --use-plan plan

    '''
    if (make_plan is None and use_plan is None) or (make_plan is not None and use_plan is not None) :
        print('Must specify one of --make-plan or --use-plan. See hailctl fs sync --help.')
        raise typer.Exit(1)

    if make_plan:
        try:
            asyncio.run(plan(
                make_plan,
                copy_to,
                copy_into,
                gcs_requester_pays_project,
                verbose,
                max_parallelism,
                overwrite_if_size_differs
            ))
        except PlanError as err:
            print(err.args[0])
            sys.exit(err.args[1])
    if use_plan:
        if copy_to or copy_into:
            print('Do not specify --copy-to or --copy-into with --use-plan. Create the plan with --make-plan then call --use-plan without any --copy-to and --copy-into.')
            raise typer.Exit(1)
        try:
            asyncio.run(aiotools_sync(use_plan, gcs_requester_pays_project, verbose, max_parallelism))
        except SyncError as err:
            print(err.args[0])
            sys.exit(err.args[1])


app = cast(click.Group, typer.main.get_command(app_without_click))
app.add_command(sync)
