import sys

from hailtop.auth import delete_user
from hailtop.utils import sync_handle_error_for_cli


def init_parser(parser):  # pylint: disable=unused-argument
    parser.add_argument("username", type=str,
                        help="Username.")


def main(args, pass_through_args):  # pylint: disable=unused-argument
    rc, _ = sync_handle_error_for_cli(delete_user, args.username)
    if rc > 0:
        sys.exit(rc)
    print('success')
