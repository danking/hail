import sys

from hailtop.auth import create_session
from hailtop.utils import sync_handle_error_for_cli


def init_parser(parser):  # pylint: disable=unused-argument
    parser.add_argument("username", type=str,
                        help="Username.")
    parser.add_argument("max-age-secs", type=int,
                        help="Maximum session age in seconds.")


def main(args, pass_through_args):  # pylint: disable=unused-argument
    rc, session = sync_handle_error_for_cli(create_session, args.username, args.max_age_secs)
    if rc > 0:
        sys.exit(rc)
    print(session)
