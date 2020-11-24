import sys

from hailtop.auth import create_user
from hailtop.utils import sync_handle_error_for_cli


def init_parser(parser):  # pylint: disable=unused-argument
    parser.add_argument("username", type=str,
                        help="Username.")
    parser.add_argument("--email", type=str, required=False,
                        help="Email.")
    parser.add_argument("--service-account",
                        action='store_true',
                        required=False,
                        help="Create a non-human user.")


def main(args, pass_through_args):  # pylint: disable=unused-argument
    rc, _ = sync_handle_error_for_cli(
        create_user, args.username, args.email,
        is_developer=False,
        is_service_account=args.service_account)
    if rc > 0:
        sys.exit(rc)
    print('success')
