from gear import configure_logging
configure_logging()

from .notebook import run  # noqa: E402

run()
