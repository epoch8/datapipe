import logging
import sys

from traceback_with_variables import format_exc
from traceback_with_variables.color import supports_ansi

from datapipe.run_config import RunConfig

logger = logging.getLogger("datapipe.event_logger")


def _format_exception(exc: Exception) -> str:
    # for_file enables ANSI colors when stderr is an interactive terminal.
    return format_exc(exc, for_file=sys.stderr)


class EventLogger:
    def log_state(
        self,
        table_name,
        added_count,
        updated_count,
        deleted_count,
        processed_count,
        run_config: RunConfig | None = None,
    ):
        logger.debug(
            f'Table "{table_name}": added = {added_count}; updated = {updated_count}; '
            f"deleted = {deleted_count}, processed_count = {processed_count}"
        )

    def log_error(
        self,
        type,
        message,
        description,
        params,
        run_config: RunConfig | None = None,
    ) -> None:
        if run_config is not None:
            header = f"Error in step {run_config.labels.get('step_name')}: {type} {message}"
        else:
            header = f"Error: {type} {message}"

        logger.error(header)
        if not description:
            return

        # Write colored tracebacks directly to stderr so ANSI escapes are not altered by logging.
        if supports_ansi(sys.stderr):
            sys.stderr.write(f"{description}\n")
        else:
            logger.error(description)

    def log_exception(
        self,
        exc: Exception,
        run_config: RunConfig | None = None,
    ) -> None:
        self.log_error(
            type=type(exc).__name__,
            message=str(exc),
            description=_format_exception(exc),
            params=[],  # exc.args, # Not all args can be serialized to JSON, dont really need them
            run_config=run_config,
        )

    def log_step_full_complete(
        self,
        step_name: str,
    ) -> None:
        logger.debug(f"Step {step_name} is marked complete")
