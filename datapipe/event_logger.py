import logging
from typing import Optional

from traceback_with_variables import format_exc

from datapipe.run_config import RunConfig

logger = logging.getLogger("datapipe.event_logger")


class EventLogger:
    def log_state(
        self,
        table_name,
        added_count,
        updated_count,
        deleted_count,
        processed_count,
        run_config: Optional[RunConfig] = None,
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
        run_config: Optional[RunConfig] = None,
    ) -> None:
        if run_config is not None:
            logger.error(f"Error in step {run_config.labels.get('step_name')}: {type} {message}\n{description}")
        else:
            logger.error(f"Error: {type} {message}\n{description}")

    def log_exception(
        self,
        exc: Exception,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        self.log_error(
            type=type(exc).__name__,
            message=str(exc),
            description=format_exc(exc),
            params=[],  # exc.args, # Not all args can be serialized to JSON, dont really need them
            run_config=run_config,
        )

    def log_step_full_complete(
        self,
        step_name: str,
    ) -> None:
        logger.debug(f"Step {step_name} is marked complete")
