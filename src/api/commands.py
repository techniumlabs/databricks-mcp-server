"""
API for executing commands on Databricks clusters.

Uses the Command Execution API 1.2 to create execution contexts,
run commands, poll for results, and clean up.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_context(cluster_id: str, language: str = "python") -> str:
    """
    Create an execution context on a cluster.

    Args:
        cluster_id: ID of the cluster
        language: Language for the context (python, scala, sql)

    Returns:
        The context ID

    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating {language} execution context on cluster {cluster_id}")
    result = make_api_request(
        "POST",
        "/api/1.2/contexts/create",
        data={"clusterId": cluster_id, "language": language},
    )
    context_id = result.get("id")
    if not context_id:
        raise DatabricksAPIError("Failed to create execution context: no ID returned")
    logger.info(f"Created execution context: {context_id}")
    return context_id


async def destroy_context(cluster_id: str, context_id: str) -> Dict[str, Any]:
    """
    Destroy an execution context on a cluster.

    Args:
        cluster_id: ID of the cluster
        context_id: ID of the context to destroy

    Returns:
        Empty response on success
    """
    logger.info(f"Destroying execution context {context_id} on cluster {cluster_id}")
    return make_api_request(
        "POST",
        "/api/1.2/contexts/destroy",
        data={"clusterId": cluster_id, "contextId": context_id},
    )


async def run_command(
    cluster_id: str, context_id: str, command: str, language: str = "python"
) -> str:
    """
    Submit a command for execution.

    Args:
        cluster_id: ID of the cluster
        context_id: ID of the execution context
        command: The command/code to execute
        language: Language of the command

    Returns:
        The command ID

    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Submitting command on cluster {cluster_id}, context {context_id}")
    result = make_api_request(
        "POST",
        "/api/1.2/commands/execute",
        data={
            "clusterId": cluster_id,
            "contextId": context_id,
            "language": language,
            "command": command,
        },
    )
    command_id = result.get("id")
    if not command_id:
        raise DatabricksAPIError("Failed to submit command: no ID returned")
    logger.info(f"Submitted command: {command_id}")
    return command_id


async def get_command_status(
    cluster_id: str, context_id: str, command_id: str
) -> Dict[str, Any]:
    """
    Get the status and results of a command.

    Args:
        cluster_id: ID of the cluster
        context_id: ID of the execution context
        command_id: ID of the command

    Returns:
        Command status and results
    """
    return make_api_request(
        "GET",
        "/api/1.2/commands/status",
        params={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id,
        },
    )


async def execute_python(
    cluster_id: str, command: str, language: str = "python", timeout: int = 120
) -> Dict[str, Any]:
    """
    Execute a command on a Databricks cluster end-to-end.

    Creates an execution context, runs the command, polls for results,
    and cleans up the context.

    Args:
        cluster_id: ID of the cluster (must be running)
        command: The Python/SQL/Scala code to execute
        language: Language for execution (default: python)
        timeout: Max seconds to wait for completion (default: 120)

    Returns:
        Dict with status, results, and any error information
    """
    context_id = None
    try:
        # Step 1: Create execution context
        context_id = await create_context(cluster_id, language)

        # Step 2: Submit the command
        command_id = await run_command(cluster_id, context_id, command, language)

        # Step 3: Poll for results
        terminal_states = {"Finished", "Error", "Cancelled"}
        start_time = time.time()
        poll_interval = 1.0  # Start with 1s, increase over time

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                return {
                    "status": "Timeout",
                    "error": f"Command did not complete within {timeout} seconds",
                }

            status_result = await get_command_status(
                cluster_id, context_id, command_id
            )
            status = status_result.get("status")
            logger.debug(f"Command status: {status} (elapsed: {elapsed:.1f}s)")

            if status in terminal_states:
                # Extract results
                results = status_result.get("results", {})
                return {
                    "status": status,
                    "result_type": results.get("resultType"),
                    "data": results.get("data"),
                    "cause": results.get("cause"),
                    "summary": results.get("summary"),
                }

            # Adaptive polling: increase interval up to 5s
            await asyncio.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 5.0)

    finally:
        # Step 4: Always clean up the context
        if context_id:
            try:
                await destroy_context(cluster_id, context_id)
            except Exception as cleanup_err:
                logger.warning(
                    f"Failed to destroy context {context_id}: {cleanup_err}"
                )
