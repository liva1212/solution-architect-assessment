"""Strict, stdio-only MCP customer and simulated refund tools."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
from typing import Annotated, Any
from uuid import uuid4

from mcp import MCPError
from mcp.server import Server, ServerRequestContext
from mcp.server.stdio import stdio_server
from mcp.types import (
    INVALID_PARAMS,
    CallToolRequestParams,
    CallToolResult,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
)
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr, ValidationError, field_validator


CustomerId = Annotated[StrictStr, Field(pattern=r"^CUST-[0-9]{5}$")]
Amount = StrictFloat | StrictInt


class CustomerInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: CustomerId


class RefundInput(CustomerInput):
    amount: Amount
    reason: StrictStr

    @field_validator("amount")
    @classmethod
    def positive_finite(cls, value: float | int) -> float | int:
        if not math.isfinite(value) or value <= 0:
            raise ValueError("amount must be positive and finite")
        return value

    @field_validator("reason")
    @classmethod
    def valid_reason(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 10:
            raise ValueError("reason must contain at least 10 characters after trimming")
        return value


CUSTOMERS: dict[str, dict[str, Any]] = {
    "CUST-00001": {"customer_id": "CUST-00001", "name": "Avery Example", "email": "avery@example.com", "status": "active", "account_balance": 125.0},
    "CUST-00002": {"customer_id": "CUST-00002", "name": "Blair Example", "email": "blair@example.com", "status": "active", "account_balance": 0.0},
    "CUST-00003": {"customer_id": "CUST-00003", "name": "Casey Example", "email": "casey@example.com", "status": "inactive", "account_balance": 42.5},
    "CUST-00004": {"customer_id": "CUST-00004", "name": "Drew Example", "email": "drew@example.com", "status": "active", "account_balance": 81.25},
    "CUST-00005": {"customer_id": "CUST-00005", "name": "Ellis Example", "email": "ellis@example.com", "status": "active", "account_balance": 10.0},
}
REFUND_LOG: list[dict[str, Any]] = []
LOGGER = logging.getLogger(__name__)


TOOLS = [
    Tool(name="get_customer_record", description="Return a synthetic customer record.", input_schema=CustomerInput.model_json_schema()),
    Tool(name="trigger_refund", description="Simulate a refund for a synthetic customer.", input_schema=RefundInput.model_json_schema()),
]


def _result(payload: dict[str, Any], *, is_error: bool = False) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, allow_nan=False))],
        is_error=is_error,
        **({} if is_error else {"structuredContent": payload}),
    )


async def list_tools(ctx: ServerRequestContext, params: PaginatedRequestParams | None) -> ListToolsResult:
    return ListToolsResult(tools=TOOLS)


async def call_tool(ctx: ServerRequestContext, params: CallToolRequestParams) -> CallToolResult:
    if params.name == "get_customer_record":
        model: type[BaseModel] = CustomerInput
    elif params.name == "trigger_refund":
        model = RefundInput
    else:
        raise MCPError(code=INVALID_PARAMS, message="Unknown tool name")

    try:
        data = model.model_validate(params.arguments or {})
    except ValidationError as exc:
        details = [{"field": ".".join(map(str, error["loc"])), "type": error["type"]} for error in exc.errors()]
        raise MCPError(code=INVALID_PARAMS, message="Invalid tool arguments", data={"issues": details}) from None

    customer = CUSTOMERS.get(data.customer_id)
    if customer is None:
        return _result({"error": "Customer not found", "customer_id": data.customer_id}, is_error=True)
    if params.name == "get_customer_record":
        return _result(customer)

    assert isinstance(data, RefundInput)
    refund = {
        "customer_id": data.customer_id,
        "refund_id": f"SIM-{uuid4().hex}",
        "amount": float(data.amount),
        "reason": data.reason,
        "status": "simulated_refund",
    }
    REFUND_LOG.append(refund)
    LOGGER.info("Simulated refund %s for %s", refund["refund_id"], data.customer_id)
    return _result(refund)


server = Server("solution-architect-assessment-task1", on_list_tools=list_tools, on_call_tool=call_tool)


async def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    LOGGER.info("Starting Task 1 MCP stdio server")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
