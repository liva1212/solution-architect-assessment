"""End-to-end JSON-RPC checks against the real stdio process."""

import json
import select
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def peer():
    proc = subprocess.Popen(
        [sys.executable, "-m", "task1.server"], cwd=ROOT,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        bufsize=0,
    )
    assert proc.stdin and proc.stdout and proc.stderr

    def request(message, response=True):
        proc.stdin.write((json.dumps(message, allow_nan=True) + "\n").encode())
        proc.stdin.flush()
        if not response:
            return None
        ready, _, _ = select.select([proc.stdout], [], [], 15)
        assert ready, "server response timed out"
        line = proc.stdout.readline()
        assert line, "server closed stdout"
        result = json.loads(line)
        assert result["jsonrpc"] == "2.0"
        assert ("result" in result) != ("error" in result)
        return result

    init = request({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": {"name": "assessment-test", "version": "1.0"},
    }})
    assert init["id"] == 1 and "result" in init
    request({"jsonrpc": "2.0", "method": "notifications/initialized"}, response=False)
    yield request, proc
    proc.stdin.close()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    remaining_stdout = proc.stdout.read()
    assert not remaining_stdout, f"unexpected stdout: {remaining_stdout!r}"
    stderr = proc.stderr.read().decode()
    assert "Starting Task 1 MCP stdio server" in stderr


def call(request, name, args, id=10):
    return request({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {"name": name, "arguments": args}})


def test_tools_and_valid_operations(peer):
    request, _ = peer
    listed = request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert {tool["name"] for tool in listed["result"]["tools"]} == {"get_customer_record", "trigger_refund"}
    customer = call(request, "get_customer_record", {"customer_id": "CUST-00001"})
    assert customer["result"]["structuredContent"]["email"] == "avery@example.com"
    refund = call(request, "trigger_refund", {"customer_id": "CUST-00001", "amount": 2.5, "reason": "Duplicate order"})
    assert refund["result"]["structuredContent"]["status"] == "simulated_refund"


def test_refund_schema_advertises_constraints(peer):
    listed = peer[0]({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
    refund = next(tool for tool in listed["result"]["tools"] if tool["name"] == "trigger_refund")
    schema = refund["inputSchema"]
    assert schema["properties"]["amount"]["exclusiveMinimum"] == 0
    assert schema["properties"]["amount"]["type"] == "number"
    assert schema["properties"]["reason"]["minLength"] == 10
    assert set(schema["required"]) == {"customer_id", "amount", "reason"}
    assert schema["additionalProperties"] is False


@pytest.mark.parametrize("name,args", [
    ("get_customer_record", {}),
    ("get_customer_record", {"customer_id": "CUST-１２３４５"}),
    ("get_customer_record", {"customer_id": 1}),
    ("get_customer_record", {"customer_id": "CUST-00001", "extra": 1}),
    ("trigger_refund", {"customer_id": "CUST-00001"}),
    ("trigger_refund", {"amount": 1, "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 1}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 0, "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": -1, "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": "2", "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": True, "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": float("nan"), "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": float("inf"), "reason": "Duplicate order"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 1, "reason": "too short"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 1, "reason": "   "}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 1, "reason": "Duplicate order", "extra": 1}),
])
def test_invalid_arguments_are_protocol_errors(peer, name, args):
    response = call(peer[0], name, args, id=42)
    assert response["id"] == 42
    assert response["error"]["code"] == -32602


def test_unknown_customer_is_tool_error(peer):
    response = call(peer[0], "get_customer_record", {"customer_id": "CUST-99999"})
    assert response["result"]["isError"] is True
    assert "Customer not found" in response["result"]["content"][0]["text"]


def test_unknown_tool(peer):
    response = call(peer[0], "missing_tool", {}, id=73)
    assert response["id"] == 73
    assert response["error"]["code"] == -32602


def test_malformed_json_is_safe(peer):
    request, proc = peer
    proc.stdin.write(b'{bad json\n')
    proc.stdin.flush()
    # The SDK's stdio reader discards malformed lines without emitting a response.
    # The connection must remain usable and stdout must stay protocol-clean.
    response = request({"jsonrpc": "2.0", "id": 81, "method": "tools/list"})
    assert response["id"] == 81
    assert "result" in response
