from __future__ import annotations

import json
import math
import os
import re
import statistics
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from functools import lru_cache
from typing import Any

from langchain.agents import AgentExecutor, create_react_agent
from langchain.tools import tool
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import BaseTool
from langchain_experimental.tools.python.tool import PythonAstREPLTool
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field

from .config import get_settings
from .database import get_store_snapshot, run_select_query
from .shopify import normalize_shop_name

MAX_RESULT_ROWS = 200
LIMIT_PATTERN = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
BLOCKED_QUESTION_PATTERN = re.compile(
    r"^\s*(insert|update|delete|drop|alter|create|grant|revoke|truncate|remove)\b|"
    r"\b(delete\s+from|drop\s+table|truncate\s+table|update\s+\w+|insert\s+into|alter\s+table|create\s+table)\b",
    re.IGNORECASE,
)
BLOCKED_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|grant|revoke|truncate|copy|comment|vacuum|analyze)\b",
    re.IGNORECASE,
)
BLOCKED_PYTHON_PATTERN = re.compile(
    r"(__|import\b|open\b|exec\b|eval\b|compile\b|globals\b|locals\b|"
    r"os\b|sys\b|subprocess\b|pathlib\b|shutil\b|socket\b|requests\b|httpx\b)",
    re.IGNORECASE,
)

SCHEMA_TEXT = """
orders(
  shop_name TEXT,
  shopify_id BIGINT,
  order_name TEXT,
  order_number BIGINT,
  email TEXT,
  customer_shopify_id BIGINT,
  customer_email TEXT,
  customer_first_name TEXT,
  customer_last_name TEXT,
  financial_status TEXT,
  fulfillment_status TEXT,
  source_name TEXT,
  currency TEXT,
  tags TEXT,
  created_at TIMESTAMPTZ,
  processed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  is_test BOOLEAN,
  subtotal_price_amount NUMERIC(18,2),
  total_discounts_amount NUMERIC(18,2),
  total_tax_amount NUMERIC(18,2),
  total_shipping_amount NUMERIC(18,2),
  total_price TEXT,
  total_price_amount NUMERIC(18,2),
  current_total_price_amount NUMERIC(18,2),
  total_line_items_price_amount NUMERIC(18,2),
  line_items_count INTEGER,
  items_quantity INTEGER,
  order_city TEXT,
  order_country TEXT,
  payload JSONB,
  synced_at TIMESTAMPTZ
)
PK: (shop_name, shopify_id)

order_line_item_facts(
  shop_name TEXT,
  order_shopify_id BIGINT,
  line_item_shopify_id BIGINT,
  product_shopify_id BIGINT,
  variant_shopify_id BIGINT,
  sku TEXT,
  product_title TEXT,
  variant_title TEXT,
  display_name TEXT,
  vendor TEXT,
  quantity INTEGER,
  price_amount NUMERIC(18,2),
  total_discount_amount NUMERIC(18,2),
  payload JSONB,
  synced_at TIMESTAMPTZ
)
PK: (shop_name, order_shopify_id, line_item_shopify_id)
FK: (shop_name, order_shopify_id) -> orders(shop_name, shopify_id)

products(
  shop_name TEXT,
  shopify_id BIGINT,
  title TEXT,
  handle TEXT,
  product_status TEXT,
  payload JSONB,
  synced_at TIMESTAMPTZ
)
PK: (shop_name, shopify_id)

customers(
  shop_name TEXT,
  shopify_id BIGINT,
  email TEXT,
  first_name TEXT,
  last_name TEXT,
  payload JSONB,
  synced_at TIMESTAMPTZ
)
PK: (shop_name, shopify_id)

Notes:
- Every table must be filtered by shop_name.
- For order date logic, prefer COALESCE(processed_at, created_at).
- For business metrics, exclude test orders with COALESCE(is_test, FALSE) = FALSE unless the user asks otherwise.
- Exclude cancelled orders with cancelled_at IS NULL unless the user asks otherwise.
- Join order_line_item_facts to orders on shop_name + order_shopify_id = shopify_id.
- Do not select payload unless the user explicitly asks for raw JSON.
- Return only the columns needed to answer the question.
""".strip()

REACT_PROMPT = PromptTemplate.from_template(
    """You are a retail data ReAct agent for a Shopify analytics warehouse.

Current shop_name: {shop_name}

You must answer the user's question by reasoning step by step and using tools when needed.

Rules:
- For data questions, use `inspect_shop_schema` if you need schema details, then use `run_shop_sql`.
- If the user asks to modify, delete, create, update, or remove data, refuse because this endpoint is read-only.
- Base your final answer only on tool observations.
- Use `analyze_rows_with_python` only after `run_shop_sql`, and only for calculations or reshaping on the current `rows`.
- Never write or request non-SELECT SQL.
- If a tool returns `SQL_ERROR` or `PYTHON_ERROR`, fix the issue and try again.
- Keep the final answer concise, plain-English, and directly responsive.

You have access to the following tools:

{tools}

Use this format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat as needed)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Question: {input}
Thought:{agent_scratchpad}"""
)


class SchemaToolInput(BaseModel):
    topic: str = Field(
        default="",
        description="Optional topic or table name to focus the schema hint on.",
    )


class SqlToolInput(BaseModel):
    query: str = Field(
        ...,
        description="A PostgreSQL SELECT query to execute against the current shop warehouse data.",
    )


class PythonToolInput(BaseModel):
    code: str = Field(
        ...,
        description="Python code that analyzes the current `rows` variable from the most recent SQL result.",
    )


@dataclass
class AgentSession:
    shop_name: str
    last_sql: str = ""
    last_rows: list[dict[str, Any]] = field(default_factory=list)


def answer_store_question(shop_name: str, question: str) -> dict[str, Any]:
    normalized_shop_name = normalize_shop_name(shop_name)
    cleaned_question = question.strip()

    if not cleaned_question:
        raise ValueError("A question is required.")

    blocked_reason = _validate_question_intent(cleaned_question)
    if blocked_reason:
        raise ValueError(blocked_reason)

    session = AgentSession(shop_name=normalized_shop_name)
    answer = _run_react_agent(session, cleaned_question)
    snapshot = get_store_snapshot(normalized_shop_name)

    return {
        "shop_name": normalized_shop_name,
        "database": snapshot,
        "assistant": {
            "question": cleaned_question,
            "generated_sql": session.last_sql,
            "row_count": len(session.last_rows),
            "rows": session.last_rows,
            "answer": answer,
            "provider": "groq",
        },
    }


def _run_react_agent(session: AgentSession, question: str) -> str:
    tools = _build_agent_tools(session)

    with _no_proxy_environment():
        executor = AgentExecutor(
            agent=create_react_agent(
                llm=_get_groq_llm(),
                tools=tools,
                prompt=REACT_PROMPT,
            ),
            tools=tools,
            verbose=False,
            handle_parsing_errors=True,
            max_iterations=6,
            return_intermediate_steps=False,
        )
        result = executor.invoke(
            {
                "input": question,
                "shop_name": session.shop_name,
            }
        )

    return str(result["output"]).strip()


def _get_groq_llm() -> ChatGroq:
    settings = get_settings()
    if not settings.groq_api_key:
        raise RuntimeError("GROQ_API_KEY is not configured in backend/.env.")

    return ChatGroq(
        api_key=settings.groq_api_key,
        model=settings.groq_model,
        temperature=0,
        timeout=settings.groq_timeout_seconds,
        max_retries=2,
    )


def _build_agent_tools(session: AgentSession) -> list[BaseTool]:
    @tool("inspect_shop_schema", args_schema=SchemaToolInput)
    def inspect_shop_schema(topic: str = "") -> str:
        """Return the warehouse schema and query rules for the current Shopify shop."""
        if topic:
            return (
                f"Current shop_name: {session.shop_name}\n"
                f"Requested focus: {topic}\n\n"
                f"{_schema_text()}"
            )
        return f"Current shop_name: {session.shop_name}\n\n{_schema_text()}"

    @tool("run_shop_sql", args_schema=SqlToolInput)
    def run_shop_sql(query: str) -> str:
        """Execute a safe PostgreSQL SELECT query for the current shop and return JSON rows."""
        try:
            safe_sql = _safety_check_sql(query, session.shop_name)
            rows = run_select_query(safe_sql)
            serialized_rows = _serialize_rows(rows)
            session.last_sql = safe_sql
            session.last_rows = serialized_rows
            return json.dumps(
                {
                    "sql": safe_sql,
                    "row_count": len(serialized_rows),
                    "rows": serialized_rows,
                },
                default=_json_default,
            )
        except Exception as exc:
            return f"SQL_ERROR: {exc}"

    @tool("analyze_rows_with_python", args_schema=PythonToolInput)
    def analyze_rows_with_python(code: str) -> str:
        """Run safe Python analysis over the current SQL result rows. Available variable: rows."""
        if not session.last_rows:
            return "PYTHON_ERROR: No SQL rows are loaded yet. Use run_shop_sql first."

        blocked_reason = _validate_python_code(code)
        if blocked_reason:
            return f"PYTHON_ERROR: {blocked_reason}"

        python_tool = PythonAstREPLTool(
            locals={
                "rows": session.last_rows,
                "json": json,
                "math": math,
                "statistics": statistics,
            },
            sanitize_input=True,
            handle_tool_error=True,
        )
        try:
            result = python_tool.invoke(code)
            return str(result)
        except Exception as exc:
            return f"PYTHON_ERROR: {exc}"

    return [inspect_shop_schema, run_shop_sql, analyze_rows_with_python]


@lru_cache(maxsize=1)
def _schema_text() -> str:
    return SCHEMA_TEXT


def _safety_check_sql(sql: str, shop_name: str) -> str:
    cleaned = _strip_code_fences(sql).strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()

    if not cleaned:
        raise RuntimeError("The SQL generation step returned an empty query.")

    if not cleaned.lower().startswith("select"):
        raise RuntimeError("The generated SQL must start with SELECT.")

    if ";" in cleaned:
        raise RuntimeError("The generated SQL must contain only one statement.")

    if BLOCKED_SQL_PATTERN.search(cleaned):
        raise RuntimeError("This operation is not permitted.")

    if f"'{shop_name}'" not in cleaned.lower():
        raise RuntimeError("The generated SQL did not scope itself to the requested shop_name.")

    limit_match = LIMIT_PATTERN.search(cleaned)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value > MAX_RESULT_ROWS:
            raise RuntimeError(f"The generated SQL exceeded the {MAX_RESULT_ROWS}-row limit.")
    else:
        cleaned = f"{cleaned}\nLIMIT {MAX_RESULT_ROWS}"

    return f"{cleaned};"


def _validate_python_code(code: str) -> str | None:
    stripped = code.strip()
    if not stripped:
        return "Python code cannot be empty."

    if BLOCKED_PYTHON_PATTERN.search(stripped):
        return (
            "Python analysis is limited to calculations on the current rows. "
            "Imports, file access, network access, subprocess calls, and dunder usage are blocked."
        )

    return None


def _validate_question_intent(question: str) -> str | None:
    if BLOCKED_QUESTION_PATTERN.search(question):
        return (
            "This endpoint is read-only. Ask an analytics question such as "
            "'How many customers do we have?' instead of a data-modifying command."
        )

    return None


def _strip_code_fences(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: _json_default(value)
            for key, value in row.items()
        }
        for row in rows
    ]


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    if isinstance(value, list):
        return [_json_default(item) for item in value]

    if isinstance(value, dict):
        return {str(key): _json_default(item) for key, item in value.items()}

    return value


@contextmanager
def _no_proxy_environment():
    previous = {
        "NO_PROXY": os.environ.get("NO_PROXY"),
        "no_proxy": os.environ.get("no_proxy"),
        "HTTP_PROXY": os.environ.get("HTTP_PROXY"),
        "http_proxy": os.environ.get("http_proxy"),
        "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
        "https_proxy": os.environ.get("https_proxy"),
        "ALL_PROXY": os.environ.get("ALL_PROXY"),
        "all_proxy": os.environ.get("all_proxy"),
    }
    try:
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        for key in (
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ):
            os.environ.pop(key, None)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
