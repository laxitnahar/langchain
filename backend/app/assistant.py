from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from functools import lru_cache
from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

from .config import get_settings
from .database import get_store_snapshot, run_select_query
from .shopify import normalize_shop_name

MAX_RESULT_ROWS = 200
LIMIT_PATTERN = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
BLOCKED_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|grant|revoke|truncate|copy|comment|vacuum|analyze)\b",
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

SQL_GENERATION_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You write exactly one PostgreSQL SELECT query for a Shopify analytics warehouse.\n"
                "Rules:\n"
                "- Output raw SQL only.\n"
                "- Start with SELECT.\n"
                "- Use PostgreSQL syntax only.\n"
                "- Use only the schema below.\n"
                "- Scope the query to shop_name = '{shop_name}'.\n"
                f"- Always return LIMIT {MAX_RESULT_ROWS} or smaller.\n"
                "- Prefer aggregation, grouping, sorting, and concise result sets over raw detailed dumps.\n"
                "- Use readable aliases for aggregates.\n"
                "- Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, COMMENT, VACUUM, or ANALYZE.\n"
                "- Never return payload JSON unless the user explicitly requests raw payload data.\n\n"
                "Schema:\n{schema_text}"
            ),
        ),
        ("human", "{question}"),
    ]
)

ANSWER_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a concise retail data analyst. "
                "Answer in plain English using only the SQL results you receive. "
                "If the result set is empty, say that no matching stored data was found. "
                "Do not invent facts. "
                "When useful, mention counts, totals, product names, currencies, or dates that appear in the rows."
            ),
        ),
        ("human", "{payload}"),
    ]
)


def answer_store_question(shop_name: str, question: str) -> dict[str, Any]:
    normalized_shop_name = normalize_shop_name(shop_name)
    cleaned_question = question.strip()

    if not cleaned_question:
        raise ValueError("A question is required.")

    generated_sql = _generate_sql(normalized_shop_name, cleaned_question)
    safe_sql = _safety_check_sql(generated_sql, normalized_shop_name)
    rows = run_select_query(safe_sql)
    serializable_rows = _serialize_rows(rows)
    answer = _generate_answer(cleaned_question, safe_sql, serializable_rows)
    snapshot = get_store_snapshot(normalized_shop_name)

    return {
        "shop_name": normalized_shop_name,
        "database": snapshot,
        "assistant": {
            "question": cleaned_question,
            "generated_sql": safe_sql,
            "row_count": len(serializable_rows),
            "rows": serializable_rows,
            "answer": answer,
            "provider": "groq",
        },
    }


def _generate_sql(shop_name: str, question: str) -> str:
    response = _invoke_llm(
        SQL_GENERATION_PROMPT.format_messages(
            shop_name=shop_name,
            schema_text=_schema_text(),
            question=question,
        )
    )
    return response


def _generate_answer(question: str, sql: str, rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        {
            "question": question,
            "sql": sql,
            "rows": rows,
        },
        default=_json_default,
        indent=2,
    )

    return _invoke_llm(ANSWER_PROMPT.format_messages(payload=payload))


def _invoke_llm(messages: list[Any]) -> str:
    with _no_proxy_environment():
        llm = _get_groq_llm()
        response = llm.invoke(messages)

    return _content_to_text(response.content)


@lru_cache(maxsize=1)
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
        raise RuntimeError("The generated SQL contained a blocked keyword.")

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


def _strip_code_fences(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
        return "\n".join(part for part in parts if part).strip()

    return str(content).strip()


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
    }
    try:
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
