from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

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
Table: orders
Primary key: (shop_name, shopify_id)
- shop_name TEXT NOT NULL: normalized Shopify myshopify domain used to partition all data per store.
- shopify_id BIGINT NOT NULL: unique Shopify order id.
- order_name TEXT: human-readable order label such as #1001.
- order_number BIGINT: sequential order number from Shopify.
- email TEXT: best available order email address.
- customer_shopify_id BIGINT: Shopify customer id linked to the order.
- customer_email TEXT: customer email copied onto the order fact row.
- customer_first_name TEXT: customer first name from the order payload.
- customer_last_name TEXT: customer last name from the order payload.
- financial_status TEXT: payment status such as paid or pending.
- fulfillment_status TEXT: fulfillment status reported by Shopify.
- source_name TEXT: channel/source that created the order.
- currency TEXT: order currency code such as USD.
- tags TEXT: comma-separated Shopify tags on the order.
- created_at TIMESTAMPTZ: order creation timestamp.
- processed_at TIMESTAMPTZ: timestamp when Shopify marked the order processed.
- updated_at TIMESTAMPTZ: last Shopify update timestamp for the order.
- cancelled_at TIMESTAMPTZ: cancellation timestamp when the order was cancelled.
- is_test BOOLEAN NOT NULL DEFAULT FALSE: whether the order is a Shopify test order.
- subtotal_price_amount NUMERIC(18, 2): order subtotal before taxes and shipping.
- total_discounts_amount NUMERIC(18, 2): total discounts applied to the order.
- total_tax_amount NUMERIC(18, 2): total taxes charged on the order.
- total_shipping_amount NUMERIC(18, 2): total shipping charges for the order.
- total_price TEXT: raw total_price string from Shopify.
- total_price_amount NUMERIC(18, 2): numeric total order amount.
- current_total_price_amount NUMERIC(18, 2): current total after edits/refunds reflected by Shopify.
- total_line_items_price_amount NUMERIC(18, 2): total price across line items before shipping/tax.
- line_items_count INTEGER NOT NULL DEFAULT 0: number of distinct line items on the order.
- items_quantity INTEGER NOT NULL DEFAULT 0: total quantity across all line items.
- order_city TEXT: city from shipping address or billing fallback.
- order_country TEXT: country from shipping address or billing fallback.
- payload JSONB NOT NULL: raw Shopify order payload.
- synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(): when this warehouse row was last synced.

Table: order_line_item_facts
Primary key: (shop_name, order_shopify_id, line_item_shopify_id)
Foreign key: (shop_name, order_shopify_id) -> orders (shop_name, shopify_id)
- shop_name TEXT NOT NULL: normalized Shopify myshopify domain for the source store.
- order_shopify_id BIGINT NOT NULL: parent Shopify order id.
- line_item_shopify_id BIGINT NOT NULL: unique Shopify line item id.
- product_shopify_id BIGINT: Shopify product id tied to the line item.
- variant_shopify_id BIGINT: Shopify variant id tied to the line item.
- sku TEXT: stock keeping unit for the sold variant.
- product_title TEXT: product title captured on the order line.
- variant_title TEXT: variant title captured on the order line.
- display_name TEXT: full line item display name from Shopify.
- vendor TEXT: product vendor/brand on the order line.
- quantity INTEGER NOT NULL DEFAULT 0: quantity sold for this line item row.
- price_amount NUMERIC(18, 2): per-unit line item price.
- total_discount_amount NUMERIC(18, 2): total discount applied to the line item.
- payload JSONB NOT NULL: raw Shopify line item payload.
- synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(): when this warehouse row was last synced.

Table: products
Primary key: (shop_name, shopify_id)
- shop_name TEXT NOT NULL: normalized Shopify myshopify domain for the source store.
- shopify_id BIGINT NOT NULL: unique Shopify product id.
- title TEXT: product title.
- handle TEXT: Shopify product handle/slug.
- product_status TEXT: current Shopify status such as active or draft.
- payload JSONB NOT NULL: raw Shopify product payload.
- synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(): when this warehouse row was last synced.

Table: customers
Primary key: (shop_name, shopify_id)
- shop_name TEXT NOT NULL: normalized Shopify myshopify domain for the source store.
- shopify_id BIGINT NOT NULL: unique Shopify customer id.
- email TEXT: customer email address.
- first_name TEXT: customer first name.
- last_name TEXT: customer last name.
- payload JSONB NOT NULL: raw Shopify customer payload.
- synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(): when this warehouse row was last synced.
""".strip()


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
        },
    }


def _generate_sql(shop_name: str, question: str) -> str:
    response = _invoke_groq(
        [
            (
                "system",
                (
                    "You write PostgreSQL queries for a Shopify analytics warehouse. "
                    "Return exactly one raw SQL query and nothing else.\n"
                    "Rules:\n"
                    f"- The query must start with SELECT.\n"
                    f"- Use PostgreSQL syntax only.\n"
                    f"- Only use the tables and columns in the provided schema.\n"
                    f"- The query must be scoped to shop_name = '{shop_name}'.\n"
                    f"- Every table in the schema includes a shop_name column.\n"
                    f"- Never write INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, COMMENT, VACUUM, or ANALYZE.\n"
                    f"- Always end with LIMIT {MAX_RESULT_ROWS} or smaller.\n"
                    f"- Do not use markdown fences, prose, or comments.\n\n"
                    f"Schema:\n{SCHEMA_TEXT}"
                ),
            ),
            ("human", question),
        ]
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

    return _invoke_groq(
        [
            (
                "system",
                (
                    "You are a concise retail data analyst. "
                    "Answer in plain English using only the SQL results you receive. "
                    "If the result set is empty, say that no matching stored data was found. "
                    "Do not invent values or mention hidden assumptions as facts. "
                    "Mention key counts, totals, product names, or dates when the rows support them."
                ),
            ),
            ("human", payload),
        ]
    )


def _invoke_groq(messages: list[tuple[str, str]]) -> str:
    settings = get_settings()
    if not settings.groq_api_key:
        raise RuntimeError("GROQ_API_KEY is not configured in backend/.env.")

    with _no_proxy_environment():
        llm = ChatGroq(
            api_key=settings.groq_api_key,
            model=settings.groq_model,
            temperature=0,
            timeout=settings.groq_timeout_seconds,
            max_retries=2,
        )
        response = llm.invoke(messages)

    return _content_to_text(response.content)


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
