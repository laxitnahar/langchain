from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import get_settings


ORDER_LINE_ITEMS_TABLE = "order_line_item_facts"


ORDER_COLUMN_DEFINITIONS = [
    ("shop_name", "TEXT NOT NULL"),
    ("shopify_id", "BIGINT NOT NULL"),
    ("order_name", "TEXT"),
    ("order_number", "BIGINT"),
    ("email", "TEXT"),
    ("customer_shopify_id", "BIGINT"),
    ("customer_email", "TEXT"),
    ("customer_first_name", "TEXT"),
    ("customer_last_name", "TEXT"),
    ("financial_status", "TEXT"),
    ("fulfillment_status", "TEXT"),
    ("source_name", "TEXT"),
    ("currency", "TEXT"),
    ("tags", "TEXT"),
    ("created_at", "TIMESTAMPTZ"),
    ("processed_at", "TIMESTAMPTZ"),
    ("updated_at", "TIMESTAMPTZ"),
    ("cancelled_at", "TIMESTAMPTZ"),
    ("is_test", "BOOLEAN NOT NULL DEFAULT FALSE"),
    ("subtotal_price_amount", "NUMERIC(18, 2)"),
    ("total_discounts_amount", "NUMERIC(18, 2)"),
    ("total_tax_amount", "NUMERIC(18, 2)"),
    ("total_shipping_amount", "NUMERIC(18, 2)"),
    ("total_price", "TEXT"),
    ("total_price_amount", "NUMERIC(18, 2)"),
    ("current_total_price_amount", "NUMERIC(18, 2)"),
    ("total_line_items_price_amount", "NUMERIC(18, 2)"),
    ("line_items_count", "INTEGER NOT NULL DEFAULT 0"),
    ("items_quantity", "INTEGER NOT NULL DEFAULT 0"),
    ("order_city", "TEXT"),
    ("order_country", "TEXT"),
    ("payload", "JSONB NOT NULL"),
    ("synced_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
]

ORDER_MUTABLE_COLUMNS = [
    "order_name",
    "order_number",
    "email",
    "customer_shopify_id",
    "customer_email",
    "customer_first_name",
    "customer_last_name",
    "financial_status",
    "fulfillment_status",
    "source_name",
    "currency",
    "tags",
    "created_at",
    "processed_at",
    "updated_at",
    "cancelled_at",
    "is_test",
    "subtotal_price_amount",
    "total_discounts_amount",
    "total_tax_amount",
    "total_shipping_amount",
    "total_price",
    "total_price_amount",
    "current_total_price_amount",
    "total_line_items_price_amount",
    "line_items_count",
    "items_quantity",
    "order_city",
    "order_country",
    "payload",
]

LINE_ITEM_COLUMNS = [
    ("shop_name", "TEXT NOT NULL"),
    ("order_shopify_id", "BIGINT NOT NULL"),
    ("line_item_shopify_id", "BIGINT NOT NULL"),
    ("product_shopify_id", "BIGINT"),
    ("variant_shopify_id", "BIGINT"),
    ("sku", "TEXT"),
    ("product_title", "TEXT"),
    ("variant_title", "TEXT"),
    ("display_name", "TEXT"),
    ("vendor", "TEXT"),
    ("quantity", "INTEGER NOT NULL DEFAULT 0"),
    ("price_amount", "NUMERIC(18, 2)"),
    ("total_discount_amount", "NUMERIC(18, 2)"),
    ("payload", "JSONB NOT NULL"),
    ("synced_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
]


def _column_sql(definitions: list[tuple[str, str]]) -> str:
    return ",\n        ".join(f"{name} {data_type}" for name, data_type in definitions)


SCHEMA_STATEMENTS = [
    f"""
    CREATE TABLE IF NOT EXISTS orders (
        {_column_sql(ORDER_COLUMN_DEFINITIONS)},
        PRIMARY KEY (shop_name, shopify_id)
    );
    """,
    *[
        f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {name} {data_type};"
        for name, data_type in ORDER_COLUMN_DEFINITIONS
        if name not in {"shop_name", "shopify_id", "payload", "synced_at", "total_price"}
    ],
    """
    CREATE TABLE IF NOT EXISTS order_line_item_facts (
        shop_name TEXT NOT NULL,
        order_shopify_id BIGINT NOT NULL,
        line_item_shopify_id BIGINT NOT NULL,
        product_shopify_id BIGINT,
        variant_shopify_id BIGINT,
        sku TEXT,
        product_title TEXT,
        variant_title TEXT,
        display_name TEXT,
        vendor TEXT,
        quantity INTEGER NOT NULL DEFAULT 0,
        price_amount NUMERIC(18, 2),
        total_discount_amount NUMERIC(18, 2),
        payload JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (shop_name, order_shopify_id, line_item_shopify_id),
        FOREIGN KEY (shop_name, order_shopify_id)
            REFERENCES orders (shop_name, shopify_id)
            ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        shop_name TEXT NOT NULL,
        shopify_id BIGINT NOT NULL,
        title TEXT,
        handle TEXT,
        product_status TEXT,
        payload JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (shop_name, shopify_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS customers (
        shop_name TEXT NOT NULL,
        shopify_id BIGINT NOT NULL,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        payload JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (shop_name, shopify_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_orders_shop_created_at ON orders (shop_name, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_orders_shop_customer ON orders (shop_name, customer_shopify_id, customer_email);",
    "CREATE INDEX IF NOT EXISTS idx_orders_shop_city ON orders (shop_name, order_city, order_country);",
    "CREATE INDEX IF NOT EXISTS idx_order_line_item_facts_shop_product ON order_line_item_facts (shop_name, product_shopify_id, product_title);",
]

ORDER_INSERT_COLUMNS = [name for name, _ in ORDER_COLUMN_DEFINITIONS if name != "synced_at"]
ORDER_INSERT_SQL = f"""
    INSERT INTO orders (
        {", ".join(ORDER_INSERT_COLUMNS)},
        synced_at
    )
    VALUES (
        {", ".join(["%s"] * len(ORDER_INSERT_COLUMNS))},
        NOW()
    )
    ON CONFLICT (shop_name, shopify_id)
    DO UPDATE SET
        {", ".join(f"{name} = EXCLUDED.{name}" for name in ORDER_MUTABLE_COLUMNS)},
        synced_at = NOW();
"""

LINE_ITEM_INSERT_COLUMNS = [name for name, _ in LINE_ITEM_COLUMNS if name != "synced_at"]
LINE_ITEM_INSERT_SQL = f"""
    INSERT INTO {ORDER_LINE_ITEMS_TABLE} (
        {", ".join(LINE_ITEM_INSERT_COLUMNS)},
        synced_at
    )
    VALUES (
        {", ".join(["%s"] * len(LINE_ITEM_INSERT_COLUMNS))},
        NOW()
    )
    ON CONFLICT (shop_name, order_shopify_id, line_item_shopify_id)
    DO UPDATE SET
        {", ".join(f"{name} = EXCLUDED.{name}" for name in LINE_ITEM_INSERT_COLUMNS[3:])},
        synced_at = NOW();
"""


def get_connection() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(settings.postgres_dsn)


def initialize_database() -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            for statement in SCHEMA_STATEMENTS:
                cursor.execute(statement)
        connection.commit()


def run_select_query(query: str) -> list[dict[str, Any]]:
    with get_connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _shipping_total(order: dict[str, Any]) -> Decimal:
    shipping_total = _to_decimal(order.get("total_shipping_price"))
    if shipping_total is not None:
        return shipping_total

    shipping_total_set = (
        (order.get("total_shipping_price_set") or {}).get("shop_money") or {}
    )
    shipping_total = _to_decimal(shipping_total_set.get("amount"))
    if shipping_total is not None:
        return shipping_total

    total = Decimal("0")
    for shipping_line in order.get("shipping_lines") or []:
        if isinstance(shipping_line, dict):
            total += _to_decimal(shipping_line.get("price")) or Decimal("0")
    return total


def _build_order_row(shop_name: str, order: dict[str, Any]) -> tuple[Any, ...] | None:
    order_id = _to_int(order.get("id"))
    if order_id is None:
        return None

    customer = order.get("customer") or {}
    shipping_address = order.get("shipping_address") or {}
    billing_address = order.get("billing_address") or {}
    line_items = [
        line_item
        for line_item in (order.get("line_items") or [])
        if isinstance(line_item, dict)
    ]
    email = _first_text(order.get("email"), order.get("contact_email"), customer.get("email"))

    values = {
        "shop_name": shop_name,
        "shopify_id": order_id,
        "order_name": _first_text(order.get("name")),
        "order_number": _to_int(order.get("order_number")),
        "email": email,
        "customer_shopify_id": _to_int(customer.get("id")),
        "customer_email": _first_text(customer.get("email"), email),
        "customer_first_name": _first_text(customer.get("first_name")),
        "customer_last_name": _first_text(customer.get("last_name")),
        "financial_status": _first_text(order.get("financial_status")),
        "fulfillment_status": _first_text(order.get("fulfillment_status")),
        "source_name": _first_text(order.get("source_name")),
        "currency": _first_text(order.get("currency")),
        "tags": _first_text(order.get("tags")),
        "created_at": order.get("created_at"),
        "processed_at": order.get("processed_at"),
        "updated_at": order.get("updated_at"),
        "cancelled_at": order.get("cancelled_at"),
        "is_test": bool(order.get("test", False)),
        "subtotal_price_amount": _to_decimal(order.get("subtotal_price")),
        "total_discounts_amount": _to_decimal(order.get("total_discounts")),
        "total_tax_amount": _to_decimal(order.get("total_tax")),
        "total_shipping_amount": _shipping_total(order),
        "total_price": _first_text(order.get("total_price")),
        "total_price_amount": _to_decimal(order.get("total_price")),
        "current_total_price_amount": _to_decimal(order.get("current_total_price")),
        "total_line_items_price_amount": _to_decimal(order.get("total_line_items_price")),
        "line_items_count": len(line_items),
        "items_quantity": sum(_to_int(item.get("quantity")) or 0 for item in line_items),
        "order_city": _first_text(shipping_address.get("city"), billing_address.get("city")),
        "order_country": _first_text(
            shipping_address.get("country"),
            billing_address.get("country"),
        ),
        "payload": Jsonb(order),
    }

    return tuple(values[column] for column in ORDER_INSERT_COLUMNS)


def _build_line_item_rows(
    shop_name: str,
    order: dict[str, Any],
) -> list[tuple[Any, ...]]:
    order_id = _to_int(order.get("id"))
    if order_id is None:
        return []

    rows: list[tuple[Any, ...]] = []
    for line_item in order.get("line_items") or []:
        if not isinstance(line_item, dict):
            continue

        line_item_id = _to_int(line_item.get("id"))
        if line_item_id is None:
            continue

        values = {
            "shop_name": shop_name,
            "order_shopify_id": order_id,
            "line_item_shopify_id": line_item_id,
            "product_shopify_id": _to_int(line_item.get("product_id")),
            "variant_shopify_id": _to_int(line_item.get("variant_id")),
            "sku": _first_text(line_item.get("sku")),
            "product_title": _first_text(line_item.get("title")),
            "variant_title": _first_text(line_item.get("variant_title")),
            "display_name": _first_text(line_item.get("name")),
            "vendor": _first_text(line_item.get("vendor")),
            "quantity": _to_int(line_item.get("quantity")) or 0,
            "price_amount": _to_decimal(line_item.get("price")),
            "total_discount_amount": _to_decimal(line_item.get("total_discount")),
            "payload": Jsonb(line_item),
        }
        rows.append(tuple(values[column] for column in LINE_ITEM_INSERT_COLUMNS))

    return rows


def upsert_orders(shop_name: str, orders: list[dict[str, Any]]) -> int:
    if not orders:
        return 0

    order_rows: list[tuple[Any, ...]] = []
    line_item_rows: list[tuple[Any, ...]] = []
    order_ids: list[int] = []

    for order in orders:
        row = _build_order_row(shop_name, order)
        if row is None:
            continue

        order_rows.append(row)
        line_item_rows.extend(_build_line_item_rows(shop_name, order))
        order_ids.append(row[1])

    if not order_rows:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(ORDER_INSERT_SQL, order_rows)
            cursor.execute(
                """
                DELETE FROM order_line_item_facts
                WHERE shop_name = %s
                  AND order_shopify_id = ANY(%s);
                """,
                (shop_name, order_ids),
            )
            if line_item_rows:
                cursor.executemany(LINE_ITEM_INSERT_SQL, line_item_rows)
        connection.commit()

    return len(order_rows)


def upsert_products(shop_name: str, products: list[dict[str, Any]]) -> int:
    if not products:
        return 0

    rows = [
        (
            shop_name,
            product["id"],
            product.get("title"),
            product.get("handle"),
            product.get("status"),
            Jsonb(product),
        )
        for product in products
        if product.get("id") is not None
    ]

    if not rows:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO products (
                    shop_name,
                    shopify_id,
                    title,
                    handle,
                    product_status,
                    payload,
                    synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (shop_name, shopify_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    handle = EXCLUDED.handle,
                    product_status = EXCLUDED.product_status,
                    payload = EXCLUDED.payload,
                    synced_at = NOW();
                """,
                rows,
            )
        connection.commit()

    return len(rows)


def upsert_customers(shop_name: str, customers: list[dict[str, Any]]) -> int:
    if not customers:
        return 0

    rows = [
        (
            shop_name,
            customer["id"],
            customer.get("email"),
            customer.get("first_name"),
            customer.get("last_name"),
            Jsonb(customer),
        )
        for customer in customers
        if customer.get("id") is not None
    ]

    if not rows:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO customers (
                    shop_name,
                    shopify_id,
                    email,
                    first_name,
                    last_name,
                    payload,
                    synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (shop_name, shopify_id)
                DO UPDATE SET
                    email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    payload = EXCLUDED.payload,
                    synced_at = NOW();
                """,
                rows,
            )
        connection.commit()

    return len(rows)


def _get_count(connection: psycopg.Connection, table_name: str, shop_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT COUNT(*) AS count FROM {} WHERE shop_name = %s;").format(
                sql.Identifier(table_name)
            ),
            (shop_name,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def _get_latest_timestamp(
    connection: psycopg.Connection,
    table_name: str,
    shop_name: str,
) -> Any:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT MAX(synced_at) AS latest_synced_at
                FROM {}
                WHERE shop_name = %s;
                """
            ).format(sql.Identifier(table_name)),
            (shop_name,),
        )
        row = cursor.fetchone()
        return row["latest_synced_at"] if row else None


def _get_primary_currency(connection: psycopg.Connection, shop_name: str) -> str | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT currency
            FROM orders
            WHERE shop_name = %s
              AND currency IS NOT NULL
              AND currency <> ''
            GROUP BY currency
            ORDER BY COUNT(*) DESC, currency ASC
            LIMIT 1;
            """,
            (shop_name,),
        )
        row = cursor.fetchone()
        return row["currency"] if row else None


def _get_orders_last_7_days(
    connection: psycopg.Connection,
    shop_name: str,
) -> dict[str, Any]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*)::INT AS orders_placed,
                NOW() - INTERVAL '7 days' AS since_at,
                NOW() AS as_of
            FROM orders
            WHERE shop_name = %s
              AND COALESCE(is_test, FALSE) = FALSE
              AND cancelled_at IS NULL
              AND COALESCE(processed_at, created_at) >= NOW() - INTERVAL '7 days';
            """,
            (shop_name,),
        )
        row = cursor.fetchone() or {}

    return {
        "count": row.get("orders_placed", 0),
        "since_at": row.get("since_at"),
        "as_of": row.get("as_of"),
    }


def _get_top_products_last_month(
    connection: psycopg.Connection,
    shop_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                oli.product_shopify_id,
                COALESCE(
                    NULLIF(oli.product_title, ''),
                    NULLIF(oli.display_name, ''),
                    'Untitled product'
                ) AS product_title,
                MAX(oli.vendor) AS vendor,
                MAX(o.currency) AS currency,
                SUM(COALESCE(oli.quantity, 0))::INT AS units_sold,
                COUNT(DISTINCT oli.order_shopify_id)::INT AS order_count,
                ROUND(
                    SUM(
                        (COALESCE(oli.price_amount, 0) * COALESCE(oli.quantity, 0))
                        - COALESCE(oli.total_discount_amount, 0)
                    )::NUMERIC,
                    2
                ) AS net_sales
            FROM order_line_item_facts AS oli
            INNER JOIN orders AS o
                ON o.shop_name = oli.shop_name
               AND o.shopify_id = oli.order_shopify_id
            WHERE o.shop_name = %s
              AND COALESCE(o.is_test, FALSE) = FALSE
              AND o.cancelled_at IS NULL
              AND COALESCE(o.processed_at, o.created_at)
                    >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
              AND COALESCE(o.processed_at, o.created_at)
                    < DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY
                oli.product_shopify_id,
                COALESCE(
                    NULLIF(oli.product_title, ''),
                    NULLIF(oli.display_name, ''),
                    'Untitled product'
                )
            ORDER BY units_sold DESC, net_sales DESC, product_title ASC
            LIMIT %s;
            """,
            (shop_name, limit),
        )
        return cursor.fetchall()


def _get_promotion_recommendation(
    connection: psycopg.Connection,
    shop_name: str,
) -> dict[str, Any] | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                oli.product_shopify_id,
                COALESCE(
                    NULLIF(oli.product_title, ''),
                    NULLIF(oli.display_name, ''),
                    'Untitled product'
                ) AS product_title,
                MAX(oli.vendor) AS vendor,
                MAX(o.currency) AS currency,
                SUM(COALESCE(oli.quantity, 0))::INT AS units_sold,
                COUNT(DISTINCT oli.order_shopify_id)::INT AS order_count,
                ROUND(
                    SUM(
                        (COALESCE(oli.price_amount, 0) * COALESCE(oli.quantity, 0))
                        - COALESCE(oli.total_discount_amount, 0)
                    )::NUMERIC,
                    2
                ) AS net_sales
            FROM order_line_item_facts AS oli
            INNER JOIN orders AS o
                ON o.shop_name = oli.shop_name
               AND o.shopify_id = oli.order_shopify_id
            WHERE o.shop_name = %s
              AND COALESCE(o.is_test, FALSE) = FALSE
              AND o.cancelled_at IS NULL
              AND COALESCE(o.processed_at, o.created_at) >= NOW() - INTERVAL '30 days'
            GROUP BY
                oli.product_shopify_id,
                COALESCE(
                    NULLIF(oli.product_title, ''),
                    NULLIF(oli.display_name, ''),
                    'Untitled product'
                )
            ORDER BY units_sold DESC, net_sales DESC, order_count DESC, product_title ASC
            LIMIT 1;
            """,
            (shop_name,),
        )
        recommendation = cursor.fetchone()

    if recommendation is None:
        return None

    return {
        **recommendation,
        "reason": (
            "Recommended because it led recent sales over the last 30 days by units sold, "
            "with revenue used as the tie-breaker."
        ),
    }


def _get_store_insights(connection: psycopg.Connection, shop_name: str) -> dict[str, Any]:
    return {
        "primary_currency": _get_primary_currency(connection, shop_name),
        "orders_last_7_days": _get_orders_last_7_days(connection, shop_name),
        "top_products_last_month": _get_top_products_last_month(connection, shop_name),
        "promotion_recommendation": _get_promotion_recommendation(connection, shop_name),
    }


def get_store_snapshot(shop_name: str, preview_limit: int = 5) -> dict[str, Any]:
    with get_connection() as connection:
        orders_count = _get_count(connection, "orders", shop_name)
        order_line_items_count = _get_count(connection, ORDER_LINE_ITEMS_TABLE, shop_name)
        products_count = _get_count(connection, "products", shop_name)
        customers_count = _get_count(connection, "customers", shop_name)

        latest_synced_at = max(
            filter(
                None,
                [
                    _get_latest_timestamp(connection, "orders", shop_name),
                    _get_latest_timestamp(connection, ORDER_LINE_ITEMS_TABLE, shop_name),
                    _get_latest_timestamp(connection, "products", shop_name),
                    _get_latest_timestamp(connection, "customers", shop_name),
                ],
            ),
            default=None,
        )

        insights = _get_store_insights(connection, shop_name)

    return {
        "counts": {
            "orders": orders_count,
            "order_line_items": order_line_items_count,
            "products": products_count,
            "customers": customers_count,
        },
        "latest_synced_at": latest_synced_at,
        "insights": insights,
    }
