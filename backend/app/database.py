from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import get_settings


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS orders (
        shop_name TEXT NOT NULL,
        shopify_id BIGINT NOT NULL,
        order_name TEXT,
        order_number BIGINT,
        email TEXT,
        total_price TEXT,
        payload JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (shop_name, shopify_id)
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
]


def get_connection() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(settings.postgres_dsn)


def initialize_database() -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            for statement in SCHEMA_STATEMENTS:
                cursor.execute(statement)
        connection.commit()


def upsert_orders(shop_name: str, orders: list[dict[str, Any]]) -> int:
    if not orders:
        return 0

    rows = [
        (
            shop_name,
            order["id"],
            order.get("name"),
            order.get("order_number"),
            order.get("email"),
            order.get("total_price"),
            Jsonb(order),
        )
        for order in orders
        if order.get("id") is not None
    ]

    if not rows:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO orders (
                    shop_name,
                    shopify_id,
                    order_name,
                    order_number,
                    email,
                    total_price,
                    payload,
                    synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (shop_name, shopify_id)
                DO UPDATE SET
                    order_name = EXCLUDED.order_name,
                    order_number = EXCLUDED.order_number,
                    email = EXCLUDED.email,
                    total_price = EXCLUDED.total_price,
                    payload = EXCLUDED.payload,
                    synced_at = NOW();
                """,
                rows,
            )
        connection.commit()

    return len(rows)


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
            f"SELECT COUNT(*) AS count FROM {table_name} WHERE shop_name = %s;",
            (shop_name,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def _get_latest_timestamp(connection: psycopg.Connection, table_name: str, shop_name: str) -> Any:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            f"""
            SELECT MAX(synced_at) AS latest_synced_at
            FROM {table_name}
            WHERE shop_name = %s;
            """,
            (shop_name,),
        )
        row = cursor.fetchone()
        return row["latest_synced_at"] if row else None


def get_store_snapshot(shop_name: str, preview_limit: int = 5) -> dict[str, Any]:
    with get_connection() as connection:
        orders_count = _get_count(connection, "orders", shop_name)
        products_count = _get_count(connection, "products", shop_name)
        customers_count = _get_count(connection, "customers", shop_name)

        latest_synced_at = max(
            filter(
                None,
                [
                    _get_latest_timestamp(connection, "orders", shop_name),
                    _get_latest_timestamp(connection, "products", shop_name),
                    _get_latest_timestamp(connection, "customers", shop_name),
                ],
            ),
            default=None,
        )

        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT shopify_id, order_name, order_number, email, total_price, synced_at
                FROM orders
                WHERE shop_name = %s
                ORDER BY synced_at DESC, shopify_id DESC
                LIMIT %s;
                """,
                (shop_name, preview_limit),
            )
            order_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT shopify_id, title, handle, product_status, synced_at
                FROM products
                WHERE shop_name = %s
                ORDER BY synced_at DESC, shopify_id DESC
                LIMIT %s;
                """,
                (shop_name, preview_limit),
            )
            product_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT shopify_id, email, first_name, last_name, synced_at
                FROM customers
                WHERE shop_name = %s
                ORDER BY synced_at DESC, shopify_id DESC
                LIMIT %s;
                """,
                (shop_name, preview_limit),
            )
            customer_rows = cursor.fetchall()

    return {
        "counts": {
            "orders": orders_count,
            "products": products_count,
            "customers": customers_count,
        },
        "latest_synced_at": latest_synced_at,
        "previews": {
            "orders": order_rows,
            "products": product_rows,
            "customers": customer_rows,
        },
    }

