from __future__ import annotations

from typing import Any

from .database import (
    get_store_snapshot,
    upsert_customers,
    upsert_orders,
    upsert_products,
)
from .shopify import ShopifyRestClient, normalize_shop_name


def sync_shop_data(shop_name: str) -> dict[str, Any]:
    normalized_shop_name = normalize_shop_name(shop_name)
    client = ShopifyRestClient(normalized_shop_name)

    orders = client.fetch_orders()
    products = client.fetch_products()
    customers = client.fetch_customers()

    inserted_orders = upsert_orders(normalized_shop_name, orders)
    inserted_products = upsert_products(normalized_shop_name, products)
    inserted_customers = upsert_customers(normalized_shop_name, customers)

    snapshot = get_store_snapshot(normalized_shop_name)

    return {
        "shop_name": normalized_shop_name,
        "fetched": {
            "orders": len(orders),
            "products": len(products),
            "customers": len(customers),
        },
        "upserted": {
            "orders": inserted_orders,
            "products": inserted_products,
            "customers": inserted_customers,
        },
        "database": snapshot,
    }


def load_store_data(shop_name: str) -> dict[str, Any]:
    normalized_shop_name = normalize_shop_name(shop_name)
    snapshot = get_store_snapshot(normalized_shop_name)

    return {
        "shop_name": normalized_shop_name,
        "database": snapshot,
    }
