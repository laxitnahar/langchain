from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from .config import get_settings


NEXT_LINK_PATTERN = re.compile(r'<([^>]+)>;\s*rel="next"')
SHOP_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*\.myshopify\.com$")


def normalize_shop_name(value: str) -> str:
    shop_name = value.strip().lower()

    if not shop_name:
        raise ValueError("A Shopify shop name is required.")

    if shop_name.startswith("http://") or shop_name.startswith("https://"):
        parsed = urlparse(shop_name)
        shop_name = parsed.netloc or parsed.path

    shop_name = shop_name.split("/")[0].strip()

    if not shop_name.endswith(".myshopify.com"):
        shop_name = f"{shop_name}.myshopify.com"

    if not SHOP_NAME_PATTERN.fullmatch(shop_name):
        raise ValueError(
            "Enter a valid Shopify shop domain such as clevrr-test.myshopify.com."
        )

    return shop_name


class ShopifyRestClient:
    def __init__(self, shop_name: str) -> None:
        settings = get_settings()
        self.shop_name = normalize_shop_name(shop_name)
        self.timeout = settings.request_timeout_seconds
        self.max_attempts = 3
        self.headers = {
            "X-Shopify-Access-Token": settings.shopify_access_token,
            "Accept": "application/json",
        }
        self.base_url = (
            f"https://{self.shop_name}/admin/api/{settings.shopify_api_version}"
        )

    def fetch_orders(self) -> list[dict[str, Any]]:
        return self._fetch_all("orders", {"status": "any", "limit": 250})

    def fetch_products(self) -> list[dict[str, Any]]:
        return self._fetch_all("products", {"limit": 250})

    def fetch_customers(self) -> list[dict[str, Any]]:
        return self._fetch_all("customers", {"limit": 250})

    def _fetch_all(
        self,
        resource_name: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_url: str | None = f"{self.base_url}/{resource_name}.json"
        request_params = params or {}

        # Ignore machine-level proxy settings. They caused Shopify requests to fail
        # in this environment even though direct connections worked.
        with httpx.Client(
            headers=self.headers,
            timeout=self.timeout,
            trust_env=False,
        ) as client:
            while next_url:
                response = self._get_with_retry(client, next_url, request_params)

                payload = response.json()
                items.extend(payload.get(resource_name, []))

                next_url = self._extract_next_url(response.headers.get("Link"))
                request_params = {}

        return items

    @staticmethod
    def _extract_next_url(link_header: str | None) -> str | None:
        if not link_header:
            return None

        match = NEXT_LINK_PATTERN.search(link_header)
        return match.group(1) if match else None

    def _get_with_retry(
        self,
        client: httpx.Client,
        url: str,
        params: dict[str, Any],
    ) -> httpx.Response:
        last_error: httpx.HTTPError | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response
            except httpx.TransportError as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    raise
                time.sleep(0.5 * attempt)

        if last_error is not None:
            raise last_error

        raise RuntimeError("Shopify request failed before a response was received.")
