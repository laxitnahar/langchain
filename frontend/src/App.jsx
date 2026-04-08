import { useState } from 'react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8004';

const EMPTY_RESULT = {
  shop_name: '',
  database: {
    counts: {
      orders: 0,
      products: 0,
      customers: 0,
    },
    latest_synced_at: null,
    previews: {
      orders: [],
      products: [],
      customers: [],
    },
  },
};

function ResourceTable({ title, rows, columns }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h3>{title}</h3>
        <span>{rows.length} preview rows</span>
      </div>
      {rows.length === 0 ? (
        <p className="empty-state">No rows stored yet for this resource.</p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column.key}>{column.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.shopify_id}>
                  {columns.map((column) => (
                    <td key={column.key}>{row[column.key] ?? '-'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function App() {
  const [shopName, setShopName] = useState('');
  const [result, setResult] = useState(EMPTY_RESULT);
  const [loadingAction, setLoadingAction] = useState('');
  const [error, setError] = useState('');

  async function callApi(path, actionLabel) {
    const trimmedShop = shopName.trim();

    if (!trimmedShop) {
      setError('Enter a Shopify shop name first.');
      return;
    }

    setLoadingAction(actionLabel);
    setError('');

    try {
      const url = new URL(`${API_BASE_URL}${path}`);
      url.searchParams.set('shop_name', trimmedShop);

      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
      });

      const payload = await response.json();

      if (!response.ok) {
        throw new Error(payload.detail || 'Request failed.');
      }

      setResult(payload);
    } catch (requestError) {
      setError(requestError.message || 'Unexpected error.');
    } finally {
      setLoadingAction('');
    }
  }

  const database = result.database || EMPTY_RESULT.database;
  const counts = database.counts || EMPTY_RESULT.database.counts;
  const previews = database.previews || EMPTY_RESULT.database.previews;

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Python + React + PostgreSQL</p>
          <h1>Shopify REST data sync with GET requests only</h1>
          <p className="hero-text">
            Enter a Shopify store domain, pull Orders, Products, and Customers
            through the backend, and store the payloads in PostgreSQL.
          </p>
        </div>
        <div className="panel form-panel">
          <label htmlFor="shop-name">SHOPIFY_SHOP_NAME</label>
          <input
            id="shop-name"
            type="text"
            value={shopName}
            onChange={(event) => setShopName(event.target.value)}
            placeholder="clevrr-test.myshopify.com"
            autoComplete="off"
          />
          <div className="actions">
            <button
              type="button"
              onClick={() => callApi('/api/sync', 'sync')}
              disabled={Boolean(loadingAction)}
            >
              {loadingAction === 'sync' ? 'Syncing...' : 'Sync Shopify Data'}
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={() => callApi('/api/store-data', 'load')}
              disabled={Boolean(loadingAction)}
            >
              {loadingAction === 'load' ? 'Loading...' : 'Load Stored Data'}
            </button>
          </div>
          {error ? <p className="error-banner">{error}</p> : null}
          <p className="helper-text">
            You can type either <code>clevrr-test</code> or the full
            <code>.myshopify.com</code> domain.
          </p>
        </div>
      </section>

      <section className="stats-grid">
        <article className="stat-card">
          <span>Orders</span>
          <strong>{counts.orders}</strong>
        </article>
        <article className="stat-card">
          <span>Products</span>
          <strong>{counts.products}</strong>
        </article>
        <article className="stat-card">
          <span>Customers</span>
          <strong>{counts.customers}</strong>
        </article>
        <article className="stat-card">
          <span>Active shop</span>
          <strong>{result.shop_name || 'Not loaded'}</strong>
        </article>
      </section>

      <section className="panel status-panel">
        <div className="panel-header">
          <h2>Database snapshot</h2>
          <span>
            {database.latest_synced_at
              ? new Date(database.latest_synced_at).toLocaleString()
              : 'No sync recorded yet'}
          </span>
        </div>
        <p className="status-copy">
          Shopify records are stored as raw JSON payloads in PostgreSQL, with a
          few searchable columns mirrored for convenience.
        </p>
      </section>

      <section className="resource-grid">
        <ResourceTable
          title="Orders"
          rows={previews.orders}
          columns={[
            { key: 'shopify_id', label: 'Shopify ID' },
            { key: 'order_name', label: 'Order' },
            { key: 'email', label: 'Email' },
            { key: 'total_price', label: 'Total' },
            { key: 'synced_at', label: 'Synced At' },
          ]}
        />
        <ResourceTable
          title="Products"
          rows={previews.products}
          columns={[
            { key: 'shopify_id', label: 'Shopify ID' },
            { key: 'title', label: 'Title' },
            { key: 'handle', label: 'Handle' },
            { key: 'product_status', label: 'Status' },
            { key: 'synced_at', label: 'Synced At' },
          ]}
        />
        <ResourceTable
          title="Customers"
          rows={previews.customers}
          columns={[
            { key: 'shopify_id', label: 'Shopify ID' },
            { key: 'email', label: 'Email' },
            { key: 'first_name', label: 'First Name' },
            { key: 'last_name', label: 'Last Name' },
            { key: 'synced_at', label: 'Synced At' },
          ]}
        />
      </section>
    </main>
  );
}
