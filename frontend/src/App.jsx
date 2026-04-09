import { useState } from 'react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8004';

const EMPTY_RESULT = {
  shop_name: '',
  database: {
    counts: {
      orders: 0,
      order_line_items: 0,
      products: 0,
      customers: 0,
    },
    latest_synced_at: null,
    insights: {
      primary_currency: null,
      orders_last_7_days: {
        count: 0,
        since_at: null,
        as_of: null,
      },
      top_products_last_month: [],
      promotion_recommendation: null,
    },
  },
  assistant: {
    question: '',
    generated_sql: '',
    row_count: 0,
    rows: [],
    answer: '',
  },
};

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  return new Date(value).toLocaleString();
}

function formatAmount(value, currency) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) {
    return String(value);
  }

  return currency ? `${numericValue.toFixed(2)} ${currency}` : numericValue.toFixed(2);
}

function ResourceTable({ title, rows, columns, emptyMessage = 'No rows available yet.' }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h3>{title}</h3>
        <span>{rows.length} rows</span>
      </div>
      {rows.length === 0 ? (
        <p className="empty-state">{emptyMessage}</p>
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
              {rows.map((row, rowIndex) => (
                <tr key={row.shopify_id ?? row.customer_key ?? row.product_shopify_id ?? `${title}-${rowIndex}`}>
                  {columns.map((column) => (
                    <td key={column.key}>
                      {column.render ? column.render(row[column.key], row) : row[column.key] ?? '-'}
                    </td>
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
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState(EMPTY_RESULT);
  const [loadingAction, setLoadingAction] = useState('');
  const [error, setError] = useState('');

  async function callApi(path, actionLabel, extraParams = {}) {
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
      Object.entries(extraParams).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.set(key, value);
        }
      });

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
  const insights = database.insights || EMPTY_RESULT.database.insights;
  const assistant = result.assistant || EMPTY_RESULT.assistant;
  const recommendation = insights.promotion_recommendation;
  const currency = insights.primary_currency;
  const previewRows = assistant.rows.slice(0, 10);

  async function askQuestion() {
    const trimmedQuestion = question.trim();
    if (!trimmedQuestion) {
      setError('Enter a question about the synced store data first.');
      return;
    }

    await callApi('/api/ask', 'ask', { question: trimmedQuestion });
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Python + React + PostgreSQL</p>
          <h1>Shopify sync with analytics-ready order storage</h1>
          <p className="hero-text">
            Sync orders, products, and customers from Shopify, keep the raw payloads,
            and mirror the fields needed to answer common business questions directly
            from PostgreSQL.
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
          <span>Order line items</span>
          <strong>{counts.order_line_items}</strong>
        </article>
        <article className="stat-card">
          <span>Products</span>
          <strong>{counts.products}</strong>
        </article>
        <article className="stat-card">
          <span>Customers</span>
          <strong>{counts.customers}</strong>
        </article>
      </section>

      <section className="panel status-panel">
        <div className="panel-header">
          <h2>Analytics snapshot</h2>
          <span>
            {database.latest_synced_at
              ? `Last sync ${formatDateTime(database.latest_synced_at)}`
              : 'No sync recorded yet'}
          </span>
        </div>
        <p className="status-copy">
          Active shop: <strong>{result.shop_name || 'Not loaded'}</strong>
          {currency ? ` | Primary currency: ${currency}` : ''}
        </p>
        <div className="answers-grid">
          <article className="answer-card">
            <span className="answer-label">Orders in last 7 days</span>
            <strong>{insights.orders_last_7_days.count}</strong>
            <p className="helper-text">
              Window: {formatDateTime(insights.orders_last_7_days.since_at)} to{' '}
              {formatDateTime(insights.orders_last_7_days.as_of)}
            </p>
          </article>
          <article className="answer-card">
            <span className="answer-label">Suggested product to promote</span>
            <strong>{recommendation?.product_title || 'Not enough sales data yet'}</strong>
            <p className="helper-text">
              {recommendation
                ? `${recommendation.units_sold} units sold across ${recommendation.order_count} orders. ${recommendation.reason}`
                : 'Sync recent order history to get a recommendation.'}
            </p>
          </article>
        </div>
      </section>

      <section className="panel ask-panel">
        <div className="panel-header">
          <h2>Ask the warehouse</h2>
          <span>Groq + LangChain + PostgreSQL</span>
        </div>
        <p className="status-copy">
          Ask business questions against the synced database for <strong>{result.shop_name || 'your selected shop'}</strong>.
        </p>
        <label className="question-label" htmlFor="analytics-question">
          NATURAL_LANGUAGE_QUESTION
        </label>
        <textarea
          id="analytics-question"
          value={question}
          onChange={(event) => setQuestion(event.target.value)}
          placeholder="Which product sold the most units last month?"
          rows={4}
        />
        <div className="actions">
          <button
            type="button"
            onClick={askQuestion}
            disabled={Boolean(loadingAction)}
          >
            {loadingAction === 'ask' ? 'Thinking...' : 'Ask Groq'}
          </button>
        </div>
        <p className="helper-text">
          The question is answered from stored PostgreSQL data, not directly from the live Shopify API.
        </p>
        <div className="assistant-grid">
          <article className="answer-card assistant-answer-card">
            <span className="answer-label">Plain-English answer</span>
            <strong>{assistant.answer || 'Ask a question after syncing store data.'}</strong>
            {assistant.question ? (
              <p className="helper-text">
                Question: <code>{assistant.question}</code>
              </p>
            ) : null}
          </article>
          <article className="answer-card assistant-answer-card">
            <span className="answer-label">Query summary</span>
            <strong>{assistant.row_count} rows returned</strong>
            <p className="helper-text">
              The SQL below is the generated warehouse query used to answer the question.
            </p>
          </article>
        </div>
        <div className="assistant-detail-grid">
          <section className="assistant-detail-card">
            <div className="panel-header">
              <h3>Generated SQL</h3>
              <span>{assistant.generated_sql ? 'Latest query' : 'Waiting for a question'}</span>
            </div>
            <pre className="code-block">{assistant.generated_sql || 'SELECT ...'}</pre>
          </section>
          <section className="assistant-detail-card">
            <div className="panel-header">
              <h3>Result preview</h3>
              <span>{previewRows.length} of {assistant.row_count} rows</span>
            </div>
            <pre className="code-block json-block">
              {previewRows.length ? JSON.stringify(previewRows, null, 2) : '[]'}
            </pre>
          </section>
        </div>
      </section>

      <section className="resource-grid">
        <ResourceTable
          title="Top products last month"
          rows={insights.top_products_last_month}
          emptyMessage="No product sales were found for last month."
          columns={[
            { key: 'product_title', label: 'Product' },
            { key: 'vendor', label: 'Vendor' },
            { key: 'units_sold', label: 'Units Sold' },
            { key: 'order_count', label: 'Orders' },
            {
              key: 'net_sales',
              label: 'Net Sales',
              render: (value, row) => formatAmount(value, row.currency),
            },
          ]}
        />
      </section>
    </main>
  );
}
