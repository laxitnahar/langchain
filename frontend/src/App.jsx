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
    provider: '',
  },
};

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function formatCellValue(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  if (Array.isArray(value) || isPlainObject(value)) {
    return JSON.stringify(value);
  }

  return String(value);
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

function formatChartValue(value) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(value);
}

function humanizeKey(value) {
  return String(value)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function choosePrimaryNumericColumn(columns) {
  const preferredPatterns = [
    /count/i,
    /total/i,
    /sales|revenue|amount/i,
    /units|quantity/i,
    /orders/i,
  ];

  for (const pattern of preferredPatterns) {
    const match = columns.find((column) => pattern.test(column));
    if (match) {
      return match;
    }
  }

  return columns[0];
}

function buildAssistantChart(rows) {
  if (rows.length === 0 || !rows.every(isPlainObject)) {
    return null;
  }

  const columns = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
  const numericColumns = columns.filter((column) => {
    const presentValues = rows
      .map((row) => row[column])
      .filter((value) => value !== null && value !== undefined && value !== '');

    return presentValues.length > 0 && presentValues.every((value) => toNumberOrNull(value) !== null);
  });

  if (numericColumns.length === 0) {
    return null;
  }

  if (rows.length === 1) {
    const points = numericColumns
      .map((column) => ({
        label: humanizeKey(column),
        value: toNumberOrNull(rows[0][column]),
      }))
      .filter((point) => point.value !== null);

    if (points.length === 0) {
      return null;
    }

    return {
      title: 'Metric chart',
      subtitle: 'Numeric values returned by the query',
      points,
    };
  }

  const labelColumns = columns.filter((column) => !numericColumns.includes(column));
  const labelColumn = labelColumns.find((column) =>
    rows.some((row) => row[column] !== null && row[column] !== undefined && row[column] !== ''),
  );

  if (!labelColumn) {
    return null;
  }

  const primaryNumericColumn = choosePrimaryNumericColumn(numericColumns);
  const points = rows
    .map((row, index) => ({
      label: formatCellValue(row[labelColumn] ?? `Row ${index + 1}`),
      value: toNumberOrNull(row[primaryNumericColumn]),
    }))
    .filter((point) => point.value !== null);

  if (points.length === 0) {
    return null;
  }

  return {
    title: `${humanizeKey(primaryNumericColumn)} by ${humanizeKey(labelColumn)}`,
    subtitle: `Auto-charting ${humanizeKey(primaryNumericColumn)} from the returned rows`,
    points,
  };
}

function AssistantChart({ chart }) {
  if (!chart) {
    return null;
  }

  const maxValue = Math.max(...chart.points.map((point) => Math.abs(point.value)), 1);

  return (
    <section className="assistant-chart-card">
      <div className="panel-header">
        <h3>{chart.title}</h3>
        <span>{chart.points.length} bars</span>
      </div>
      <p className="helper-text">{chart.subtitle}</p>
      <div className="assistant-chart">
        {chart.points.map((point) => (
          <div className="assistant-chart-row" key={`${point.label}-${point.value}`}>
            <div className="assistant-chart-meta">
              <span className="assistant-chart-label">{point.label}</span>
              <strong>{formatChartValue(point.value)}</strong>
            </div>
            <div className="assistant-chart-track">
              <div
                className="assistant-chart-fill"
                style={{ width: `${Math.max((Math.abs(point.value) / maxValue) * 100, 6)}%` }}
              />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function AssistantResultsPreview({ rows, rowCount }) {
  if (rows.length === 0) {
    return <p className="empty-state">No rows were returned for this question.</p>;
  }

  const isTabular = rows.every(isPlainObject);
  if (!isTabular) {
    return <pre className="code-block json-block">{JSON.stringify(rows, null, 2)}</pre>;
  }

  const columns = Array.from(
    new Set(rows.flatMap((row) => Object.keys(row))),
  );
  const chart = buildAssistantChart(rows);

  return (
    <>
      <p className="helper-text">
        Showing {rows.length} of {rowCount} rows.
      </p>
      <AssistantChart chart={chart} />
      <div className="table-wrap assistant-table-wrap">
        <table className="assistant-table">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={`assistant-row-${rowIndex}`}>
                {columns.map((column) => (
                  <td key={`${rowIndex}-${column}`}>{formatCellValue(row[column])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export default function App() {
  const [shopName, setShopName] = useState('');
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState(EMPTY_RESULT);
  const [loadingAction, setLoadingAction] = useState('');
  const [error, setError] = useState('');
  const [errorContext, setErrorContext] = useState('');

  async function callApi(path, actionLabel, extraParams = {}) {
    const trimmedShop = shopName.trim();

    if (!trimmedShop) {
      setError('Enter a Shopify shop name first.');
      setErrorContext(actionLabel);
      return;
    }

    setLoadingAction(actionLabel);
    setError('');
    setErrorContext('');

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
      setErrorContext('');
    } catch (requestError) {
      setError(requestError.message || 'Unexpected error.');
      setErrorContext(actionLabel);
      if (actionLabel === 'ask') {
        setResult((currentResult) => ({
          ...currentResult,
          assistant: EMPTY_RESULT.assistant,
        }));
      }
    } finally {
      setLoadingAction('');
    }
  }

  const database = result.database || EMPTY_RESULT.database;
  const counts = database.counts || EMPTY_RESULT.database.counts;
  const assistant = result.assistant || EMPTY_RESULT.assistant;
  const previewRows = assistant.rows.slice(0, 10);
  const assistantError = errorContext === 'ask' ? error : '';

  async function askQuestion() {
    const trimmedQuestion = question.trim();
    if (!trimmedQuestion) {
      setError('Enter a question about the synced store data first.');
      setErrorContext('ask');
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

      <section className="panel ask-panel">
        <div className="panel-header">
          <h2>Ask the warehouse</h2>
          <span></span>
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
            {loadingAction === 'ask' ? 'Thinking...' : 'Ask'}
          </button>
        </div>
        <p className="helper-text">
          The question is answered from stored PostgreSQL data, not directly from the live Shopify API.
        </p>
        {assistantError ? <p className="error-banner assistant-error-banner">{assistantError}</p> : null}
        <article className="answer-card assistant-answer-card assistant-answer-block">
          <span className="answer-label">Plain-English answer</span>
          <strong>{assistant.answer || 'Ask a question after syncing store data.'}</strong>
          {assistant.question ? (
            <p className="helper-text">
              Question: <code>{assistant.question}</code>
            </p>
          ) : null}
        </article>
        <div className="assistant-detail-grid">
          <section className="assistant-detail-card">
            <div className="panel-header">
              <h3>Result preview</h3>
              <span>{previewRows.length} of {assistant.row_count} rows</span>
            </div>
            <AssistantResultsPreview rows={previewRows} rowCount={assistant.row_count} />
          </section>
        </div>
      </section>

    </main>
  );
}
