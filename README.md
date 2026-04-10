# Shopify REST ETL Demo

This project fetches Shopify `Orders`, `Products`, and `Customers` with the official Shopify Admin REST API using `GET` requests only, then stores the raw payloads in PostgreSQL. The React UI collects the Shopify shop name at runtime and calls the backend through `GET` endpoints only.

## Stack

- Backend: FastAPI
- Frontend: React + Vite
- Database: PostgreSQL

## Project structure

- `backend/`: FastAPI app, Shopify client, PostgreSQL persistence
- `frontend/`: React UI for entering the shop name and triggering sync

## Backend setup

The backend `.env` file has been prefilled with the credentials you supplied. Rotate them if they are real credentials.

1. Make sure PostgreSQL is running and that the database `etl_db` exists.
2. Create a virtual environment and install dependencies:

```powershell
cd backend
$env:NO_PROXY='*'
$env:no_proxy='*'
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8004
```

3. Start the API:

```powershell
uvicorn app.main:app --reload --host 127.0.0.1 --port 8004
```

## Frontend setup

1. Install dependencies:

```powershell
cd frontend
npm install
```

2. Start the React app:

```powershell
npm run dev
```

3. Open the app in the browser and enter the shop name, for example `clevrr-test.myshopify.com`.

## Environment files

- `backend/.env`: contains the Shopify API version, access token, and PostgreSQL credentials you provided
