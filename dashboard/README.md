# gRPC Dashboard

A lightweight, minimalistic Next.js dashboard for managing your gRPC key-value store.

## Architecture

```
┌───────────┐      gRPC-Web       ┌───────────┐      native gRPC    ┌───────────┐
│ Browser   ├─────────────────────►│   Envoy   ├─────────────────────►│  grpcsrv │
│(Dashboard)│   via localhost:8080│  (Proxy)  │   via localhost:50051│  (tonic) │
└───────────┘                      └───────────┘                       └───────────┘
```

## Features

- **HTTP Basic Auth**: Simple username/password authentication
- **PUT Data**: Store key-value pairs with namespace support
- **Statistics**: Real-time stats for reads, writes, deletes, and list operations
- **Health Status**: Monitor server health (SERVING/NOT_SERVING)

## Prerequisites

- Node.js 18+ (for running the dashboard)
- Docker & Docker Compose (for the backend services)

## Quick Start

### 1. Start the Backend Services

```bash
# From the project root
cd /path/to/gRPC-server

# Start Kafka, gRPC server, and Envoy
make docker-compose-up
```

### 2. Configure Dashboard Authentication

Edit `.env.local` in the dashboard directory:

```bash
cd dashboard

# Set your credentials (plain text for development)
echo "DASHBOARD_USERNAME=myuser" >> .env.local
echo "DASHBOARD_PASSWORD=mypassword" >> .env.local
```

**For production**, use a bcrypt hash:
```bash
# Generate a hash
node -e "console.log(require('bcrypt').hashSync('your-password', 10))"

# Use the hash in .env.local
DASHBOARD_PASSWORD=$2a$10$...
```

### 3. Start the Dashboard

```bash
cd dashboard
npm install
npm run dev
```

### 4. Access the Dashboard

1. Open `http://localhost:3000` in your browser
2. When prompted, enter the username and password from `.env.local`
3. Start managing your key-value store!

## Usage

### PUT Data

1. Enter a **Namespace** (e.g., `users`, `products`)
2. Enter an **ID** (e.g., `user-123`, `prod-456`)
3. Enter the **Data** (any text content)
4. Click **Store Data**

### Statistics

The dashboard automatically shows:
- **Total Reads**: Number of GET operations
- **Total Writes**: Number of PUT operations  
- **Total Deletes**: Number of DELETE operations
- **List Operations**: Number of LIST operations

### Health Status

Shows the current server status:
- **SERVING**: Server is healthy and accepting requests
- **NOT_SERVING**: Server is unavailable
- **UNKNOWN**: Unable to determine status

## Project Structure

```
dashboard/
├── app/
│   ├── layout.tsx          # Root layout
│   ├── page.tsx           # Dashboard UI
│   └── globals.css        # Styles
├── src/
│   ├── lib/
│   │   ├── auth.ts       # Authentication utilities
│   │   └── grpc.ts       # gRPC client configuration
│   └── proto/
│      └── kv.ts          # TypeScript definitions from proto
├── middleware.ts          # HTTP Basic Auth middleware
├── .env.local            # Environment variables (gitignored)
├── .env.example          # Example environment variables
└── package.json
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DASHBOARD_USERNAME` | HTTP Basic Auth username | `admin` |
| `DASHBOARD_PASSWORD` | HTTP Basic Auth password (plain or bcrypt hash) | `admin` |
| `NEXT_PUBLIC_GRPC_WEB_URL` | Envoy gRPC-Web proxy URL | `http://localhost:8080` |

## Troubleshooting

### "Failed to fetch" errors

1. Ensure all services are running: `docker ps`
2. Check Envoy is forwarding: `curl http://localhost:8080/`
3. Verify gRPC server is accessible: `curl http://localhost:50051/`

### Authentication issues

1. Verify credentials in `.env.local` match what you're entering
2. Check browser console for 401 errors
3. Try clearing browser cache/cookies

### Stats not updating

The dashboard refreshes stats every 5 seconds. If stats aren't updating:
1. Check browser console for errors
2. Verify Envoy is running and forwarding to gRPC server
