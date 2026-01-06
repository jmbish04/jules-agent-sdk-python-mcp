# Jules Agent SDK - Cloudflare Python Worker Example

This example demonstrates how to run the Jules Agent SDK inside a Cloudflare Python Worker using FastAPI.

## Prerequisites

- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) installed
- A Cloudflare account
- A Jules API key from [Jules dashboard](https://jules.google.com)
- At least one source configured in your Jules account

## Setup

1. Navigate to this directory:
   ```bash
   cd examples/cloudflare-worker
   ```

2. Set your Jules API key as a secret:
   ```bash
   wrangler secret put JULES_API_KEY
   # Enter your API key when prompted
   ```

## Deployment

Deploy the worker to Cloudflare:

```bash
wrangler deploy
```

After deployment, Wrangler will provide you with the worker URL.

## Usage

### Health Check

Test that the worker is running:

```bash
curl https://your-worker-url.workers.dev/
```

Response:
```json
{
  "status": "Jules Agent Worker is running"
}
```

### Start a Session

Create a new Jules agent session:

```bash
curl "https://your-worker-url.workers.dev/agent/start?prompt=Add%20error%20handling"
```

Parameters:
- `prompt` (optional): Task description for the Jules agent (default: "Checking status")
- `source` (optional): Source ID (e.g., 'sources/your-source-id'). If not provided, uses the first available source.
- `starting_branch` (optional): Git branch to start from (default: "main")

Response:
```json
{
  "session_id": "sessions/abc123...",
  "status": "created",
  "url": "https://jules.google.com/sessions/abc123...",
  "docs": "Session created via Cloudflare Python Worker"
}
```

## Local Development

To test locally before deploying:

```bash
wrangler dev
```

Then access the worker at `http://localhost:8787`

## Configuration

The `wrangler.toml` file contains the worker configuration:

- `name`: Worker name (jules-python-worker)
- `main`: Entry point (src/entry.py)
- `compatibility_date`: Cloudflare compatibility date
- `compatibility_flags`: Enables Python workers

## Dependencies

The worker uses these dependencies (defined in `requirements.txt`):

- `fastapi`: Web framework
- `uvicorn`: ASGI server
- `httpx`: HTTP client
- `pydantic`: Data validation
- `jules-agent-sdk`: Jules Agent SDK

## API Endpoints

### `GET /`

Health check endpoint.

**Response:**
```json
{
  "status": "Jules Agent Worker is running"
}
```

### `GET /agent/start`

Create a new Jules agent session.

**Query Parameters:**
- `prompt` (string, optional): Task description
- `source` (string, optional): Source ID
- `starting_branch` (string, optional): Git branch name

**Response:**
```json
{
  "session_id": "sessions/...",
  "status": "created",
  "url": "https://jules.google.com/sessions/...",
  "docs": "Session created via Cloudflare Python Worker"
}
```

**Error Response:**
```json
{
  "detail": "Error message"
}
```

## Troubleshooting

### "JULES_API_KEY not configured"

Make sure you've set the secret:
```bash
wrangler secret put JULES_API_KEY
```

### "No sources available"

Ensure you have at least one source configured in your Jules account. Visit the [Jules dashboard](https://jules.google.com) to add a source.

## Learn More

- [Jules Agent SDK Documentation](../../docs/README.md)
- [Cloudflare Python Workers](https://developers.cloudflare.com/workers/languages/python/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Wrangler Documentation](https://developers.cloudflare.com/workers/wrangler/)
