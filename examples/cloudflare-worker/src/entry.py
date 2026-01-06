from fastapi import FastAPI, HTTPException
from jules_agent_sdk import AsyncJulesClient
from typing import Optional
import os

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "Jules Agent Worker is running"}

@app.get("/agent/start")
async def start_agent_session(
    prompt: str = "Checking status",
    source: Optional[str] = None,
    starting_branch: str = "main"
):
    """
    Start a new Jules agent session.
    
    Args:
        prompt: Task description for the Jules agent
        source: Source ID (e.g., 'sources/your-source-id'). If not provided, uses first available source.
        starting_branch: Git branch to start from (default: 'main')
    """
    api_key = os.getenv("JULES_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="JULES_API_KEY not configured")

    async with AsyncJulesClient(api_key=api_key) as client:
        try:
            # If no source provided, get the first available source
            if not source:
                sources = await client.sources.list_all()
                if not sources:
                    raise HTTPException(status_code=500, detail="No sources available")
                source = sources[0].name
            
            # Create a session using the SDK
            session = await client.sessions.create(
                prompt=prompt,
                source=source,
                starting_branch=starting_branch
            )
            return {
                "session_id": session.id,
                "status": "created",
                "url": session.url,
                "docs": "Session created via Cloudflare Python Worker"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))