from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from .db.neo4j_db import Neo4jDb
from contextlib import asynccontextmanager

db = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # connect to db
    global db
    db = Neo4jDb()
    yield
    # close db
    if db is not None:
        db.close()

# create instance
app = FastAPI(lifespan=lifespan)


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Hello~"}


@app.get("/characters/{name}")
async def get_character(
    name: str,
    include_related_character: bool = Query(False, description="Include related characters"),
    include_related_poem: bool = Query(False, description="Include related poems"),
    character_limit: Optional[int] = Query(None, ge=1, description="Maximum number of related characters"),
    poem_limit: Optional[int] = Query(None, ge=1, description="Maximum number of related poems")
):
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    
    try:
        data = db.get_character_data(
            name=name,
            include_related_character=include_related_character,
            include_related_poem=include_related_poem,
            character_limit=character_limit,
            poem_limit=poem_limit
        )
        
        if not data:
            raise HTTPException(status_code=404, detail=f"Character '{name}' not found")
        
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
