from fastapi import FastAPI

# create instance
app = FastAPI()


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Hello~"}