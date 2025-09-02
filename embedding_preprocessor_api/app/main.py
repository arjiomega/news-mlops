from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoTokenizer
from langchain.text_splitter import RecursiveCharacterTextSplitter

model_name = "sentence-transformers/all-MiniLM-L6-v2"
tokenizer = AutoTokenizer.from_pretrained(model_name)

app = FastAPI(title="Chunking API", version="1.0.0")

@app.get("/")
def read_root():
    return {"message": "Hi! This is the Chunking API. Visit /docs for API documentation."}

@app.get("/health", summary="Health check", tags=["health"])
async def health_check():
    return {"status": "healthy"}

def get_token_size(text: str) -> int:
    tokens = tokenizer(text, truncation=False)
    return len(tokens['input_ids'])

class Chunk(BaseModel):
    chunk_id: int
    text: str
    token_count: int

class Chunks(BaseModel):
    chunks: list[Chunk]

class ChunkRequest(BaseModel):
    text: str
    chunk_size: int = 250
    chunk_overlap: int = 25

@app.post("/text/get_token_count/")
def token_count(text: str) -> int:
    return get_token_size(text)

@app.post("/text/chunk/")
def chunk_text(request: ChunkRequest) -> Chunks:
    text, chunk_size, chunk_overlap = request.text, request.chunk_size, request.chunk_overlap

    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        add_start_index=True,
    )

    chunks = text_splitter.split_text(text)

    print(chunks)
    
    chunks = [
        Chunk(chunk_id=i, text=chunk, token_count=get_token_size(chunk))
        for i, chunk in enumerate(chunks)
    ]
    

    return Chunks(chunks=chunks)