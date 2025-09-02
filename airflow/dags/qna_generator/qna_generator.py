import json
from pydantic import BaseModel, create_model, conlist

from .system_prompt import system_prompt

def make_chunks_model(num_questions_per_chunk: int = 3, num_chunks: int = 3):
    Chunk = create_model(
        "Chunk",
        chunk_index=int,
        questions=conlist(str, min_length=num_questions_per_chunk, max_length=num_questions_per_chunk),
        __base__=BaseModel
    )
    Chunks = create_model(
        "Chunks",
        chunks=conlist(Chunk, min_length=num_chunks, max_length=num_chunks),
        __base__=BaseModel
    )

    return Chunks


def list_of_chunks_to_str(list_of_chunks_json: list[dict]) -> str:
    cleaned_chunks = {
        json_object.get("chunk_index"): json_object.get("text")
        for json_object in list_of_chunks_json
    }
    
    chunks_str = json.dumps(cleaned_chunks, indent=2)
    
    return chunks_str
       
       

def generate_questions(list_of_chunks_json: list[dict], num_questions_per_chunk: int = 3, max_attempts=5):
    from ollama import Client
    from airflow.sdk import Variable
    
    OLLAMA_ENDPOINT = Variable.get("OLLAMA_ENDPOINT")
    OLLAMA_MODEL = Variable.get("OLLAMA_MODEL")
    client = Client(host=OLLAMA_ENDPOINT)
        
    num_chunks = len(list_of_chunks_json)
    
    ChunksModel = make_chunks_model(num_questions_per_chunk=num_questions_per_chunk, num_chunks=num_chunks)
    schema = ChunksModel.model_json_schema()
    processed_chunks = list_of_chunks_to_str(list_of_chunks_json)
                
    def model_exists(client, model_name: str) -> bool:
        try:
            client.show(model_name)
            return True
        except Exception:
            return False
        
    if not model_exists(client, OLLAMA_MODEL):
        print(f"Model {OLLAMA_MODEL} not found. Pulling...")
        for progress in client.pull(OLLAMA_MODEL, stream=True):
            print(progress)  # optional: shows pull progress
        print(f"✅ Model {OLLAMA_MODEL} pulled successfully")
    else:
        print(f"✅ Model {OLLAMA_MODEL} already exists")
        
    response = client.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": processed_chunks}
        ],
        stream=False,
        format=schema
    )
        
    
    content = response.message.content
    
    parsed = ChunksModel.model_validate_json(content)
            
    return parsed