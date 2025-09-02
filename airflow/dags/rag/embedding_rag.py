from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import time

from bs4 import BeautifulSoup

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from article_processor import article_processor_loader
from article_processor.base import ArticleProcessor
from airflow.sdk import Variable


news_sources = [
    ("cnn", "sources"),
    ("bbc-news", "sources"),
    ("nbc-news", "sources"),
    ("al-jazeera-english", "sources"),
    ("phoronix", "domains"),
    ("nintendolife", "domains"),
    ("9to5google", "domains"),
    ("pushsquare", "domains"),
    ("jalopnik", "domains"),
    ("nintendoeverything", "domains"),
    ("macrumors", "domains"),
    ("tomsguide", "domains"),
    ("gamerant", "domains"),
    ("polygon", "domains"),
    ("theverge", "domains"),
    ("gizmodo", "domains")
]

def transform_to_chunks(text: str, chunk_size: int = 250, chunk_overlap: int = 25) -> list[dict[str,str|int]]:
    import requests
        
    response = requests.post(
        "http://embedding-api:8000/text/chunk/",
        json={
            "text": text,
            "chunk_size": chunk_size,
            "chunk_overlap": chunk_overlap
        }
    )
    
    data = response.json()
              
    return data.get('chunks', [])

def chunk_articles_to_bucket(
    list_of_article_keys: list[str], 
    hook: S3Hook, 
    article_processor: ArticleProcessor
    ):
    import json
    
    bucket_name = 'news-bucket'

    for key in list_of_article_keys:
        html_content: str = hook.read_key(key, bucket_name=bucket_name)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        title, text = article_processor.process(soup)
        
        if title is None or text is None:
            print(f"Skipping article due to missing title or text: {key}")
            continue
        
        chunks = transform_to_chunks(text)
                        
        jsonl_data = "\n".join([
            json.dumps({
                "chunk_index": chunk.get("chunk_id"),
                "title": title,
                "text": chunk.get("text"),
                "token_count": chunk.get("token_count")
            }) 
            for chunk in chunks
        ])
                
        jsonl_key = key.replace("news-htmls", "article-chunks").replace(".html", ".jsonl")
        
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                hook.load_string(
                    string_data=jsonl_data,
                    key=jsonl_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                print(f"Successfully uploaded {jsonl_key} to {bucket_name}")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to upload {jsonl_key}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2 ** attempt)
        
def fetch_list_of_article_keys(news_source: str, date: str, hook: S3Hook) -> list[str]:
    bucket_name = 'news-bucket'
    prefix = f"{date}/{news_source}/news-htmls/"
    return hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        
def generate_qna_pairs(
    date: str, 
    news_source: str,
    hook: S3Hook
    ):
    import time
    import json
    from qna_generator.qna_generator import generate_questions
    
    bucket_name = 'news-bucket'
    prefix = f"{date}/{news_source}/article-chunks/"
    list_of_chunked_article_keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    
        
    for key in list_of_chunked_article_keys:
        start_time = time.time()
        
        jsonl_content = hook.read_key(key, bucket_name=bucket_name)
        
        # keys = chunk_index, title, chunk
        list_of_chunks_json = [json.loads(line) for line in jsonl_content.splitlines() if line.strip()]
                
        output = generate_questions(list_of_chunks_json)
        
        jsonl_data = "\n".join([
            json.dumps({
                "chunk_index": chunk.chunk_index,
                "title": list_of_chunks_json[0].get("title"),
                "questions": chunk.questions
            }) 
            for chunk in output.chunks
        ])
    
        jsonl_key = key.replace("article-chunks", "qna")
    
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                hook.load_string(
                    string_data=jsonl_data,
                    key=jsonl_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                end_time = time.time()
                print(f"Successfully uploaded {jsonl_key} to {bucket_name} after {end_time-start_time:.2f}s.")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to upload {jsonl_key}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2 ** attempt)
    
    
def get_previous_day(**kwargs):
    ds = kwargs["ds"]
    current_date = datetime.strptime(ds, "%Y-%m-%d")
    previous_day = current_date - timedelta(days=1)

    print(previous_day.strftime("%Y-%m-%d"))

    return {
        'date_str': previous_day.strftime("%Y-%m-%d"),
        'year': previous_day.strftime("%Y"),
        'month': previous_day.strftime("%m"),
        'day': previous_day.strftime("%d")
    }

with DAG(
    dag_id='rag_dag',
    start_date=datetime(2025, 7, 22),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    
    hook = S3Hook(aws_conn_id="MINIO_S3")
    
    get_previous_day_task = PythonOperator(
        task_id='get_previous_day',
        python_callable=get_previous_day
    )

 
    check_ollama_api = HttpSensor(
        task_id="check_api_health",
        http_conn_id="ollama_api",  # set up in Airflow Connections UI
        endpoint="/api/version", # e.g. GET https://api.example.com/health
        poke_interval=300,                  # check every 30s
        timeout=1800,                       # fail after 5 min
        mode="reschedule",
        retries=24,
        retry_delay=timedelta(hours=1),
    )
 
    grouped_tasks = []
 
    for news_source, source_type in news_sources:
        
        with TaskGroup(group_id=f'news_source_{news_source}') as task_group:
            
            
            # wait_for_news = ExternalTaskSensor(
            #     task_id=f'wait_for_news__{news_source}',
            #     external_dag_id='load_news_dag',
            #     external_task_group_id=f'news_source_{news_source}',
            #     allowed_states=['success'],
            #     failed_states=['failed'],
            #     poke_interval=60,
            #     timeout=7200,
            #     mode='reschedule'
            # )
            
            fetch_list_of_article_keys_task = PythonOperator(
                task_id=f'fetch_list_of_article_keys__{news_source}',
                python_callable=fetch_list_of_article_keys,
                op_args=[news_source, get_previous_day_task.output, hook]
            )
            
            article_processor = article_processor_loader(news_source)
            
            chunk_articles_to_bucket_task = PythonOperator(
                task_id=f'chunk_articles_to_bucket__{news_source}',
                python_callable=chunk_articles_to_bucket,
                op_args=[fetch_list_of_article_keys_task.output, hook, article_processor],
            )
            
            generate_qna_pairs_task = PythonOperator(
                task_id=f'generate_qna_pairs__{news_source}',
                python_callable=generate_qna_pairs,
                op_args=[get_previous_day_task.output, news_source, hook],
            )
            
            # wait_for_news >> fetch_list_of_article_keys_task >> chunk_articles_to_bucket_task >> generate_qna_pairs_task
            fetch_list_of_article_keys_task >> chunk_articles_to_bucket_task >> generate_qna_pairs_task
            
            
        grouped_tasks.append(task_group)

    get_previous_day_task >> check_ollama_api >> grouped_tasks