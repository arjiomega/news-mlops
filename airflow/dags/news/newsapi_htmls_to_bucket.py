from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.sdk import Variable
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator, S3ListOperator, S3CreateBucketOperator
)
from requests.exceptions import HTTPError
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

class APINews:
    def __init__(self, api_key, from_date, to_date):
        self.api_key = api_key
        self.from_date = from_date
        self.to_date = to_date

    def fetch_news(self, source_name, source_type, page=1):
        import requests
        import json
        
        base_path = "https://newsapi.org/v2"
        endpoint = "/everything"

        url = base_path + endpoint + "?"

        if source_type == "sources":
            url += f"sources={source_name}" + "&"
        elif source_type == "domains":
            url += f"domains={source_name}.com" + "&"
        else:
            raise ValueError("Invalid Source Type.")

        url += f"from={self.from_date}" + "&"
        url += f"to={self.to_date}" + "&"
        url += f"language=en" + "&"
        url += f"page={page}" + "&"
        url += f"apiKey={self.api_key}"

        max_attempts = 1

        for attempt in range(max_attempts):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as http_err:
                # HTTPError includes response
                status = http_err.response.status_code if http_err.response else None
                reason = http_err.response.reason if http_err.response else None
                print(f"⚠️ HTTP error on attempt {attempt}: {status} {reason} – URL: {url}")
                if attempt == max_attempts:
                    raise
            except requests.exceptions.Timeout as timeout_err:
                print(f"⚠️ Timeout on attempt {attempt}: {timeout_err}")
                if attempt == max_attempts:
                    raise
            except requests.exceptions.ConnectionError as conn_err:
                print(f"⚠️ Connection error on attempt {attempt}: {conn_err}")
                if attempt == max_attempts:
                    raise
            except requests.exceptions.RequestException as req_err:
                print(f"⚠️ Request exception on attempt {attempt}: {req_err}")
                if attempt == max_attempts:
                    raise

        response_json = response.json()
        return json.dumps(response_json)

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

def fetch_list_of_todays_news(aws_conn_id, bucket_name, object_path, **kwargs):
    import json
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    hook = S3Hook(aws_conn_id=aws_conn_id)
    client = hook.get_conn()
    
    raw_json_path = object_path+'/raw.json'
    object_ = client.get_object(Bucket=bucket_name, Key=raw_json_path)

    object_json = json.loads(object_['Body'].read().decode('utf-8'))
    articles = object_json['articles']

    print(articles)

    return articles

def fetch_article_htmls_to_bucket(articles, bucket_name, aws_conn_id, object_path, **kwargs):
    import time
    import random
    import requests
    from bs4 import BeautifulSoup
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # articles = kwargs['ti'].xcom_pull(task_ids=f'fetch_list_of_todays_news__{source_name}')

    user_agents = [
    # Newest Chrome variants on popular platforms
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ]

    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com"
    })

    hook = S3Hook(aws_conn_id=aws_conn_id)
    client = hook.get_conn()

    print(articles)
    get_object_path = lambda filename: object_path+f'/news-htmls/{filename}'

    print(f"Uploading {len(articles)} articles to bucket {bucket_name}")

    for article_idx, article in enumerate(articles):
        title = article['title']
        url = article['url']
        # https://www.bbc.co.uk/news/articles/cq53v066x52o -> cq53v066x52o
        # article_code = url.split("/")[-1]
        # article_code = ''.join(char.lower() for char in article_code if char.isalnum())

        max_attempts = 5
        for attempt in range(1, max_attempts + 1):

            try:
                response = session.get(url, timeout=10)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Failed to fetch {title} {url}")
                print(f"Error: {e}")
                    
                if attempt == max_attempts:
                    print(f"Failed to fetch {title} after {max_attempts} attempts.")
                    break
                    # raise Exception(f"Failed to fetch {title} after {max_attempts} attempts.")
                else:
                    print(f"Retrying {title} ({url})... Attempt {attempt + 1}/{max_attempts}")
                    time.sleep(2 ** attempt)
                    session = requests.Session()
                    session.headers.update({
                        "User-Agent": random.choice(user_agents),
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                        "Referer": "https://www.google.com"
                    })
                    continue
            
        soup = BeautifulSoup(response.content, "html.parser")
        html = soup.prettify()

        file_name = f"article_{article_idx}.html"
        html_object_path = get_object_path(file_name)

        try:
            client.put_object(
                Bucket=bucket_name,
                Key=html_object_path,
                Body=html.encode("utf-8"),
                ContentType="text/html"
            )

            print(f"Uploaded {title} to {bucket_name}/{html_object_path}")
            time.sleep(1)

            if ((article_idx+1) % 10 == 0) or ((article_idx+1) == 1):
                print(f"Processed {article_idx+1} articles, sleeping for 5 seconds to avoid rate limiting.")
                time.sleep(5)

        except Exception as e:
            print(f"Failed to upload {title} to {bucket_name}/{html_object_path}: {e}")
            continue

    session.close()



BUCKET_NAME = "news-bucket"
AWS_CONN_ID = "minio-s3"

with DAG(
    dag_id='load_news_dag',
    start_date=datetime(2025,6,27),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    create_news_bucket = S3CreateBucketOperator(
        task_id="create_news_bucket",
        bucket_name=BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )
    
    get_previous_day_task = PythonOperator(
        task_id='get_previous_day',
        python_callable=get_previous_day
    )

    # (source_name, source_type)
    news_sources = [
        ("cnn", "sources"),
        # ("the-washington-post", "sources"), # FAILING
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
    
    apinews_loader = APINews(
        api_key=Variable.get("NEWSAPI_API_KEY"),
        from_date=get_previous_day_task.output["date_str"],
        to_date=get_previous_day_task.output["date_str"]
        )
    
    news_grouped_tasks_list = []
    
    year = "{{ ti.xcom_pull(task_ids='get_previous_day')['year'] }}"
    month = "{{ ti.xcom_pull(task_ids='get_previous_day')['month'] }}"
    day = "{{ ti.xcom_pull(task_ids='get_previous_day')['day'] }}"
    object_path_builder = lambda source_name: year + f"/{month}" + f"/{day}" + f"/{source_name}"
    
    for source_name, source_type in news_sources:

        with TaskGroup(group_id=f'news_source_{source_name}') as news_grouped_task:
            fetch_current_news = PythonOperator(
                task_id=f'fetch_current_news__{source_name}',
                python_callable=apinews_loader.fetch_news,
                op_kwargs={
                    "source_name": source_name,
                    "source_type": source_type,
                }
            )
            
            object_path = object_path_builder(source_name)
            upload_news_json = S3CreateObjectOperator(
                task_id=f'upload_news_json__{source_name}',
                aws_conn_id=AWS_CONN_ID,
                s3_bucket=BUCKET_NAME,
                s3_key=object_path+"/raw.json",
                # s3_key=(
                #     "{{ ti.xcom_pull(task_ids='get_previous_day')['year'] }}/"
                #     "{{ ti.xcom_pull(task_ids='get_previous_day')['month'] }}/"
                #     "{{ ti.xcom_pull(task_ids='get_previous_day')['day'] }}/"
                #     f"{source_name}/raw.json"
                # ),
                data=fetch_current_news.output,
                replace=True,
            )

            fetch_list_of_todays_news_task = PythonOperator(
                task_id=f'fetch_list_of_todays_news__{source_name}',
                python_callable=fetch_list_of_todays_news,
                op_kwargs={
                    "aws_conn_id": AWS_CONN_ID,
                    "bucket_name": BUCKET_NAME,
                    "object_path": object_path
                }
            )

            fetch_article_htmls_to_bucket_task = PythonOperator(
                task_id=f'fetch_article_htmls_to_bucket__{source_name}',
                python_callable=fetch_article_htmls_to_bucket,
                op_kwargs={
                    "articles": fetch_list_of_todays_news_task.output,
                    "bucket_name": BUCKET_NAME,
                    "aws_conn_id": AWS_CONN_ID,
                    "object_path": object_path
                }
            )
            
            fetch_current_news >> upload_news_json >> fetch_list_of_todays_news_task >> fetch_article_htmls_to_bucket_task

        news_grouped_tasks_list.append(news_grouped_task)

    create_news_bucket >> get_previous_day_task >> news_grouped_tasks_list
