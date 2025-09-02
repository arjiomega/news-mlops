

```bash
docker build -t fastapi-app:latest .
docker run -d -p 8000:8000 fastapi-app:latest
```

```bash
docker run -it \
  --name fastapi-dev \
  -v $(pwd):/app \
  -w /app \
  -p 8000:8000 \
  python:3.12-slim \
  bash
```

```bash
fastapi dev app/main.py --host 0.0.0.0 --port 8000
```