FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml requirements.txt ./
RUN pip install --upgrade pip && \
    pip install .

COPY pipeline ./pipeline
COPY server ./server

EXPOSE 8000

CMD ["uvicorn", "server.api:api", "--host", "0.0.0.0", "--port", "1032"]
