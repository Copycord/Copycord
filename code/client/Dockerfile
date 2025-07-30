FROM python:3.13-slim
WORKDIR /app

# install client deps
COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy shared code + client package
COPY common    ./common
COPY client    ./client

ENV PYTHONPATH=/app:${PYTHONPATH}
CMD ["python", "-m", "client.client"]