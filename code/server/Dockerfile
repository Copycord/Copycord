FROM python:3.13-slim
WORKDIR /app

# install server deps
COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy shared code + server package
COPY common    ./common
COPY commands    ./commands
COPY server    ./server

# make /app importable
ENV PYTHONPATH=/app:${PYTHONPATH}

# run the server package
CMD ["python", "-m", "server.server"]