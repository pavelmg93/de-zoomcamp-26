# Dockerizing the Ingestion Script

**[↑ Up](README.md)** | **[← Previous](07-pgadmin.md)** | **[Next →](09-docker-compose.md)**

Let's modify the Dockerfile we created before to include our `ingest_data.py` script:

```dockerfile
# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory inside container
WORKDIR /app

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install all dependencies (pandas, sqlalchemy, psycopg2)
RUN uv sync --locked

# Copy ingestion script
COPY ingest_data.py ingest_data.py 

# Set entry point to run the ingestion script
ENTRYPOINT [ "python", "ingest_data.py" ]
```

**Explanation:**

- `uv sync --locked` installs exact versions from `uv.lock` for reproducibility
- Dependencies (pandas, sqlalchemy, psycopg2) are already in `pyproject.toml`
- Multi-stage build pattern copies uv from official image
- Copying dependency files before code improves Docker layer caching

**Build the Docker Image**

```bash
docker build -t taxi_ingest:v001 .
```

**Run the Containerized Ingestion**

You can drop the table in pgAdmin beforehand if you want, but the script will automatically replace the pre-existing table.

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips_2021_2 \
    --year=2021 \
    --month=2 \
    --chunksize=100000
```

**Important notes:**

* We need to provide the network for Docker to find the Postgres container. It goes before the name of the image.
* Since Postgres is running on a separate container, the host argument will have to point to the container name of Postgres (`pgdatabase`).
* You can drop the table in pgAdmin beforehand if you want, but the script will automatically replace the pre-existing table.

**[↑ Up](README.md)** | **[← Previous](07-pgadmin.md)** | **[Next →](09-docker-compose.md)**
