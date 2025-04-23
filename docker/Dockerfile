# Install uv
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile and `pyproject.toml` into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy main package to /app directory
COPY imgw /app/imgw

# Copy main script into /app directory
COPY pipeline.py /app/pipeline.py

# Sync the project
RUN uv sync --frozen

CMD [ "uv", "run", "pipeline.py" ]
