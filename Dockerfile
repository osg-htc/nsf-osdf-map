# Use Python 3.13 slim image as base
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy all application files
COPY pyproject.toml uv.lock main.py ./

# Install dependencies using uv pip (installs to system Python)
RUN uv pip install --system .

# Create a volume mount point for output files and .env
VOLUME ["/app/output"]

# Set the entrypoint to python with main.py
ENTRYPOINT ["python", "main.py"]

# Default command (shows usage)
CMD []
