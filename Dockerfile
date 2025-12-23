# syntax=docker/dockerfile:1.4

# ============================================
# Award Archive - Data Ingestion Pipeline
# Multi-stage build with uv for fast dependency management
# ============================================

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# ============================================
# Stage 1: Install dependencies
# ============================================
FROM base AS deps

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock* ./

# Install dependencies (without dev dependencies for production)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# ============================================
# Stage 2: Production image
# ============================================
FROM base AS runtime

# Copy virtual environment from deps stage
COPY --from=deps /app/.venv /app/.venv

# Copy application code
COPY src/ src/
COPY pyproject.toml README.md ./

# Install the package
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"

# Default command
CMD ["award-archive", "--help"]

# ============================================
# Stage 3: Development image
# ============================================
FROM base AS dev

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install all dependencies including dev
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Copy application code
COPY . .

# Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"

CMD ["pytest"]

# ============================================
# Stage 4: Streamlit App
# ============================================
FROM base AS streamlit

COPY pyproject.toml uv.lock* ./

# Install with app dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra app --no-install-project

COPY src/ src/
COPY pyproject.toml README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra app

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "src/award_archive/app/explorer.py", "--server.port=8501", "--server.address=0.0.0.0"]
