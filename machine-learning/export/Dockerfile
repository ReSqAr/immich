FROM mambaorg/micromamba:2.0.4-debian12@sha256:a88c9206ec8cea36ef25f180e46fcf11b34652847796e10d49a0a6c96511a979

ENV TRANSFORMERS_CACHE=/cache \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  PATH="/opt/venv/bin:$PATH" \
  PYTHONPATH=/usr/src

USER root
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /cache && chown $MAMBA_USER:$MAMBA_USER /cache
USER $MAMBA_USER

COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
    micromamba clean --all --yes

WORKDIR /usr/src/app

COPY --chown=$MAMBA_USER:$MAMBA_USER run.py .
COPY --chown=$MAMBA_USER:$MAMBA_USER models/ ./models/

ARG MAMBA_DOCKERFILE_ACTIVATE=1

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python"]
CMD ["/usr/src/app/run.py"]
