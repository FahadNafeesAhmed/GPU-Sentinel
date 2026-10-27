# One image for both roles: `sentinel worker ...` and `sentinel controller ...`.
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# docker CLI so the controller can restart sibling containers through the mounted socket
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

COPY pyproject.toml README.md ./
COPY sentinel ./sentinel
COPY bench ./bench
COPY config ./config
RUN pip install . && mkdir -p /app/data

EXPOSE 8080 8090 9001
HEALTHCHECK --interval=5s --timeout=2s --start-period=10s --retries=3 \
  CMD python -c "import sys,urllib.request,os; p=os.environ.get('HEALTH_PORT','9001'); urllib.request.urlopen(f'http://127.0.0.1:{p}/healthz' if p in ('8090',) else f'http://127.0.0.1:{p}/health', timeout=1.5)" || exit 1

ENTRYPOINT ["python", "-m", "sentinel"]
CMD ["--help"]
