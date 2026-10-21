# Python and Java are pinned to the exact runtime builds verified in Phase 1.
FROM python:3.11.14-slim-bookworm@sha256:65a93d69fa75478d554f4ad27c85c1e69fa184956261b4301ebaf6dbb0a3543d

ARG JAVA_PACKAGE_VERSION=17.0.20.1+1-1~deb12u1

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      "openjdk-17-jre-headless=${JAVA_PACKAGE_VERSION}" make procps \
    && rm -rf /var/lib/apt/lists/*

# Debian uses a different JAVA_HOME directory on amd64 than arm64.
RUN java_home="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -sfn "$java_home" /opt/java-home
ENV JAVA_HOME=/opt/java-home

WORKDIR /opt/project
COPY pyproject.toml ./
# Keep the 318 MB PySpark layer reusable when application code changes.
RUN python -m pip install --no-cache-dir \
      setuptools==80.9.0 \
      boto3==1.43.116 \
      pyspark==3.5.9 \
      pytest==9.1.1 \
      pytest-cov==7.0.0 \
      ruff==0.17.1
COPY README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation .
COPY tests ./tests
COPY scripts ./scripts
COPY docs ./docs
COPY sql ./sql
RUN mkdir -p data

WORKDIR /opt/project
CMD ["bash"]
