FROM apache/airflow:2.9.0

# Install system dependencies (Chromium & Driver untuk Scraping)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    fonts-freefont-ttf \
    xfonts-encodings \
    libxss1 \
    libappindicator3-1 \
    libgconf-2-4 \
    libxinerama1 \
    libxcursor1 \
    libxtst6 \
    libxi6 \
    libx11-6 \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libxslt1.1 \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
