FROM python:3.10-slim-bookworm

# Install megatools and system dependencies
RUN apt-get update && apt-get install -y \
    megatools \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["bash", "start.sh"]
