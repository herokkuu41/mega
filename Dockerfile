FROM python:3.10-slim-bookworm

# Install megatools, git, and ca-certificates (crucial for valid HTTPS/CURL connections)
RUN apt-get update && apt-get install -y \
    megatools \
    git \
    ca-certificates \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["bash", "start.sh"]
