FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y python3.10 python3-pip openjdk-11-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/local/bin/python3

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

ENV PATH=$PATH:$JAVA_HOME/bin

ENV PYSPARK_PYTHON=/usr/bin/python3

ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir --upgrade pip && pip3 install --no-cache-dir -r requirements.txt

COPY EU_Budget_Pipeline_STANDALONE.py /app/

COPY config/ /app/config/

COPY run.sh /app/

COPY data/raw/eu_budget_spending_and_revenue_2000-2023.xlsx /app/data/raw/

COPY data/raw/eu_budget_spending_and_revenue_2000-2022.xlsx /app/data/raw/

RUN mkdir -p /app/data/processed /app/outputs /app/logs

RUN chmod +x /app/run.sh

VOLUME ["/app/outputs"]

CMD ["/bin/bash", "/app/run.sh"]

