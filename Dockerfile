FROM python:3.13

WORKDIR /app

COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["sh", "-c", "python src/main.py ${COLLECTOR_TYPE:-trades}"]