FROM python:3.10-slim

WORKDIR /etl

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY . .

ENTRYPOINT ["sh", "-c", "sleep 5 && python3 main.py"]