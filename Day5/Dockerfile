FROM python:3.12-slim
WORKDIR /ex05
RUN pip install confluent_kafka dotenv fastapi fastapi[standard] dotenv
COPY ex05-async.py .
CMD ["fastapi", "run", "ex05-async.py"]