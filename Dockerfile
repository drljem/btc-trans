FROM python:3.8

WORKDIR /app

ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY btc_price.py .
COPY upload_to_s3.py .

CMD ["sh", "-c", "python btc_price.py & pid1=$! && python upload_to_s3.py & pid2=$! && while kill -0 $pid1 $pid2 > /dev/null 2>&1; do wait; done"]



