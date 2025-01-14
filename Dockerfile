from python:3

ENV AMPLITUDE_KEY=9314f6f12a26e6a5989287b16d030c30
ENV AMPLITUDE_SECRET=b47c1ce817448cb8fa6cc6f624c695d9
DB_NAME=dw002
DB_USER=stitch
DB_PASS=xHD5TG2UU0Tx
DB_HOST=dw002.cka3r3gokfjc.sa-east-1.rds.amazonaws.com
DB_PORT=5432

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD [ "python", "/app/main.py" ]
