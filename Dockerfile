from python:3

ENV AMPLITUDE_KEY=
ENV AMPLITUDE_SECRET=
ENV DB_NAME=
ENV DB_USER=
ENV DB_PASS=
ENV DB_HOST=
ENV DB_PORT=

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD [ "python", "/app/main.py" ]
