FROM python:3

# Silence annoying warning
ENV PYTHONWARNINGS ignore::UserWarning:psycopg2

WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir .
