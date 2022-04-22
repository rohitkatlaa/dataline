FROM python:3
ENV DATALINE_URL1=http://127.0.0.1:8001
ENV DATALINE_URL2=http://127.0.0.1:8002
ENV SQL_USER=root
ENV SQL_PASSWORD=
ENV SQL_HOST=localhost
RUN mkdir /app
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "manage.py", "runserver", "8001"]