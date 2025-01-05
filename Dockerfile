FROM python:3.9.9-slim

RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip3 install -r requirements.txt

CMD ["python3", "-u", "/app/NR_KPI_Monitor_WEB.py"]