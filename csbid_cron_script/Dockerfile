
FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY .env .env
COPY . .
RUN mkdir -p /logs
RUN current_date=$(date +%Y-%m-%d) && \
    touch /logs/${current_date}.log
RUN mkdir -p /temp_data
#CMD [ "python", "./my.py" ]
ENTRYPOINT ["sh", "start.sh"]
#ENTRYPOINT ["tail", "-f", "/dev/null"] 