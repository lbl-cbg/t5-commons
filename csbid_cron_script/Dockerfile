
FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY .env .env
COPY . .

#CMD [ "python", "./my.py" ]
ENTRYPOINT ["sh", "start.sh"]
#ENTRYPOINT ["tail", "-f", "/dev/null"] 