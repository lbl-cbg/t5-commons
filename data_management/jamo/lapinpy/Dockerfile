FROM debian:bullseye

RUN groupadd sdm -g 9004 &&  adduser -u 9004  --gid 9004 sdm && \
    apt update && apt install -y rsync && apt install -y python2.7 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1 && \
    update-alternatives --set python /usr/bin/python2.7 && \
    apt install -y wget && wget https://bootstrap.pypa.io/pip/2.7/get-pip.py && python get-pip.py

# region TODO: Remove ... used for testing
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y net-tools

RUN apt-get update && apt-get install -y lsof

RUN apt-get update && apt-get install -y curl
# endregion 

WORKDIR /lapinpy

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN pip install future

COPY . .

RUN chown -R sdm:sdm .

RUN python setup.py install

USER sdm

#CMD ["lapind", "start", "config"]
