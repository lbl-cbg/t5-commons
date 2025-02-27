# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables to prevent Python from writing .pyc files and to ensure output is not buffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install git
RUN apt-get update && apt-get install -y git

# Create and set the working directory
WORKDIR /usr/src/app

# Copy local repositories into the Docker image and install them
COPY lapinpy.git /usr/src/app/jgi-lapinpy
RUN cd jgi-lapinpy \
    && pip install .

COPY sdm-common.git /usr/src/app/jgi-sdm-common
RUN cd jgi-sdm-common \
    && pip install .

COPY jat.git /usr/src/app/jgi-jat-ui
RUN cd jgi-jat-ui \
    && pip install .

COPY jamo.git /usr/src/app/jgi-jamo
RUN cd jgi-jamo \
    && pip install .

# Copy the initialization script
COPY init_and_run.sh /usr/local/bin/init_and_run.sh
RUN chmod +x /usr/local/bin/init_and_run.sh

# Copy the cleaning script
COPY clean.sh /usr/local/bin/clean.sh
RUN chmod +x /usr/local/bin/clean.sh

WORKDIR /var/log

# Define the entrypoint command
ENTRYPOINT ["/usr/local/bin/init_and_run.sh"]

# Define the default command arguments, which can be overridden when running the container
CMD [ "/config" ]
