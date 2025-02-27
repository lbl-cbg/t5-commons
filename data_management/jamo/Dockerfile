# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables to prevent Python from writing .pyc files and to ensure output is not buffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create and set the working directory
WORKDIR /usr/src/app

# Copy local repositories into the Docker image and install them
COPY lapinpy /usr/src/app/lapinpy
RUN cd lapinpy \
    && pip install .

COPY sdm-common /usr/src/app/sdm-common
RUN cd sdm-common \
    && pip install .

COPY jat /usr/src/app/jat-ui
RUN cd jat-ui \
    && pip install .

COPY jamo /usr/src/app/jamo
RUN cd jamo \
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
