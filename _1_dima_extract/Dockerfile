# python ubuntu image
FROM python:3.8.12-slim-buster

RUN echo "America/Denver" > /etc/timezone
RUN dpkg-reconfigure -f noninteractive tzdata

# install linux dependencies
RUN apt-get update && \
    apt-get install postgresql \
            postgresql-contrib \
            unixodbc-dev \
            libpq-dev \
            g++ \
            default-jre \
            # added mdbtools
            mdbtools \
            bash -y && \
            pip3 install --upgrade pip

# prepare environment
RUN mkdir /dimas
COPY dimas/ /dimas/

# Copy the shell script into the container
COPY ./extract.sh /usr/local/bin/extract.sh

# Make the shell script executable + removing windows line endings
RUN chmod +x /usr/local/bin/extract.sh
RUN sed -i -e 's/\r$//' ./usr/local/bin/extract.sh

# default command to run the shell script
CMD ["/usr/local/bin/extract.sh"]
