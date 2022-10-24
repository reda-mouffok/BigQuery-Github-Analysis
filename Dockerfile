FROM ubuntu:focal

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-arm64
RUN export JAVA_HOME


# Copy the project
RUN mkdir -p /var/Github-Analysis-Project
WORKDIR /var/Github-Analysis-Project
COPY ./ /var/Github-Analysis-Project


# Install Python3
RUN apt-get install -y python3
RUN apt-get install -y pip

# Install Python virtual Environnement
RUN pip install virtualenv

# Install Python packages
RUN pip install -r requirements.txt


# Command at the runtime
CMD ["python3","main.py"]