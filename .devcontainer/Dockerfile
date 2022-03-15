#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM python:3.6

# Copy default endpoint specific user settings overrides into container to specify Python path
COPY .devcontainer/settings.vscode.json /root/.vscode-remote/data/Machine/settings.json

ENV PYTHONUNBUFFERED 1

# Install git, process tools
RUN apt-get update && apt-get -y install git procps fish postgresql-client default-jre 
#RUN apt-get -y install python3-tables
RUN wget http://ftp.us.debian.org/debian/pool/main/m/mysql-transitional/mysql-client_5.5.9999+default_amd64.deb 
RUN mkdir /workspace
WORKDIR /workspace

# Install pylint
RUN pip install pylint pipenv
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc |  apt-key add -
RUN add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/18.04/mssql-server-2019.list)"
RUN ACCEPT_EULA=y DEBIAN_FRONTEND=noninteractive \
        apt-get install -y --no-install-recommends mssql-tools unixodbc-dev
 
# Install Python dependencies from requirements.txt if it exists
COPY requirements.txt requirements.txt
RUN if [ -f "requirements.txt" ]; then pip install -r requirements.txt; rm requirements.txt; fi
 
# Clean up
RUN apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*
