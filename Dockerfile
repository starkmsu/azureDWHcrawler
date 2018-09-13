#FROM python:3.6
FROM centos/python-36-centos7

RUN yum install -y \
    python-devel \
    python-setuptools \
    gcc-c++ \
    openssl-devel \
    bash

RUN easy_install pip
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y yum install -y msodbcsql
RUN ACCEPT_EULA=Y yum install -y mssql-tools
RUN yum install -y unixODBC-devel

RUN pip install pyodbc

ADD AzureBlobCrawlerBot.py /


#RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
#Download appropriate package for the OS version
#Choose only ONE of the following, corresponding to your OS version
#Debian 8
#RUN curl https://packages.microsoft.com/config/debian/8/prod.list > /etc/apt/sources.list.d/mssql-release.list

#Debian 9
#RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list

#RUN apt-get update
#RUN ACCEPT_EULA=Y apt-get install msodbcsql17
# optional: for bcp and sqlcmd
#RUN ACCEPT_EULA=Y apt-get install mssql-tools
#RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
#RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
#RUN source ~/.bashrc
# optional: for unixODBC development headers
#RUN apt-get update
#RUN apt-get install -y unixodbc unixodbc-dev
#RUN apt-get install python-pyodbc -y
RUN pip install telepot
RUN pip install pyodbc
RUN pip install argparse
RUN pip install azure.storage.blob

ENTRYPOINT [ "python", "./AzureBlobCrawlerBot.py"]