FROM python:3.6
ADD AzureBlobCrawlerBot.py /


RUN apt-get update
RUN apt-get install -y unixodbc unixodbc-dev
RUN apt-get install python-pyodbc -y
RUN pip install telepot
RUN pip install pyodbc
RUN pip install argparse
RUN pip install azure.storage.blob

CMD [ "python", "./AzureBlobCrawlerBot.py --accountName lkedevdwhconvertedblobs --accountKey LL6semSNFdsVT5L87mbZi9btT8/19GSYAH0NzHjQQ5J72lCx0M2dGJIdyd7PIGXrfZJX+jrDjMsvcnnexWz22Q== --dbserver lyk-dwh.database.windows.net --dbname lykkeDW --dbuid lykkedwhadmin --dbpwd Q!W@E#r4t5y6 --tltoken 693327158:AAGG4Fw6NHoqHloFcQCCz8vozTN5-8ANniY" ]