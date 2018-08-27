import sys
import time
import random
import json
import traceback
import pyodbc
import telepot
from telepot.loop import MessageLoop
from telepot.delegate import per_chat_id, create_open, pave_event_space
import argparse
import json
from azure.storage.blob import BlockBlobService

"""
$ python3.5 guess.py <token>
Guess a number:
1. Send the bot anything to start a game.
2. The bot randomly picks an integer between 0-99.
3. You make a guess.
4. The bot tells you to go higher or lower.
5. Repeat step 3 and 4, until guess is correct.
"""

class Crawler(telepot.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(Crawler, self).__init__(*args, **kwargs)
        self._answer = random.randint(0,99)

    def _get_dev_env(self):
        ##TODO ReadFrom File config
        #fl = json.load()
        return args

    def _get_prod_env(self):
        #TODO ReadFrom File config to support multiple environments
        return

    def open(self, initial_msg, seed):
        self.sender.sendMessage('type RunDEV if you whant to run Azure blob Crowler on Dev DWH ')
        return True  # prevent on_message() from being called on the initial message

    def on_chat_message(self, msg):
        content_type, chat_type, chat_id = telepot.glance(msg)

        if 'RunDEV' == msg['text']:
            self.sender.sendMessage('Im starting...')
            try:
                args = self._get_dev_env()
                self._runBlobCrawlerv2()
            except Exception as err:
                self.sender.sendMessage("UUPPPS... ERROR: {0}".format(err))
            return

        try:
           guess = int(msg['text'])
        except ValueError:
            self.sender.sendMessage('Give me a command, please.')
            return


    def _createOrRepalceExternalTablev2(self,cursor, aName, aKey, container, tablename, azureblobfoder, columnlist):

        # TODO add check for Storage procedure existing and it is not exists create it from file

        sql = "exec CreateOrRepalceExternalTablev2 @StorageAccountName=?" \
              ", @StorageAccountKey=?" \
              ", @ContainerName=?" \
              ", @TableName=?" \
              ", @AzureBlobFolder=?" \
              ", @ColumnList=?" \
              ", @FileFormat=NULL"

        params = (aName, aKey, container, tablename, azureblobfoder, columnlist)
        try:
            cursor.execute(sql, params)
        except pyodbc.Error as err:
            print(err)
            raise

    def _defineColumnsv2(self,columnlist):
        res = ''
        for column in columnlist:
            if 'DateTime' in column["ColumnType"]:
                res = res + '[' + column["ColumnName"] + '] DATETIME, '
            elif 'Double' in column["ColumnType"]:
                res = res + '[' + column["ColumnName"] + '] Decimal, '
            elif 'Boolean' in column["ColumnType"]:
                res = res + '[' + column["ColumnName"] + '] Bit, '
            else:
                res = res + '[' + column["ColumnName"] + '] VARCHAR(500), '

        print('-------- ' + res[:-2])
        return res[:-2]

    def _runBlobCrawlerv2(self):

        block_blob_service = BlockBlobService(account_name=args.accountName, account_key=args.accountKey)
        containers = block_blob_service.list_containers()
        conn = pyodbc.connect(driver="{ODBC Driver 13 for SQL Server}", server=args.dbserver, database=args.dbname,
                              uid=args.dbuid,
                              pwd=args.dbpwd, autocommit=True)
        cursor = conn.cursor()
        for container in containers:
            MSG = []
            MSG.append("Container processing: " + container.name)
            generator = block_blob_service.list_blobs(container.name)
            ## TODO  add external  data source for blob container
            i=0
            for blob in generator:
                i=i+1
                #self.sender.sendMessage('     ' + blob.name)
                if '.str2' in blob.name:
                    struct = block_blob_service.get_blob_to_text(container.name, blob.name)
                    #self.sender.sendMessage('Structure File: ' + struct.content)
                    ##TODO add table generator for Azure SQL DWH
                    metadata = json.loads(struct.content)
                    for table in metadata["Tables"]:
                        #self.sender.sendMessage("External Table: " + table["TableName"])
                        tablename = table["TableName"]
                        catalog = table["AzureBlobFolder"]
                        if "Colums" in table:
                            columns = table["Colums"]
                        if "Columns" in table:
                            columns = table["Columns"]
                        try:
                            self._createOrRepalceExternalTablev2(cursor, args.accountName, args.accountKey, container.name
                                                       , tablename.lower()
                                                       , catalog.lower()
                                                       , self._defineColumnsv2(columns))
                            MSG.append("External Table: " + table["TableName"] + " created")
                        except Exception as err:
                            self.sender.sendMessage("UUPPPS...{0} ERROR: {1}".format(table["TableName"],err))

            MSG.append("Container {0}  has {1} files".format(container.name, i))
            MSGsend = MSG
            if len(MSG)>5:
                MSGsend = MSG[0:4]
                MSGsend.append("Container {0}  has {1} files".format(container.name, i))

            self.sender.sendMessage(MSGsend)

        conn.close()
        self.sender.sendMessage("ALL DONE")

    def on__idle(self, event):
        self.sender.sendMessage('Time expired.')
        self.close()

global args

parser = argparse.ArgumentParser()

parser.add_argument("--accountName", help=" accountName")
parser.add_argument("--accountKey", help=" accountKey")
parser.add_argument("--dbserver", help=" dbserver")
parser.add_argument("--dbname", help=" dbname")
parser.add_argument("--dbuid", help=" dbuid")
parser.add_argument("--dbpwd", help=" dbpwd")
parser.add_argument("--tltoken", help=" telgramToken")
args = parser.parse_args()

TOKEN = args.tltoken

bot = telepot.DelegatorBot(TOKEN, [
    pave_event_space()(
        per_chat_id(), create_open, Crawler, timeout=3600),
])
MessageLoop(bot).run_as_thread()
print('Listening ...')

while 1:
    time.sleep(10)