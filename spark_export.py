import findspark
findspark.init('/mnt/nfs/spark')
findspark.find()
 
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window, HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import requests, os, sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

conf = pyspark.SparkConf().setAll([
('spark.ui.port', '3778'),
('spark.executor.memory', '512m'),
('spark.driver.extraClassPath', 'ojdbc7.jar'),
('spark.executor.extraClassPath', 'ojdbc7.jar'),
('spark.sql.catalogImplementation', 'hive'),
('spark.sql.parquet.writeLegacyFormat', 'True'),
('spark.sql.sources.partitionOverwriteMode', 'dynamic')
])

master = 'mesos://zk://mesosserver:mesosport/mesos'

conf.setMaster(master)
sc = pyspark.SparkContext(appName='Spark-Export', conf=conf)
spark = pyspark.sql.SparkSession.builder.master(master).config(conf=conf).enableHiveSupport().getOrCreate()
 
sc
spark

dbase = str(sys.argv[1])
hdfs_table = str(sys.argv[2])
table_ora = str(sys.argv[3])
start_date = sys.argv[4]

try:
    end_date = sys.argv[5]
except:
    end_date = start_date

if dbase.lower() == 'databasename1':
    dependence = 'database1 dependence'
    url = 'jdbc:oracle:thin:@test.example.com:port/servicename'
elif dbase.lower() == 'databasename2':
    dependence = 'database2 dependence'
    url = 'jdbc:oracle:thin:@test.example.com:port/servicename2'    

token = os.environ['token']
 
payload = requests.get(f'api with passwords/?token={token}&dependence={dependence}').json()
username, password = payload['username'], payload['password']

if start_date.find(']') != -1:
    start_date = start_date.replace("'", "").strip("[]").split(',')
    
if type(start_date) == list:
    for date in start_date:
        df = spark.sql(f"select * from schema.{hdfs_table} where parition_name = '{date}'").drop('partition_name')
        df.write\
            .mode('append')\
            .format('jdbc')\
            .option('url', url)\
            .option('batchsize', 5000)\
            .option('dbtable', f"{table_ora}")\
            .option('user', username)\
            .option('password', password)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver')\
            .save()
        print('Finished date:', date)
elif type(start_date) == str:
    start_t = datetime.strptime(start_date, '%Y%m%d')
    end_t = datetime.strptime(end_date, '%Y%m%d')
    while start_t <= end_t:
        dt = start_t.strftime('%Y%m%d')
        df = spark.sql(f"select * from schema.{hdfs_table} where parition_name = '{dt}'").drop('parition_name')
        df.write\
            .mode('append')\
            .format('jdbc')\
            .option('url', url)\
            .option('batchsize', 5000)\
            .option('dbtable', f"{table_ora}")\
            .option('user', username)\
            .option('password', password)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver')\
            .save()
        print('Finished date:', dt)
        start_t = start_t + relativedelta(days=1)

spark.stop()
