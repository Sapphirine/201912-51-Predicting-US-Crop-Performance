
import subprocess
from datetime import datetime,timezone
import time

from pyspark.sql import Row,SQLContext,SparkSession
from google.cloud import storage
from google.cloud import bigquery

class GCPInterface():
    def __init__(self,project_id,bucket,directory='gs://tmp',dataset='dataset',
                    table='table',columns=['column']):
        
        self.project_id = project_id # "eecs-e6893-edu"
        self.bucket = bucket # "eecs-e6893-edu"    
        self.directory = directory #'gs://{}/hadoop/tmp/bigquery/pyspark_output/nodes'.format(bucket)
        self.dataset = dataset #'hw4_dataset'                     #the name of your dataset in BigQuery
        self.table = table #'nodes'
        self.columns = columns #['node']

    def get_timestamp(self,dt_format="%Y-%m-%d %H:%M:%S",tz=None):
        """
        Return the current time as a string. Default timezone(tz) uses local time. 
        Use tz=timezone.utc to return UTC time
        Args:
            dt_format (str): desired format of returned timestamp (if error, return default format)
            tz (str): desired timezone of timestamp (if error, return UTC time)
        Returns:
            dt_string (str): string representation of current datetime
            dt_now (datetime): datetime object
        """
        try:
            dt_now = datetime.now(tz)
            dt_string = dt_now.strftime(dt_format)
        except:
            dt_now = datetime.now(timezone.utc)
            dt_string = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        
        return dt_string, dt_now

    def saveToBigQuery(self,sc, output_dataset, output_table, directory):
        """
        Put temp streaming json files in google storage to google BigQuery
        and clean the output files in google storage
        """
        files = directory + '/part-*'
        subprocess.check_call(
            'bq load --source_format NEWLINE_DELIMITED_JSON '
            '--replace '
            '--autodetect '
            '{dataset}.{table} {files}'.format(
                dataset=output_dataset, table=output_table, files=files
            ).split())
        output_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
        output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
            output_path, True) 
    
    def saveToStorage(self, rdd, output_directory, columns_name, mode='overwrite'):
        """
        Save each RDD in this DStream to google storage
        Args:
            rdd: input rdd
            output_directory: output directory in google storage
            columns_name: columns name of dataframe
            mode: mode = "overwirte", overwirte the file
                mode = "append", append data to the end of file
        """
        # Format the data should be in
        
        if not rdd.isEmpty():
            (rdd.toDF( columns_name ) \
            .write.save(output_directory, format="json", mode=mode))
        print("Results stored in bucket \'{0}\'...".format(output_directory))
        
    def run(self,data,schema,appname='usheatmap',debug=0):
        # df_sql = spark.sql("SELECT * FROM components WHERE component == {}".format(component))
        
        # assign GC Storage
        gc_client = storage.Client(project=self.project_id) # add project = project_id if needed
        bucket = gc_client.get_bucket(self.bucket)

        # configure Spark
        if debug > 1:
            print("[{}] Start Spark application\n".format(self.get_timestamp()[0]))
        
        spark = SparkSession.builder.appName(appname).getOrCreate()

        # checkpoint set in gs://eecs-e6893-edu/notebooks/jupyter/checkpoints (or just 'checkpoints' for local)
        # spark.sparkContext.setCheckpointDir('checkpoints')
        sc = spark.sparkContext 
        sqlContext = SQLContext(sc)
        # sqlContext = spark.sqlContext
        # create dataframe
        rdd = sc.parallelize(data)
        df = sqlContext.jsonRDD(rdd)
        # print("[{}] Get verticies\n".format(get_timestamp()[0]))
        # vertices = data.map(lambda x: (x[0],))
        print("[{}] Create dataframes\n".format(self.get_timestamp()[0]))
        # df_v = spark.createDataFrame(vertices,["id"])
        # put data into spark RDD form
        row_nodes = Row("node")
        rdd_nodes =  df_sql.select("id").rdd.flatMap(lambda x: x).map(row_nodes)
