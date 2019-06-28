from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext, SparkConf

from classes import MongoDB
from classes import DataExplore


if __name__ == '__main__':
    """ Connect Spark to Mongo editing SparkSession"""
    #url = "mongodb://milo:cyt12@127.0.0.1:27017/foursquare.searchVeanues?authSource=admin"
    #my_spark = SparkSession\
    #            .builder\
    #            .appName('test_connection')\
    #            .config("spark.mongodb.input.uri", str(url) ) \
    #            .config("spark.mongodb.output.uri", str(url)) \
    #            .getOrCreate()

    #df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    """ Connect Spark to Mongo, setting up SparkConfig.setAll() options:
        for more complete options check site:
        'https://spark.apache.org/docs/latest/configuration.html'
    """
    config = SparkConf().setAll([('spark.master', 'local[*]'),
                                 ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.0')])

    sc = SparkContext(conf=config)
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://milo:cyt12@127.0.0.1:27017/?authSource=admin") \
        .option("database", "foursquare") \
        .option("collection", "searchVeanues") \
        .load()

    print(df.count())
    df.printSchema()

    """ Connect to Mongo with pymongo 
        Developed in class MongoDB
    """
    MDB = MongoDB.Mongo()
    client = MDB.connectMongoClient('127.0.0.1:27017', 'admin')
    COL = client['foursquare']['searchVeanues']
    print(type(COL))
    print(COL.count())
    Query = COL.find({"categories.name ": "Taco Place"})
    print(Query.count())

    """ Load Mongo Collection to Pandas"""
    df_pd = DataExplore.GroupByCategory.load_list_toPandas(list(Query))
    print('Pandas count : \n', df_pd.count())
    print('Columns : \n', df_pd.columns.tolist())

