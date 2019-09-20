from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pandas as pd

import parameters.Globals as G
import functions.Utilities as U
import matplotlib.pyplot as plt
from functions.FooPlot import create_dict_colors
from functions.FooPySpark import *

import datetime


class Category:
    """
        Prints out a pie graph with the catogories that are the must
        popular by region
    """
    sc: object
    sql: object
    color ={''}

    #def __init__(self, host_):
    #    # check all options at :
    #    'https://spark.apache.org/docs/latest/configuration.html'
    #    self.host = host_
    #    conf = SparkConf().setAll([('spark.master', self.host),
    #                               ('spark.executor.memory', '4g'),
    #                               ('spark.app.name', 'foursquare mining App'),
    #                               ('spark.executor.cores', '4'),
    #                               ('spark.cores.max', '4'),
    #                               ('spark.driver.memory', '4g')])
    #    self.engine(conf)
    #
    # TODO : Overload constructor with more options for database setup
    def __init__(self, user_, u_pass, ip_port):
        url = 'mongodb://'+user_+':'+u_pass+'@'\
              + str(ip_port)+'/?authSource=admin'
        #
        conf = SparkConf().setAll([('spark.master', 'local[*]'),
                                   ('spark.app.name', 'foursquare Mongo connection App'),
                                   ('spark.mongodb.input.uri', url),
                                   ('spark.mongodb.output.uri', url),
                                   ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')])

        self.sc = SparkContext(conf=conf)
        self.sql = SQLContext(self.sc)

    # def engine(self, conf_: object):
    #    self.sc = SparkContext(conf=conf_)
    #    self.sql = SQLContext(self.sc)

    # Reads json data
    # file_name : string
    def load_json_file(self, file_name) -> object:
        dataVenues = self.sql.read.json(G.SAVE_PATH_4SQR+file_name)
        dataVenues = dataVenues.select(
            f.col('categories.primary'),
            f.col('id'),
            f.col('name'),
            f.col('categories.name').alias('cat_name'),
            f.col('categories.id').alias('cat_id'),
            #f.col('categories.shortName').alias('cat_shortName')
            f.col('location.country'),
            f.col('location.state'),
            f.col('location.formattedAddress'),
            f.col('location.postalCode'),
            f.col('location.lng'),
            f.col('location.lat')
        )
        #dataFsqr.printSchema()

        return dataVenues

    # TODO : When calling .load() connection fails spark-mongo-connector
    # driver fails, possible error : version, invalid jar package
    def load_mongoDB(self, database_, collection_) -> object:
        df = self.sql.read.format("com.mongodb.spark.sql.DefaultSource")\
                          .option("database", database_)\
                          .option("collection", collection_).load()

        df = df.select(
            f.col('categories.primary'),
            f.col('id'),
            f.col('name'),
            f.col('categories.name').alias('cat_name'),
            f.col('categories.id').alias('cat_id'),
            f.col('location.country'),
            f.col('location.state'),
            f.col('location.formattedAddress'),
            f.col('location.postalCode'),
            f.col('location.lng'),
            f.col('location.lat')
        )
        return df

    # Loads mongo with cleaned format for pie plot group
    # returns : DataFrame
    def load_full_collection_mongoDB(self,database_, collection_) -> object:
        df = self.sql.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", database_) \
            .option("collection", collection_).load()

        return df

    # Create new collection in mongoDB
    def write_mongoDB(self, df_A, database_, collection_):
        try:
            df_A.write.format("com.mongodb.spark.sql.DefaultSource")\
                .mode("overwrite")\
                .option("database", database_)\
                .option("collection", collection_).save()
            print('Data frame saved in MongoDB as : %s:%s '
                  % (database_, collection_))
        except Exception as e:
            print('Error loading DataFrame :\n', e)

    @staticmethod
    def load_list_toPandas(collection: list) -> object:
        df = pd.DataFrame(collection)
        #df_a = df[['categories.name',
        #           'id']].rename(columns={'categories.name': 'cat_name', 'id': 'id_i'})
        #df = self.sql.createDataFrame(df)
        #df = self.sql.createDataFrame(list(map(lambda x: Row(words=x), collection)))
        return df

    # Drops duplicates, drop empty category fields
    def clean_categories(self, df_A):
        # Drops repeated ids
        df_A = df_A.drop_duplicates(subset=['id'])
        print(' after drop duplicate : ', df_A.count())
        # Filters
        filter_A = f.size(f.col('cat_name')) != 0
        filter_B = f.col('primary')[0] == True

        df_A = df_A.filter(filter_A & filter_B) \
            .select('*', df_A['cat_name'][0].alias('category'))

        print(' after filter empty category : ', df_A.count())
        return df_A

    # Filter by Categories : drops repeated ids, and empty
    # category fields
    def filter_categories_colored(self, df_A, ValidCategories, ApplyCatFilter):
        # If no filter applied, too may categories blocks view colors
        colors_dict = create_dict_colors(ValidCategories)
        if ApplyCatFilter:
            # Keeps selected Categories
            df_A = df_A.filter(f.col('category').isin(ValidCategories))
            # Create column of colors for categories

        df_A = df_A.withColumn('color_cat',
                               color_category_udf(colors_dict)(f.col('category')))

        return df_A, colors_dict

    # Adds column<municipio> crosssed with PostalCode
    def validate_postalCode_municipio(self, df_A):
        # Load Postal Codes of CDMX
        df_PC = self.sql.read.format("com.databricks.spark.csv") \
            .options(delimiter=',')\
            .options(header='true', inferSchema='true') \
            .load('./data_sample/PostalCodeCDMX.csv')

        # Drop duplicates
        df_PC = df_PC.drop_duplicates(subset=['d_codigo'])
        # Cross PostalCode vs Municipio
        a = df_A.alias('a')
        b = df_PC.alias('b')
        df_A = a.join(b, f.col('a.postalCode') == f.col('b.d_codigo'), 'left')\
            .select([f.col('a.'+xx) for xx in a.columns] +
                    [f.col('b.D_mnpio').alias('municipio'),f.col('b.d_tipo_asenta')])

        # Filter not assigned municipio to PostalCode
        df_A = df_A.filter(f.col('municipio').isNotNull())
        print('after drop <municipio> null values : ', df_A.count())
        return df_A

    # Filer and keyword in address street
    def filter_by_keyword_address(self, df_A, keywords: list) -> object:
        AddressData = ['formattedAddress', 'postalCode']
        df_A = df_A.filter(contains_keyword_udf(keywords)(f.struct(AddressData)))\
            .select('*')
        return df_A

    # Groups by category, all categories or filtered categories
    # returns -> dict : categori vs count
    def group_by_categories_dict(self, df_A,
                            categories_: list, ApplyFilter) -> dict:
        df_A = df_A.groupby('category').count()\
            .sort(f.col('count').desc())
        if ApplyFilter:
            df_A = df_A.filter(f.col('category').isin(categories_))

        CatCountDict = {}
        for item in df_A.collect():
            CatCountDict[item['category']] = item['count']

        # CatCountDict.update([('Other', CountOther)])
        return CatCountDict

    # Counts remaining categories out of MaxCategoryDisplay
    def count_others(self, KeyCat, CountDict):
        CountOther = 0
        for Key in CountDict.keys():
            if Key not in KeyCat:
                CountOther += CountDict[Key]

        return CountOther

    # Plots iterated pie plots by <municipio>
    def plot_pie(self, df):
        df_mnpio = df.groupby('municipio').count()\
            .sort(f.col('count').desc()).toPandas()
        mnpio = df_mnpio['municipio'].values.tolist()
        #mnpio = mnpio[:3]
        N = len(mnpio)
        print(mnpio)
        for i in range(0, N):
            plt.figure(figsize=(15, 12))
            df_plot = df.filter(f.col('municipio') == mnpio[i]).select('*')
            df_plot = df_plot.groupby('category').count().sort(f.col('count').desc())
            #df_plot = df_plot.filter(f.col('count') > 40).select('*')
            df_plot = with_column_index(df_plot)
            df_plot = df_plot.withColumn('explode', f.col('explode')*0.008)
            df_plot = df_plot.toPandas()
            plt.pie(df_plot['count'],
                    explode=df_plot['explode'],
                    labels=df_plot['category'],
                    autopct='%1.1f%%',
                    startangle=0)
            plt.title(str(mnpio[i]))
            plt.show()

    # Maps 4sqr format to OCB DataModel
    # returns : List of dictionary
    @staticmethod
    def maps_to_OCB_dataModel(df: object) -> list:
        print('Mapeando datos a formato OCB')
        D = datetime.datetime.today()
        try:
            ocb_model = df.rdd.map(
                lambda x: {"id": "4sqr-"+str(x['id']),
                           "type": "PointOfInterest",
                           "address": {
                              "type": "StructuredValue",
                              "value": {
                                  "addressCountry": U.cleanChars(x["country"]),
                                  "addressLocality": U.cleanChars(x["formattedAddress"])
                              }
                           },
                           "category": {
                               "type": "Text",
                               "value": [
                                  "1",
                                  U.cleanChars(x["category"]),
                                  x["color_cat"]
                               ]
                           },
                           "dateCreated": {
                              "type": "DateTime",
                              "value": str(D.date())+'T'+str(D.time())+'Z'
                           },
                           "description": {
                              "type": "Text",
                              "value": U.cleanChars(x["category"])
                           },
                           "location": {
                              "type": "geo:json",
                              "value": {
                                  "type": "Point",
                                  "coordinates": [
                                      x["lng"],
                                      x["lat"]
                                  ]
                              }
                           },
                           "name": {
                              "type": "Text",
                              "value": U.cleanChars(x["name"])
                           },
                           "url": {
                              "type": "Text",
                              "value": "no url"
                           }
                           })

            print('maped data ....')
            out = ocb_model.collect()
            print(type(out))

        except Exception as e:
            out = []
            print('Error mapping venues to OCB format :\n', str(e))

        return out
