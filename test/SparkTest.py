from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f


if __name__ == '__main__':
    # Levanta servicios Spark SQL
    sc = SparkContext("local[*]")
    sql = SQLContext(sc)

    FsqrPoints = sql.read.json("../data_test/fsquare_browse_format.json")
    FsqrPoints.printSchema()

    print(len(FsqrPoints.columns))
    print(FsqrPoints.count())
    print(type(FsqrPoints['response']['venues'].columns))
    #df = FsqrPoints.select(FsqrPoints['response.venues.id'],FsqrPoints['response.venues.location.city'])
    if FsqrPoints['response.venues'] is not None:
        df = FsqrPoints.select(f.explode(FsqrPoints['response.venues']))
        df.printSchema()
        df_2 = df.select(df['col.id'], df['col.name'], df['col.location'])
        print("df : columns", len(df_2.columns))
        df_2.printSchema()
        print("df : count", df_2.count())
        df_2.show()

    print("Spark ejecutado exitosamente")
