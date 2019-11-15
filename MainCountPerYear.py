from classes.DataExploreDenue import Denue
import parameters.Globals as G
import pyspark.sql.functions as f
import csv


if __name__ == '__main__':

    spark = Denue('local[*]')

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    Categoria = 'serviciosProfesionales'

    File = 'denue_inegi_54_2018.csv'
    df_ant = spark.load_denue_raw(Path, File)

    File = 'denue_inegi_54_2019.csv'
    df_pos = spark.load_denue_raw(Path, File)

    print('count 2016: ', df_ant.count())
    df_ant.printSchema()

    print('count 2017: ', df_pos.count())
    df_pos.printSchema()
    """
    df_1 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'full_outer').select(df_2015.ID, df_2016.id,
                                                                         df_2015['Razón social'],
                                                                         df_2016['raz_social'])
    print('count full_outer join: ', df_1.count())
    df_2 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'left_outer').select(df_2015.ID, df_2016.id,
                                                                                df_2015['Razón social'],
                                                                                df_2016['raz_social'])
    print('count left_outer join: ', df_2.count())
    df_3 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'right_outer').select(df_2015.ID, df_2016.id,
                                                                                df_2015['Razón social'],
                                                                                df_2016['raz_social'])
    print('count right_outer join: ', df_3.count())
    df_4 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'inner').select(df_2015.ID, df_2016.id,
                                                                                df_2015['Razón social'],
                                                                                df_2016['raz_social'])
    print('count inner join: ', df_4.count())
    df_5 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'cross').select(df_2015.ID, df_2016.id,
                                                                           df_2015['Razón social'],
                                                                           df_2016['raz_social'])
    print('count cross join: ', df_5.count())
    df_6 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'full').select(df_2015.ID, df_2016.id,
                                                                           df_2015['Razón social'],
                                                                           df_2016['raz_social'])
    print('count full join: ', df_6.count())

    df_2 = df_2015.join(df_2016, df_2015.ID == df_2016.id, 'left_outer').select(df_2015.ID, df_2016.id,
                                                                                df_2015['Razón social'],
                                                                                df_2016['raz_social'])
    print('count left_outer join: ', df_2.count())
    """
    df_3 = df_ant.join(df_pos, df_ant.id == df_pos.id, 'full').select(df_ant.id.alias('id_1'),
                                                                          df_pos.id.alias('id_2'),
                                                                          df_ant['raz_social'].alias('raz_social_ant'),
                                                                          df_pos['raz_social'].alias('raz_sicial_pos'),
                                                                          df_ant['fecha_alta'].alias('fecha_alta_ant'),
                                                                          df_pos['fecha_alta'].alias('fecha_alta_pos'))
    print('count full join: ', df_3.count())
    df_3.printSchema()
    df_4 = df_3.select('*').filter( f.col('id_2').isNull() )
    # df_4.show()
    # print(' filtered by null:  ', df_4.show() )
    print(' count id_2 null:  ', df_4.count() )
    # .filter(df_2015.ID.isNotNull() )
    df_5 = df_3.select('*').filter( f.col('id_1').isNull() )
    # print(' filtered by null:  ', df_5.show() )
    print(' count id_1 null:  ', df_5.count() )
