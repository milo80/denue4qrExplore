from classes.DataExploreDenue import Denue
import parameters.Globals as G
import csv


if __name__ == '__main__':

    spark = Denue('local[*]')

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    Categoria = 'serviciosProfesionales'

    File = 'denue_inegi_54_2015.csv'
    df_2015 = spark.load_denue_raw(Path, File)

    File = 'denue_inegi_54_2016.csv'
    df_2016 = spark.load_denue_raw(Path, File)

    print('count 2015: ', df_2015.count())
    # df_2015.printSchema()

    print('count 2016: ', df_2016.count())
    # df_2016.printSchema()

    df_1 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'full_outer').select(df_2015.ID, df_2016.ID,
                                                                         df_2015['Razón social'],
                                                                         df_2016['Razón social'])
    print('count full_outer join: ', df_1.count())
    df_2 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'left_outer').select(df_2015.ID, df_2016.ID,
                                                                                df_2015['Razón social'],
                                                                                df_2016['Razón social'])
    print('count left_outer join: ', df_2.count())
    df_3 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'right_outer').select(df_2015.ID, df_2016.ID,
                                                                                df_2015['Razón social'],
                                                                                df_2016['Razón social'])
    print('count right_outer join: ', df_3.count())
    df_4 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'inner').select(df_2015.ID, df_2016.ID,
                                                                                df_2015['Razón social'],
                                                                                df_2016['Razón social'])
    print('count inner join: ', df_4.count())
    df_5 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'cross').select(df_2015.ID, df_2016.ID,
                                                                           df_2015['Razón social'],
                                                                           df_2016['Razón social'])
    print('count cross join: ', df_5.count())
    df_6 = df_2015.join(df_2016, df_2015.ID == df_2016.ID, 'full').select(df_2015.ID, df_2016.ID,
                                                                           df_2015['Razón social'],
                                                                           df_2016['Razón social'])
    print('count full join: ', df_6.count())
    # print(df.show(60))
    # .filter(df_2015.ID.isNotNull() )
