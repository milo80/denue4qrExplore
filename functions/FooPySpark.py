from pyspark.sql.types import *
import pyspark.sql.functions as f


# Crates column with new numbered  index over DataFrame input
def with_column_index(sdf):
    new_schema = StructType(sdf.schema.fields + [StructField("explode", LongType(), False)])
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)


"""
    UDF Functions
    DataExplore Foursquare
"""


# Filters by keywords con avenue address street
# returns : Boolean
def contains_keyword_udf(S):
    def contain(A):
        flagLoop = True
        if str(A[1]) in str(A[0][0]):
            return False
        for item in S:
            if item.lower() not in str(A[0][0]).lower():
                flagLoop = False
                break

        return flagLoop

    return f.udf(lambda x: contain(x), BooleanType())


# Maps a dictionary of colors for each category as key
# Requires dictionary of colors as input
# return color_dictionary
def color_category_udf(keys_dict):
    def color_category(Category):
        if str(Category) in keys_dict:
            return keys_dict[Category]
        else:
            return keys_dict['Other']

    return f.udf(color_category, StringType())

"""
    UDF Functions
    Data Explore, Denue-Inegi
"""


# Filters by address Keword and Federal Entity
# Note: last KWrds item must be Federal Entity
def contains_keyword_and_entity_udf(KWords):
    def contains(DCols):
        flag = True
        for item in KWords[:-1]:
            if item == '':
                flag = True

            elif item.lower() not in str(DCols[0]).lower():
                flag = False
                break
            if flag is True and KWords[-1] != '':
                if str(KWords[-1]).lower() not in str(DCols[1]).lower():
                    flag = False
                    break

        return flag

    return f.udf(lambda x: contains(x), BooleanType())



# Reduce name of denue category
def reduce_name_of_category_udf(RedDict):
    def reduce(Category):
        S = str(Category[0])
        for key in RedDict.keys():
            if key in str(Category[0]):
                S = RedDict[key]
                break

        return S
    return f.udf(lambda x: reduce(x), StringType())


def filter_column_udf(KeyWords):
    def filter_column(COLS):
        Flag = False
        for key in KeyWords:
            for cols in COLS:
                if str(key).lower() in str(cols).lower():
                    Flag = True
                    break
            if Flag:
                break

        return Flag

    return f.udf(lambda x: filter_column(x), BooleanType())
