# IMPORT DEPENDENCIES
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import explode, udf, split, explode, countDistinct
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row, SparkSession
from pyspark import *

import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection

import pyspark
import itertools
import datetime

def get_cooccurences(allnames_string):
    '''
    Map function to get a list of entity pairs(Cartesian Product) from an
    AllNames column string.
    '''
    entities = allnames_string.split(';')
    entitites_names = [row.split(',', 2)[0] for row in entities]
    cooccurences = itertools.product(entitites_names, entitites_names)
    cooccurences_list= map(list, cooccurences)
    return cooccurences_list

def get_top_mentions(gkg_df, col):
    '''
    Returns an ordered dataframe of named entity count corresponding to an
    arbitrary column.
    '''
    log.warn("GETTING COUNTS AND ORDERED DATAFRAME FOR {} COLUMN".format(col))
    themes = gkg_df.select(col)
    # split_text = udf(lambda x: x.split(';'))
    themes_split_col = pyspark.sql.functions.split(gkg_df[col], ';')
    themes_df = gkg_df.select(explode(themes_split_col).alias(col))
    theme_counts = themes.groupBy(col).count().orderBy('count', ascending=False)
    log.warn("FINISHED GETTING COUNTS AND ORDERED DATAFRAME FOR {} COLUMN".format(col))
    return theme_counts

def query_time():
    'NEED TO ADD CONTINGENCIES FOR 2 DIGIT DATES'
    now = datetime.datetime.now()
    today_seq = '{}0{}0{}'.format(now.year,now.month,now.day)
    return today_seq

def make_allnames_df():
    '''
    Returns a filtered dataframe where rows have multiple named entitites.
    '''
    log.warn("CREATING ALLNAMES DATAFRAME")
    gkg_allnames_df = master_gkg.filter("AllNames LIKE '%;%'").select(master_gkg.AllNames)
    gkg_allnames_df.createOrReplaceTempView('gkg_allnames')

    gkg_allnames_today_df = master_gkg_today.filter("AllNames LIKE '%;%'").select(master_gkg_today.AllNames)
    gkg_allnames_today_df.createOrReplaceTempView('gkg_allnames_today')

    log.warn("FINISHED CREATING ALLNAMES DATAFRAME")
    return gkg_allnames_today_df

def make_cooccurence_df():
    '''
    Creates the Cooccurences dataframe with entity pairs, counts, and weights.
    '''

    log.warn("ABOUT TO CREATE ALLNAMES DATAFRAME")
    gkg_allnames_today_df = make_allnames_df()
    gkg_allnames_today_df.createOrReplaceTempView('gkg_allnames_today')
    log.warn("FINISHED CREATING ALLNAMES DATAFRAME")

    log.warn("CREATING LISTS OF COOCCURENCES DATAFRAME")
    coocs_today_lists = sqlContext.sql('SELECT AllNames, get_cooccurences(gkg_allnames_today.AllNames) AS Cooccurences FROM gkg_allnames_today')

    log.warn("APPLYING EXPLODE FUNCTION TO GET COOCCURENCE DATAFRAME")
    coocs_today_explode = coocs_today_lists.select(explode("Cooccurences").alias("cooccurence"))

    log.warn("GROUPING COOCCURENCE DATAFRAME BY KEYS TO GET UNIQUE COUNTS")
    coocs_counts = coocs_today_explode.groupby(coocs_today_explode.cooccurence).count().alias('Counts')

    log.warn("ORDERING AND FORMATING COOCCURENCE DATAFRAME")
    coocs_counts_ordered = coocs_counts.orderBy('count', ascending=False)
    coocs_counts_ordered.createOrReplaceTempView('coocs_counts_ordered')
    coocs_counts_ordered = sqlContext.sql(
        "SELECT row_number()\
         OVER (ORDER BY count DESC) AS rn, * \
         FROM coocs_counts_ordered")

    log.warn("GETTING THE TOTAL NUMBER OF COOCCURENCES")
    total_counts = coocs_counts_ordered.rdd.map(lambda x: float(x["count"])).reduce(lambda x, y: x+y)

    log.warn("WEIGHTING COOCCURENCE KEYS BY FREQUENCY")
    weights = coocs_counts_ordered.rdd.map(lambda x: x['count'] /total_counts)
    row = Row("weight")
    weights_df = weights.map(row).toDF()
    weights_df.createOrReplaceTempView('weights_df')
    weights_df = sqlContext.sql(
        "SELECT row_number() \
        OVER(ORDER BY weight DESC) AS rn, * \
        FROM weights_df")

    log.warn("APPLYING JOIN OF DF AND WEIGHTS TO FINALIZE COOCCURENCE DF")
    coocs_counts_ordered = coocs_counts_ordered.join(weights_df,
        weights_df.rn == coocs_counts_ordered.rn).drop(weights_df.rn)

    log.warn("FINISHED CREATING COOCCURENCE DATAFRAME")
    return coocs_counts_ordered


if __name__ == '__main__':

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    APP_NAME = "Global Knowledge Graph"
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    spark = SparkSession.builder.appName("APP_NAME") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sc.setLogLevel(logLevel="WARN")
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    log.warn('SETTING COMPRESSION CODEC AS GZIP')
    sqlContext.setConf("spark.sql.parquet.compression.code","gzip")

    log.warn("LOADING TODAY'S PARQUET FILES")
    today = query_time()
    gkg_today = spark.read.parquet("s3a://gdelt-streaming/{}*".format(today))

    gkg_today.createOrReplaceTempView("gkg_today")
    log.warn("CREATING TODAY'S MASTER DATAFRAME")
    master_gkg_today = spark.sql("SELECT GKGRECORDID, cast(DATE AS STRING), AllNames, Persons, Organizations, Themes, Locations FROM gkg_today LIMIT 10")

    master_gkg_today.registerTempTable("master_gkg_today")
    log.warn("FINISHED TODAY'S MASTER DATAFRAME")

    log.warn("LOADING ALL PARQUET FILES")
    gkg = spark.read.parquet("s3a://gdelt-streaming/*.parquet")

    gkg.createOrReplaceTempView("gkg_view")
    log.warn("CREATING ALLTIME MASTER DATAFRAME")
    master_gkg = spark.sql("SELECT GKGRECORDID, cast(DATE AS STRING), AllNames, Persons, Organizations, Themes, Locations FROM gkg_view LIMIT 10")

    master_gkg.registerTempTable('master_gkg')

    log.warn("REGISTERING GET_OCCURENCES SQL FUNCTION")
    pyspark.sql.UDFRegistration(get_cooccurences)
    sqlContext.registerFunction("get_cooccurences", get_cooccurences, ArrayType(ArrayType(StringType())))

    log.warn("FINISHED REGISTERING GET_OCCURENCES SQL FUNCTION")

    log.warn("ABOUT TO CREATE COOCCURENCES DATAFRAME")
    cooccurences = make_cooccurence_df()
    log.warn("UPLOADING CONCURRENCES TO S3")

    cooccurences_string = 'cooccurences_{}'.format(datetime.datetime.now())
    cooccurences.write.parquet("s3a://gdelt-spark-output/{}".format(cooccurences_string))

    themes = get_top_mentions(gkg_today, 'Themes')
    people = get_top_mentions(gkg_today, 'Persons')
    organizations = get_top_mentions(gkg_today, 'Organizations')

    log.warn("UPLOADING Themes, People, and Orgs to S3")
    themes_string = 'themes_{}'.format(datetime.datetime.now())
    themes.write.parquet("s3a://gdelt-spark-output/{}".format(themes_string))

    people_string = 'people_{}'.format(datetime.datetime.now())
    people.write.parquet("s3a://gdelt-spark-output/{}".format(people_string))

    orgs_string = 'organizations_{}'.format(datetime.datetime.now())
    organizations.write.parquet("s3a://gdelt-spark-output/{}".format(orgs_string))

    log.warn("UPLOADING COOCCURENCES, THEMES, PEOPLE, AND ORGS TO S3 AS JSON")
    output_dir = "s3a://gdelt-spark-output/"
    orgs_txt_string = 'organizations_json_{}'.format(datetime.datetime.now())
    organizations.write.json("s3a://gdelt-spark-output/{}".format(orgs_txt_string))

    themes_txt_string = 'themes_json_{}'.format(datetime.datetime.now())
    themes.write.json("s3a://gdelt-spark-output/{}".format(themes_txt_string))

    people_txt_string = 'themes_json_{}'.format(datetime.datetime.now())
    people.write.json("s3a://gdelt-spark-output/{}".format(people_txt_string))

    cooccurences_txt_string = 'cooccurences_json_{}'.format(datetime.datetime.now())
    cooccurences.write.json("s3a://gdelt-spark-output/{}".format(cooccurences_txt_string))

    log.warn("UPLOADING COOCCURENCES, THEMES, PEOPLE, AND ORGS TO REDSHIFT")

    output_dir = "s3a://gdelt-spark-output/"
    cooccurences.write \
        .format("com.databricks.spark.redshift")\
        .option("url", "jdbc:redshift://gdelt-cluster.cmgwgjhrlvjy.us-west-2.redshift.amazonaws.com:5439/gdelt?user={}&password={}".format(redshift_usrname, redshift_password)) \
        .option("dbtable", "cooccurences") \
        .option("tempdir", output_dir) \
        .mode("overwrite") \
        .save()

    themes.write \
        .format("com.databricks.spark.redshift")\
        .option("url", "jdbc:redshift://gdelt-cluster.cmgwgjhrlvjy.us-west-2.redshift.amazonaws.com:5439/gdelt?user={}&password={}".format(redshift_usrname, redshift_password)) \
        .option("dbtable", "themes") \
        .option("tempdir", output_dir) \
        .mode("overwrite") \
        .save()

    people.write \
        .format("com.databricks.spark.redshift")\
        .option("url", "jdbc:redshift://gdelt-cluster.cmgwgjhrlvjy.us-west-2.redshift.amazonaws.com:5439/gdelt?user={}&password={}".format(redshift_usrname, redshift_password)) \
        .option("dbtable", "people") \
        .option("tempdir", output_dir) \
        .mode("overwrite") \
        .save()


    organizations.write \
        .format("com.databricks.spark.redshift")\
        .option("url", "jdbc:redshift://gdelt-cluster.cmgwgjhrlvjy.us-west-2.redshift.amazonaws.com:5439/gdelt?user={}&password={}".format(redshift_usrname, redshift_password)) \
        .option("dbtable", "organizations") \
        .option("tempdir", output_dir) \
        .mode("overwrite") \
        .save()


    log.warn("SUCCESS! EVERYTHING WORKED!")
