from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


class SparkConnector:
    def __init__(self, master="local[*]", gcs=False, mongo=False):
        """
        master: master url for spark session. could be local or yarn
        mongo: dictionary with mongo connection information. Its keys are:
            - input_uri: uri for reading from mongo
            - output_uri: uri for writing to mongo
        GCS: dictionary with GCS connection information. Its keys are:
            - keyfile: path to the keyfile
            - bucket: bucket name
            - Jar: path to the jar file
        """
        if mongo:
            self.spark = (
                SparkSession.builder.master(master)
                .appName("mongoSpark")
                .config(
                    "spark.jars.packages",
                    "org.mongodb.spark:mongo-spark-connector_2.12:2.4.0",
                )
                .config("spark.mongodb.input.uri", mongo["input_uri"])
                .config("spark.mongodb.output.uri", mongo["output_uri"])
                .config("spark.network.timeout", "3600s")
                .getOrCreate()
            )
        elif gcs:
            if "jar" not in gcs:
                self.spark = (
                    SparkSession.builder.master(master)
                    .appName("GCSSpark")
                    .config(
                        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        gcs["keyfile"],
                    )
                    .config(
                        "spark.hadoop.fs.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                    )
                    .config(
                        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                    )
                    .config("spark.hadoop.fs.gs.system.bucket", gcs["bucket"])
                    .getOrCreate()
                )
            else:
                self.spark = (
                    SparkSession.builder.master(master)
                    .appName("GCSSpark")
                    .config(
                        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        gcs["keyfile"],
                    )
                    .config("spark.jars", gcs["jar"])
                    .config(
                        "spark.hadoop.fs.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                    )
                    .config(
                        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                    )
                    .config("spark.hadoop.fs.gs.system.bucket", gcs["bucket"])
                    .getOrCreate()
                )

        else:
            self.spark = (
                SparkSession.builder.master(master)
                .appName("NormalSpark")
                .getOrCreate()
            )

    def get_SparkSession(self):
        return self.spark

    def get_data(self, path, format, **kwargs):
        """
        :param path: path to the data (local)
        :param format: format of the data (csv, json, parquet, etc.)
        :param kwargs: additional arguments for the format. Including schema with StructType

        :return: spark dataframe
        """
        if schema:
            return self.spark.read.format(format).schema(schema).load(path)
        else:
            return self.spark.read.format(format).load(path)

    def write_data(self, df, path, format):
        """
        :param df: spark dataframe to write
        :param format: format of the data (csv, json, parquet, etc.)
        :param path: path to the data (local)
        """
        df.write.format(format).save(path)

    def get_data_from_mongo(self, db=None, collection=None):
        """
        :param db: database name. Default is None which will read from the default database
        :param collection: collection name. Default is None which will read from the default collection

        :return: spark dataframe
        """
        if db and collection:
            return (
                self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
                .option("database", db)
                .option("collection", collection)
                .load()
            )
        else:
            return self.spark.read.format(
                "com.mongodb.spark.sql.DefaultSource"
            ).load()

    def write_data_to_mongo(self, df, db=None, collection=None):
        """
        :param df: spark dataframe to write
        :param db: database name. Default is None which will write to the default database
        :param collection: collection name. Default is None which will write to the default collection
        """
        if db and collection:
            df.write.format("com.mongodb.spark.sql.DefaultSource").option(
                "database", db
            ).option("collection", collection).save()
        else:
            df.write.format("com.mongodb.spark.sql.DefaultSource").save()
