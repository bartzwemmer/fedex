import logging

from pandas import DataFrame
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOG = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# Set the autoBroadcastJoinThreshold large enough to fit the largest dimension table
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1)

def _clean_dates(df: DataFrame, column=str) -> DataFrame:
    """
    Clean a formatting error in date columns.

    param df: The source data that will be updated
    param col: The column name in the source DataFrame that will be updated
    """
    df = df.withColumn(column, F.explode(column)).withColumn(
        column, F.regexp_replace(column, "Â ", "")
    )

    return df


def _read_xml(tag: str) -> DataFrame:
    """
    Read XML files from DBMS
    https://kontext.tech/article/369/read-xml-files-in-pyspark
    """
    return (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", tag)
        .load("file:/xmls/*.xml")
    )


def _create_dim_df(df: DataFrame, dimension: str) -> DataFrame:
    """
    Create a dimension DataFrame

    param df: The source data that will be used for creating the dimension
    param dimension: The column name and name used for the dimention. Possible values: date, coverage
    """
    date_dim = df[dimension].drop_duplicates().reset_index(drop=True)
    date_dim.loc[:, f"{dimension}_key"] = 0

    # Add keys
    df_index = date_dim.select("*").withColumn(
        f"{dimension}_id", F.monotonically_increasing_id()
    )

    return df_index


def _create_rel_fact(
    fact_df: DataFrame, date_df: DataFrame, coverage_df: DataFrame
) -> DataFrame:
    """
    Update the facts DataFrame with the reliation keys
    It is wise to only include the columns you really need, this might get expensive
    """
    return (
        fact_df.join(date_df, ["date"])
        .join(coverage_df, ["coverage"])
        .select(
            date_df["date_id"],
            coverage_df["coverage_id"],
            fact_df["creator"],
            fact_df["description"],
            fact_df["format"],
            fact_df["identifier"],
            fact_df["language"],
            fact_df["publisher"],
            fact_df["rights"],
            fact_df["subject"],
            fact_df["title"],
            fact_df["type"],
        )
    )


def _load_data(tag: str) -> None:
    """
    Load the downloaded data, clean it up, transform to a start model en write to delta tables
    """
    LOG.info(f'Start loading data for {tag}.')
    df = _read_xml(tag)

    df = _clean_dates(df=df, column="date")

    # Create dimension tables based on data
    if tag == "oai_dc":
        df_date_dim = _create_dim_df(df=df, dimension="date")
        df_date_dim.write.format("delta").mode("append").saveAsTable("dim_date")
        LOG.info(f'Date dimension table created.')
        
        df_coverage_dim = _create_dim_df(df=df, dimension="coverage")
        df_coverage_dim.write.format("delta").mode("append").saveAsTable("dim_coverage")
        LOG.info(f'Coverage dimension table created.')

        df_facts = _create_rel_fact(df, df_date_dim, df_coverage_dim)
        df_facts.write.format("delta").mode("append").saveAsTable("fact_object")
        LOG.info(f'Fact table with objects created.')

    elif tag == "header":
        df.write.format("delta").mode("append").saveAsTable("fact_header")
        LOG.info(f'Fact table with headers created.')

    else:
        raise ValueError(f"Received unknow tag type {tag}.")


def create_bronze_tables(tags_rdd: RDD) -> None:
    """
    Create Delta Tables from XML files
    For each allows parallel processing
    """
    tags_rdd.foreach(_load_data)


if __name__ == "__main__":
    tags = ["oai_dc", "header"]
    tags_rdd = sc.parallelize(tags)
    create_bronze_tables(tags=tags_rdd)
