from pyspark.sql.functions import col, lower, when, regexp_replace, to_date, coalesce, lit, mean, count
from pyspark.sql import SparkSession
from pyspark.sql.types import *

data_path = "D:/amazon_reviews/"

review_file_mapping = {
    # "wireless": "amazon_reviews_us_Wireless_v1_00.tsv",
    # "watches": "amazon_reviews_us_Watches_v1_00.tsv",
    # "video": "amazon_reviews_us_Video_v1_00.tsv",
    # "video_games": "amazon_reviews_us_Video_Games_v1_00.tsv",
    # "video_DVD": "amazon_reviews_us_Video_DVD_v1_00.tsv",
    # "toys": "amazon_reviews_us_Toys_v1_00.tsv",
    "tools": "amazon_reviews_us_Tools_v1_00.tsv",
    # "sports": "amazon_reviews_us_Sports_v1_00.tsv",
    # "software": "amazon_reviews_us_Software_v1_00.tsv",
    # "shoes": "amazon_reviews_us_Shoes_v1_00.tsv",
    "pet_products": "amazon_reviews_us_Pet_Products_v1_00.tsv",
    "personal_care_appliances": "amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv",
    # "pc": "amazon_reviews_us_PC_v1_00.tsv",
    # "outdoors": "amazon_reviews_us_Outdoors_v1_00.tsv",
    # "office_products": "amazon_reviews_us_Office_Products_v1_00.tsv",
    # "musical_instruments": "amazon_reviews_us_Musical_Instruments_v1_00.tsv",
    # "music": "amazon_reviews_us_Music_v1_00.tsv",
    # "mobile_electronics": "amazon_reviews_us_Mobile_Electronics_v1_00.tsv",
    # "mobile_apps": "amazon_reviews_us_Mobile_Apps_v1_00.tsv",
    # "major_appliances": "amazon_reviews_us_Major_Appliances_v1_00.tsv",
    # "luggage": "amazon_reviews_us_Luggage_v1_00.tsv",
    # "health_personal_care": "amazon_reviews_us_Health_Personal_Care_v1_00.tsv",
    # "grocery": "amazon_reviews_us_Grocery_v1_00.tsv",
    # "gift_card": "amazon_reviews_us_Gift_Card_v1_00.tsv",
    # "furniture": "amazon_reviews_us_Furniture_v1_00.tsv",
    # "electronics": "amazon_reviews_us_Electronics_v1_00.tsv",
    # "digital_video_games": "amazon_reviews_us_Digital_Video_Games_v1_00.tsv",
    # "digital_software": "amazon_reviews_us_Digital_Software_v1_00.tsv",
    # "digital_music_purchase": "amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv",
    # "digital_ebook_purchase": "amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv",
    "camera": "amazon_reviews_us_Camera_v1_00.tsv",
    # "books": "amazon_reviews_us_Books_v1_00.tsv",
    # "beauty": "amazon_reviews_us_Beauty_v1_00.tsv",
    "baby": "amazon_reviews_us_Baby_v1_00.tsv",
    # "automotive": "amazon_reviews_us_Automotive_v1_00.tsv",
    "apparel": "amazon_reviews_us_Apparel_v1_00.tsv"
}

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyse Reviews") \
    .getOrCreate()

schema = StructType([
    StructField("marketplace", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_parent", StringType(), True),
    StructField("product_title", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("star_rating", IntegerType(), True),
    StructField("helpful_votes", IntegerType(), True),
    StructField("total_votes", IntegerType(), True),
    StructField("vine", StringType(), True),
    StructField("verified_purchase", StringType(), True),
    StructField("review_headline", StringType(), True),
    StructField("review_body", StringType(), True),
    StructField("review_date", StringType(), True)
])


def preprocess_category(file_path, sample_fraction=0.01):
    df_sample = spark.read.csv(file_path, schema=schema, sep="\t", header=True).sample(fraction=sample_fraction)

    df_sample = df_sample.fillna(
        {'product_title': 'Unknown', 'product_category': 'Unknown', 'review_headline': 'No Title'})

    df_sample = df_sample.na.fill(value="None", subset=["vine", "verified_purchase"])

    # numeric fields: -1 = missing data
    df_sample = df_sample.na.fill(value=-1, subset=["helpful_votes", "total_votes"])

    # text fields
    df_sample = df_sample.withColumn('review_headline',
                                     when(col('review_headline').isNull() | (col('review_headline') == ""),
                                          "No Title").otherwise(col('review_headline')))
    df_sample = df_sample.withColumn('review_body', when(col('review_body').isNull() | (col('review_body') == ""),
                                                         "No Content").otherwise(col('review_body')))

    df_sample = df_sample.withColumn('product_title', lower(col('product_title')))
    df_sample = df_sample.withColumn('review_headline', lower(col('review_headline')))
    df_sample = df_sample.withColumn('review_body', lower(col('review_body')))

    df_sample = df_sample.withColumn('review_date', to_date(col('review_date'), 'yyyy-MM-dd'))

    # removing non-ASCII characters
    df_sample = df_sample.withColumn('review_body', regexp_replace(col('review_body'), '[^\\x00-\\x7F]+', ''))
    df_sample = df_sample.withColumn('review_headline', regexp_replace(col('review_headline'), '[^\\x00-\\x7F]+', ''))

    df_sample = df_sample.withColumn('review_date', col('review_date').cast(DateType()))

    df_sample = df_sample.dropDuplicates()

    return df_sample


# sample_df = preprocess_category(data_path + review_file_mapping["camera"])
# sample_df = preprocess_category(data_path + review_file_mapping["giftcards"])
# sample_df = preprocess_category(data_path + review_file_mapping["personal_care_appliances"])
# sample_df.show()


def process_all_categories(review_file_mapping, data_path):
    category_dataframes = {}
    for category, file_name in review_file_mapping.items():
        file_path = data_path + file_name
        print(f"Processing file for category: {category}")

        category_df = preprocess_category(file_path)
        category_df.show(truncate=False)

        category_dataframes[category] = category_df

    return category_dataframes


category_dataframes = process_all_categories(review_file_mapping, data_path)
