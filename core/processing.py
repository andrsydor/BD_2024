from pyspark.sql import functions as f

from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from transformers import pipeline

from metadata.columns import StackExchangeColumns as columns
from metadata.time_formats import PARTITION_DATE_FORMAT


def answers_with_question_tags(questions_dataframe, answers_dataframe):
    question_id_column = "question_id"
    tags_and_question_id = questions_dataframe.select(f.col(columns.id).alias(question_id_column), columns.tags)
    parent_and_date = answers_dataframe.select(columns.id, columns.parent_id, columns.creation_date)
    answers_creation_date_and_tags = tags_and_question_id.join(
        parent_and_date, [tags_and_question_id[question_id_column] == parent_and_date[columns.parent_id]], "inner"
    ).select(columns.id, columns.creation_date, columns.tags)
    return answers_creation_date_and_tags


def comments_with_posts_tags(posts_with_tags_dataframe, comments_dataframe):
    posts_with_tags_for_comments = posts_with_tags_dataframe.select(columns.id, columns.tags)
    comments_post_id_and_date = comments_dataframe.select(columns.post_id, columns.creation_date)
    comments_dates_with_tags = posts_with_tags_for_comments.join(
        comments_post_id_and_date,
        [posts_with_tags_for_comments[columns.id] == comments_post_id_and_date[columns.post_id]], "inner"
    ).select(columns.creation_date, columns.tags)
    return comments_dates_with_tags


def explode_tags(dataframe):
    result = dataframe.withColumn(
        columns.tag,
        f.explode(f.array_remove(f.split(f.regexp_replace(columns.tags, "<", ""), ">"), ""))
    ).select(columns.creation_date, columns.tag)
    return result


def clean_from_tag_synonyms(dataframe, tag_synonyms_dataframe):
    source_target = tag_synonyms_dataframe.select(columns.source_tag_name, columns.target_tag_name)
    cleaned_date_and_single_tag = dataframe.join(
        source_target, [dataframe[columns.tag] == source_target[columns.source_tag_name]], 'left'
    ).withColumn(
        columns.tag,
        f.when(~f.col(columns.target_tag_name).isNull(), f.col(columns.target_tag_name)).otherwise(f.col(columns.tag))
    ).select(columns.creation_date, columns.tag)
    return cleaned_date_and_single_tag


def group_by_day_and_count_tags(date_and_single_tag_dataframe, tags_dataframe):
    day_tag_records = date_and_single_tag_dataframe.withColumn(
        columns.date, f.date_format(columns.creation_date, "yyyy-MM-dd")
    ).select(columns.date, columns.tag)

    day_tag_count = (day_tag_records.withColumn(columns.count, f.lit(1))
                     .groupBy(columns.date, columns.tag)
                     .agg(f.sum(columns.count)
                          .alias(columns.count)))

    tags_dataframe_for_join = tags_dataframe.withColumnRenamed(columns.count, "GlobalCount")
    day_id_tag_count_df = day_tag_count.join(
        tags_dataframe_for_join, [day_tag_count[columns.tag] == tags_dataframe_for_join[columns.tag_name]], "inner"
    ).select(columns.date, f.col(columns.id).alias(columns.tag_id), columns.tag, columns.count)

    return day_id_tag_count_df


def tag_usage_per_day(posts_dataframe, comments_dataframe, tag_synonyms_dataframe, tags_dataframe):
    posts_dataframe = posts_dataframe.drop_duplicates()
    posts_dataframe = posts_dataframe.dropna('all')

    question_type_id = 1
    questions_dataframe = posts_dataframe.filter(
        (f.col(columns.post_type_id) == question_type_id) & (~f.col(columns.tags).isNull())
    )

    answer_type_id = 2
    answers_dataframe = posts_dataframe.filter(
        (f.col(columns.post_type_id) == answer_type_id) & (f.col(columns.tags).isNull())
    )

    answers_creation_date_and_tags = answers_with_question_tags(questions_dataframe, answers_dataframe)
    questions_creation_date_and_tags = questions_dataframe.select(columns.id, columns.creation_date, columns.tags)
    posts_with_tags = questions_creation_date_and_tags.union(answers_creation_date_and_tags).distinct()

    comments_dates_with_tags = comments_with_posts_tags(posts_with_tags, comments_dataframe)

    posts_dates_with_tags = posts_with_tags.select(columns.creation_date, columns.tags)
    dates_with_tags = posts_dates_with_tags.union(comments_dates_with_tags).distinct()

    date_and_single_tag_df = explode_tags(dates_with_tags)

    cleaned_date_and_single_tag = clean_from_tag_synonyms(date_and_single_tag_df, tag_synonyms_dataframe)

    day_id_tag_count_df = group_by_day_and_count_tags(cleaned_date_and_single_tag, tags_dataframe)
    return day_id_tag_count_df


def prepare_posts(posts_dataframe):
    posts_dataframe = posts_dataframe.drop_duplicates()
    posts_dataframe = posts_dataframe.dropna('all')

    posts_dataframe = posts_dataframe.select(
        columns.id, columns.post_type_id, columns.parent_id, 
        columns.accepted_answer_id, columns.owner_user_id,
        columns.score, columns.view_count,
        columns.title, columns.body, columns.tags,
        columns.creation_date, columns.closed_date
    ).where(
        ~f.col(columns.owner_user_id).isNull()
    )

    posts_dataframe = posts_dataframe.withColumn(
        columns.body,
        f.regexp_replace(f.regexp_replace(columns.body, "<.*?>|\n", ""), "\s\s+", " ")
    ).withColumn(
        columns.wordsCount,
        f.array_size(f.split(f.col(columns.body), " "))
    )

    return posts_dataframe


def questions_from_posts(posts_dataframe, tag_synonyms_dataframe):
    question_type_id = 1
    questions_dataframe = posts_dataframe.select(
        columns.id, columns.accepted_answer_id, columns.owner_user_id,
        columns.score, columns.view_count,
        columns.tags, columns.wordsCount,
        columns.creation_date, columns.closed_date,
        f.date_format(columns.creation_date, PARTITION_DATE_FORMAT).alias(columns.partition_date)
    ).filter(
        (f.col(columns.post_type_id) == question_type_id) & (~f.col(columns.tags).isNull())
    ).withColumn(
        columns.tags,
        f.array_remove(f.split(f.regexp_replace(columns.tags, "<", ""), ">"), "")
    )

    id_window = Window.partitionBy(columns.id)
    synonyms_dataframe = tag_synonyms_dataframe.select(columns.source_tag_name, columns.target_tag_name)

    exploded_questions_dataframe = questions_dataframe.withColumn(
        columns.tag,
        f.explode(columns.tags)
    )

    questions_with_synonyms = exploded_questions_dataframe.join(
        synonyms_dataframe,
        [exploded_questions_dataframe[columns.tag] == synonyms_dataframe[columns.source_tag_name]],
        'left'
    ).withColumn(
        columns.tag,
        f.when(~f.col(columns.target_tag_name).isNull(), f.col(columns.target_tag_name)).otherwise(f.col(columns.tag))
    ).drop(
        columns.target_tag_name
    ).drop(
        columns.source_tag_name
    ).drop(
        columns.tags
    ).withColumn(
        columns.tags,
        f.collect_set(columns.tag).over(id_window)
    ).drop(
        columns.tag
    ).dropDuplicates(
        [columns.id]
    )

    return questions_with_synonyms


def answers_from_posts(posts_dataframe):
    answer_type_id = 2
    answers_dataframe = posts_dataframe.select(
        columns.id, columns.parent_id, columns.owner_user_id,
        columns.score, columns.wordsCount,
        columns.creation_date,
        f.date_format(columns.creation_date, PARTITION_DATE_FORMAT).alias(columns.partition_date)
    ).filter(
        (f.col(columns.post_type_id) == answer_type_id) & (f.col(columns.tags).isNull())
    )
    return answers_dataframe


def get_sentiment_definer():
    default_pipeline = pipeline("sentiment-analysis")
    tokenizer_kwargs = {"truncation": True}
    sentiment_encodings = {
        "NEGATIVE": 0,
        "POSITIVE": 1
    }

    def define_sentiment(text):
        return sentiment_encodings[str(default_pipeline(text, **tokenizer_kwargs)[0]["label"])]
    return define_sentiment


def prepare_comments(comments_dataframe):

    comments_dataframe = comments_dataframe.withColumn(
        columns.text,
        f.regexp_replace(f.regexp_replace(columns.text, "<.*?>|\n", ""), "\s\s+", " ")
    ).withColumn(
        columns.wordsCount,
        f.array_size(f.split(f.col(columns.text), " "))
    ).where(
        ~f.col(columns.creation_date).isNull()
    )

    define_sentiment_udf = f.udf(get_sentiment_definer(), IntegerType())
    comments_dataframe = comments_dataframe.withColumn(
        columns.sentiment,
        define_sentiment_udf(f.col(columns.text))
    )

    comments_dataframe = comments_dataframe.select(
        columns.id, columns.post_id, 
        columns.score, columns.wordsCount, columns.sentiment,
        f.date_format(columns.creation_date, PARTITION_DATE_FORMAT).alias(columns.partition_date)
    )
    return comments_dataframe
