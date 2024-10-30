from pyspark.sql import functions as f

from metadata.columns import StackExchangeColumns as columns


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
