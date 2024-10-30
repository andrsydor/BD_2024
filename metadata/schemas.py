from pyspark.sql.types import StringType, IntegerType, StructType, StructField, TimestampType

from metadata.columns import StackExchangeColumns


posts_schema = StructType([
    StructField(StackExchangeColumns.id, IntegerType()),
    StructField(StackExchangeColumns.post_type_id, IntegerType()),
    StructField(StackExchangeColumns.accepted_answer_id, IntegerType()),
    StructField(StackExchangeColumns.parent_id, IntegerType()),
    StructField(StackExchangeColumns.creation_date, TimestampType()),
    StructField(StackExchangeColumns.deletion_date, TimestampType()),
    StructField(StackExchangeColumns.score, IntegerType()),
    StructField(StackExchangeColumns.view_count, IntegerType()),
    StructField(StackExchangeColumns.body, StringType()),
    StructField(StackExchangeColumns.owner_user_id, IntegerType()),
    StructField(StackExchangeColumns.owner_display_name, StringType()),
    StructField(StackExchangeColumns.last_editor_user_id, IntegerType()),
    StructField(StackExchangeColumns.last_editor_display_name, StringType()),
    StructField(StackExchangeColumns.last_edit_date, TimestampType()),
    StructField(StackExchangeColumns.last_activity_date, TimestampType()),
    StructField(StackExchangeColumns.title, StringType()),
    StructField(StackExchangeColumns.tags, StringType()),  # initial type, will be changed during transforms
    StructField(StackExchangeColumns.answer_count, IntegerType()),
    StructField(StackExchangeColumns.comment_count, IntegerType()),
    StructField(StackExchangeColumns.favorite_count, IntegerType()),
    StructField(StackExchangeColumns.closed_date, TimestampType()),
    StructField(StackExchangeColumns.community_owned_date, TimestampType()),
    StructField(StackExchangeColumns.content_license, StringType()),
])

comments_schema = StructType([
    StructField(StackExchangeColumns.id, IntegerType()),
    StructField(StackExchangeColumns.post_id, IntegerType()),
    StructField(StackExchangeColumns.score, IntegerType()),
    StructField(StackExchangeColumns.text, StringType()),
    StructField(StackExchangeColumns.creation_date, TimestampType()),
    StructField(StackExchangeColumns.user_display_name, StringType()),
    StructField(StackExchangeColumns.user_id, IntegerType()),
    StructField(StackExchangeColumns.content_license, StringType()),
])

tag_synonyms_schema = StructType([
    StructField(StackExchangeColumns.id, IntegerType()),
    StructField(StackExchangeColumns.source_tag_name, StringType()),
    StructField(StackExchangeColumns.target_tag_name, StringType()),
    StructField(StackExchangeColumns.creation_date, TimestampType()),
    StructField(StackExchangeColumns.owner_user_id, IntegerType()),
    StructField(StackExchangeColumns.auto_rename_count, IntegerType()),
    StructField(StackExchangeColumns.last_auto_rename, TimestampType()),
    StructField(StackExchangeColumns.score, IntegerType()),
    StructField(StackExchangeColumns.approved_by_user_id, IntegerType()),
    StructField(StackExchangeColumns.approval_date, TimestampType()),
])

tags_schema = StructType([
    StructField(StackExchangeColumns.id, IntegerType()),
    StructField(StackExchangeColumns.tag_name, StringType()),
    StructField(StackExchangeColumns.count, IntegerType()),
    StructField(StackExchangeColumns.excerpt_post_id, IntegerType()),
    StructField(StackExchangeColumns.wiki_post_id, IntegerType()),
    StructField(StackExchangeColumns.is_moderator_only, StringType()),
    StructField(StackExchangeColumns.is_required, StringType()),
])
