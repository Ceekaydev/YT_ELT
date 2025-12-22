import logging

logger =  logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):

    try:

        if schema == 'staging':

            video_id = 'video_id' # Different key name in staging schema

            # Insert row
            cur.execute(
                f"""INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s); """,
                row
                
            )

        else:
            
            # Assuming schema is 'production'
            video_id = 'video_id' # Different key name in production schema

            # Insert row
            cur.execute(
                f"""INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_type", "Video_Views", "Likes_Count", "Comments_Count", "Title_Sentiment", "Sentiment_Score")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(video_type)s, %(viewCount)s, %(likeCount)s, %(commentCount)s, %(title_sentiment)s, %(sentiment_score)s); """,
                row
            )

        # Commit transaction
        conn.commit()

        logger.info(f"Inserted row for Video ID: {row.get(video_id)} into {schema}.{table}")

    except Exception as e:
        logger.error(f"Error inserting row for Video ID: {row.get(video_id)} - {e}")
        # Rollback transaction
        conn.rollback()
        raise


    return None


def update_rows(cur, conn, schema, row):

    try:


        if schema == 'staging':
            video_id = 'video_id'

            cur.execute(
                f"""UPDATE {schema}.{table}
                    SET "Video_Title" = %(title)s,
                        "Video_Views" = %(viewCount)s,
                        "Likes_Count" = %(likeCount)s,
                        "Comments_Count" = %(commentCount)s
                    WHERE "Video_ID" = %(video_id)s
                      AND "Upload_Date" = %(publishedAt)s;
                """,
                row
            )

        else:
            video_id = 'video_id'

            cur.execute(
                f"""UPDATE {schema}.{table}
                    SET "Video_Title" = %(title)s,
                        "Video_Views" = %(viewCount)s,
                        "Likes_Count" = %(likeCount)s,
                        "Comments_Count" = %(commentCount)s,
                        "Title_Sentiment" = %(title_sentiment)s,
                        "Sentiment_Score" = %(sentiment_score)s
                    WHERE "Video_ID" = %(video_id)s
                      AND "Upload_Date" = %(publishedAt)s;
                """,
                row
            )

        conn.commit()
        logger.info(f"Updated row for Video ID: {row.get(video_id)} in {schema}.{table}")

    except Exception as e:
        logger.error(f"Error updating row for Video ID: {row.get(video_id)} - {e}")
        conn.rollback()
        raise

    return None


def delete_rows(cur, conn, schema, id_to_delete):

    try:

        # Create placeholders for the list of IDs
        placeholders = ', '.join(['%s'] * len(id_to_delete)) 

        # Delete rows
        query = f"""DELETE FROM {schema}.{table}
                    WHERE "Video_ID" = ANY(%s);""" # Using ANY for list of IDs

        cur.execute(
            query,
            (id_to_delete,)
        )

        conn.commit()
        logger.info(f"Deleted rows for Video IDs: {id_to_delete} from {schema}.{table}")

    except Exception as e:
        logger.error(f"Error deleting rows for Video IDs: {id_to_delete} - {e}")
        conn.rollback()
        raise

    return None
