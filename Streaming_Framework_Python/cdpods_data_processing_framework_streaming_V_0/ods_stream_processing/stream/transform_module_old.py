from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_data_structure(df: DataFrame):
    """
    Transform the data into required structure.
    
    :param df: DataFrame containing the data to be transformed.
    """

    # TODO - THE BELOW LINES OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    df = (
        df.select("MessageHeader.*", "MessageBody.*")
    )
    df = (
        df.select(
            "*",
            "AccArr.*",
            "Branch.*",
        ).drop("AccArr", "Branch")
    )
    return df


def transform_data(existing_df: DataFrame, new_df: DataFrame):
    """
    Write data to Pheonix table with UPSERT, using lookup for ID.
    
    :param: existing_df: DataFrame containing the existing data in the pheonix table.
    :param: new_df: DataFrame containing the new data to be written.
    """

    # TODO - THE BELOW LINES OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    target_columns = existing_df.columns

    join_condition = F.expr("""
                             ACNT_ID_NUM = CASE 
                             WHEN OriSou = 'BKK'
                             THEN concat('BKKG', '^', AccNum, '^', NSC)
                             END
                             """)
    
    # TODO - WE CANNOT INCLUDE THE BELOW CONDITION SINCE
    # THE SeqNum IS SOME RANDOM DIGIT AT THE MOMENT,
    # HENCE POSSIBILITY OF RETURNING NULL RECORDS AFTER JOIN.
    # CAN BE PART OF NEXT SPRINT OR WHEN PROPER DATA IS POPULATED.

    # join_condition = expr("""
    #       ACNT_ID_NUM = CASE
    #           WHEN OriSou = 'BKK'
    #           THEN concat('BKKG', '^', AccNum, '^', NSC)
    #       END
    #       AND
    #       CASE 
    #           WHEN BalanceStatus = 'SHADOW'
    #               THEN Timestamp > SHDW_BAL_DTTM
    #               AND SeqNum > SHDW_BAL_SEQ
    #           WHEN BalanceStatus = 'POSTED'
    #               THEN Timestamp > LDGR_BAL_DTTM
    #               AND SeqNum > LDGR_BAL_SEQ
    #       END
    # """)

    final_df = new_df.join(
        existing_df,
        join_condition,
        "left"
    )

    final_df = (
        final_df
        .withColumn("ACNT_ROLE_UPDT_SEQ", F.lit(None).cast("string"))
        .withColumn("ACNT_ROLE_UPDT_DTTM", F.lit(None).cast("timestamp"))
        .withColumn("SHDW_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("Balance")
                    ).otherwise(F.lit(None).cast("decimal(23,4)")))
        .withColumn("SHDW_BAL_DTTM",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("Timestamp")
                    ).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("SHDW_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("SeqNum")
                    ).otherwise(F.lit(None).cast("string")))
        .withColumn("LDGR_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("Balance")
                    ).otherwise(F.lit(None).cast("decimal(23,4)")))
        .withColumn("LDGR_BAL_DTTM",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("Timestamp")
                    ).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("LDGR_BAL_SEQ",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("SeqNum")
                    ).otherwise(F.lit(None).cast("string")))
    ).select(target_columns)

    return final_df