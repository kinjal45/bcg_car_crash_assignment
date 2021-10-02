"""
Common utility methods for analysis of car crash
"""

import json
from pyspark.sql import functions as psf
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pkg_resources import resource_string

def read_json(file_name):

    return json.load(open(file_name))

def analytics1(driver_df, gender):
    """
    Unique count of crash id where driver is male
    """
    values = ['MALE', 'FEMALE']

    if not isinstance(driver_df, DataFrame):
        raise TypeError("driver_df is not pyspark dataframe")

    if not isinstance(gender, str):
        raise TypeError("gender is not string")

    if gender not in values:
        raise ValueError("Possible values for gender %s" % (values))

    return driver_df.select("crash_id", 'PRSN_GNDR_ID')\
                    .where(psf.col("PRSN_GNDR_ID") == gender)\
                    .distinct().count()

def analytics2(veh_df, veh_type):
    """
    Crashes which involved motorcycle vehicle type
    """

    if not isinstance(veh_df, DataFrame):
        raise TypeError("veh_df is not pyspark dataframe")

    if not isinstance(veh_type, list):
        raise TypeError("veh_type is not list")

    return veh_df.select("crash_id","VEH_BODY_STYL_ID")\
                     .where(psf.col("VEH_BODY_STYL_ID").isin(veh_type))\
                     .distinct().count()

def analytics3(driver_df, gender):
    """
    returns state which has highest number of accidents
    in which females are involved
    """
    values = ['MALE', 'FEMALE']

    if not isinstance(driver_df, DataFrame):
        raise TypeError("driver_df is not pyspark dataframe")

    if not isinstance(gender, str):
        raise TypeError("gender is not string")

    if gender not in values:
        raise ValueError("Possible values for gender %s" % (values))

    return driver_df.where(psf.col("PRSN_GNDR_ID") == gender) \
            .select("crash_id", 'PRSN_GNDR_ID', 'DRVR_LIC_STATE_ID') \
            .groupBy("DRVR_LIC_STATE_ID")\
            .agg(psf.countDistinct("crash_id").alias("no_of_accidents"))\
            .orderBy(psf.desc("no_of_accidents")).head()['DRVR_LIC_STATE_ID']

def analytics4(veh_df, top_n_vals):
    """
    rank VEH_MAKE_IDs that contribute to a largest number of
    injuries including death and return rows between min and max rank
    """
    if not isinstance(veh_df, DataFrame):
        raise TypeError("veh_df is not pyspark dataframe")

    if not isinstance(top_n_vals, list):
        raise TypeError("top_n_vals is not string")

    if len(top_n_vals) != 2:
        raise ValueError("should consists min and max values")

    w = Window.orderBy("total_injury")
    return veh_df.select("crash_id", "VEH_MAKE_ID", "TOT_INJRY_CNT", "DEATH_CNT")\
                 .withColumn("total_injury", psf.col("DEATH_CNT") + psf.col("TOT_INJRY_CNT")) \
                 .groupBy("VEH_MAKE_ID")\
                 .agg(psf.sum("total_injury").alias("total_injury"))\
                 .withColumn("rank", psf.dense_rank().over(w))\
                 .where((psf.col("rank") >= top_n_vals[0]) & (psf.col("rank") <= top_n_vals[1]))

def analytics5(veh_df, driver_df):
    """
    compute top ethnic user group for each body style
    """
    if not isinstance(veh_df, DataFrame):
        raise TypeError("veh_df is not pyspark dataframe")

    if not isinstance(driver_df, DataFrame):
        raise TypeError("driver_df is not pyspark dataframe")

    veh_df = veh_df.select("crash_id","VEH_BODY_STYL_ID")
    driver_df = driver_df.select("crash_id", 'PRSN_ETHNICITY_ID')

    w1 = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(psf.desc("count"))
    return veh_df.join(driver_df, on='crash_id').groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")\
                 .count()\
                 .withColumn("rank", psf.dense_rank().over(w1)).where("rank=1")\
                 .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').drop_duplicates()

def analytics6(driver_df, top_n_zip_codes, alchol_result):
    """
    top n zip codes with highest number of crashes involved alchol factor to crash
    """

    if not isinstance(driver_df, DataFrame):
        raise TypeError("driver_df is not pyspark dataframe")
    if not isinstance(top_n_zip_codes, int):
            raise TypeError("top_n_zip_codes is not integer type")
    if not isinstance(alchol_result, str):
            raise TypeError("alchol_result is not string type")

    return driver_df.select("crash_id", 'DRVR_ZIP', 'PRSN_ALC_RSLT_ID')\
            .where(psf.col("PRSN_ALC_RSLT_ID") == alchol_result) \
            .na.drop(subset=['DRVR_ZIP']) \
            .groupBy("DRVR_ZIP") \
            .agg(psf.countDistinct("crash_id").alias("cnt")) \
            .orderBy(psf.desc("cnt")).limit(top_n_zip_codes)

def analytics7(damage_df, veh_df):
    """
    Unique count of crash id where no damaged to property observed
    and damage level >= 4 and car has insurance
    """
    if not isinstance(damage_df, DataFrame):
        raise TypeError("damage_df is not pyspark dataframe")
    if not isinstance(veh_df, DataFrame):
        raise TypeError("veh_df is not pyspark dataframe")

    df_no_dmg = damage_df.where(psf.col("DAMAGED_PROPERTY").like("%NO DAMAGE%") | psf.col("DAMAGED_PROPERTY").isNull())
    df = veh_df.select("CRASH_ID","FIN_RESP_TYPE_ID","VEH_DMAG_SCL_1_ID")\
               .where(psf.col("FIN_RESP_TYPE_ID").like(" % INSURANCE % "))\
               .withColumn("damage_rate", psf.regexp_extract("VEH_DMAG_SCL_1_ID", "\\d+", 0)).where(psf.col("damage_rate") >= 4)

    return df_no_dmg.join(df, on='CRASH_ID').select("CRASH_ID").distinct().count()

def analytics8(veh_df, driver_df, charge_df, top_n_state, top_n_colour, top_n_models):
    """
    top n vehicle where licensed driver charged with speeding,
    and uses top 10 vehile colour and in state where top 25 state with highest offences.
    """
    if not isinstance(veh_df, DataFrame):
        raise TypeError("veh_df is not pyspark dataframe")
    if not isinstance(driver_df, DataFrame):
        raise TypeError("driver_df is not pyspark dataframe")
    if not isinstance(charge_df, DataFrame):
        raise TypeError("charge_df is not pyspark dataframe")
    if not isinstance(top_n_state, int):
            raise TypeError("top_n_state is not integer type")
    if not isinstance(top_n_colour, int):
            raise TypeError("top_n_colour is not integer type")
    if not isinstance(top_n_models, int):
            raise TypeError("top_n_models is not integer type")


    driver_with_license = driver_df.select("CRASH_ID","DRVR_LIC_TYPE_ID")\
                                       .where(psf.col("DRVR_LIC_TYPE_ID").like('%DRIVER LIC%'))
    veh_df = veh_df.select("CRASH_ID","VEH_LIC_STATE_ID","VEH_COLOR_ID","VEH_MAKE_ID")\

    df_state = veh_df.join(charge_df, on='CRASH_ID')\
                     .groupBy("VEH_LIC_STATE_ID")\
                     .agg(psf.count("CHARGE").alias("charges"))\
                     .orderBy(psf.desc("charges")).limit(top_n_state)

    df_col = veh_df.groupBy("VEH_COLOR_ID").count()\
                   .orderBy(psf.desc("count")).limit(top_n_colour)

    return charge_df.where(psf.col("CHARGE").like("%SPEED%"))\
                    .join(driver_with_license, on='CRASH_ID')\
                    .join(veh_df, on='CRASH_ID')\
                    .join(df_col.select("VEH_COLOR_ID"), on='VEH_COLOR_ID')\
                    .join(df_state, on='VEH_LIC_STATE_ID')\
                    .groupBy("VEH_MAKE_ID").count()\
                    .orderBy(psf.desc("count")).limit(top_n_models)
