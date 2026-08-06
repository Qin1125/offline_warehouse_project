# ============================================================
# 广电用户画像系统 - 完整 Pipeline
# 功能：特征工程 + SVM建模 + 8维度标签生成
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LinearSVC
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# ---------- 1. 初始化 Spark ----------
spark = SparkSession.builder.appName("UserProfile").enableHiveSupport().getOrCreate()

# ---------- 2. 特征工程 ----------
# 2.1 月均消费
df_spend = spark.table("mmconsume_billevents_clean") \
    .withColumn("net_pay", col("should_pay").cast("double") - col("favour_fee").cast("double")) \
    .groupBy("phone_no") \
    .agg(avg("net_pay").alias("avg_spend"))

# 2.2 入网时长
df_tenure = spark.table("mediamatch_usermsg_clean") \
    .withColumn("tenure", datediff(current_date(), col("open_time").cast("timestamp")) / 365.25)

# 2.3 日均观看时长
df_watch = spark.table("media_index_clean") \
    .groupBy("phone_no") \
    .agg((sum(col("duration").cast("double")) / 3600000 / 365.25).alias("avg_daily_hours")) \
    .withColumn("avg_daily_hours", coalesce(col("avg_daily_hours"), lit(0.0)))

# 合并特征
df_features = df_tenure.select("phone_no", "tenure") \
    .join(df_spend, on="phone_no", how="left") \
    .join(df_watch, on="phone_no", how="left") \
    .fillna({"avg_spend": 0.0, "avg_daily_hours": 0.0, "tenure": 0.0})

# 标签列
df_status = spark.table("mediamatch_usermsg_clean").select("phone_no", "run_name")
df_labeled = df_features.join(df_status, on="phone_no", how="inner") \
    .withColumn("label",
                when((col("run_name").isin("主动暂停", "主动销户")), 0.0)
                .when((col("run_name") == "正常") & (col("avg_daily_hours") > 0), 1.0)
                .otherwise(-1.0)) \
    .filter(col("label") >= 0)

# ---------- 3. 模型训练 ----------
feature_cols = ["avg_spend", "tenure", "avg_daily_hours"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
svm = LinearSVC(labelCol="label", featuresCol="features", maxIter=50, regParam=0.01)
pipeline = Pipeline(stages=[assembler, scaler, svm])

train_data, test_data = df_labeled.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_data)

# 评估
predictions = model.transform(test_data)
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / predictions.count()
print("准确率 (Accuracy): {:.4f}".format(accuracy))

evaluator_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auroc = evaluator_roc.evaluate(predictions)
print("AUROC: {:.4f}".format(auroc))

evaluator_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
auprc = evaluator_pr.evaluate(predictions)
print("AUPRC: {:.4f}".format(auprc))

# 预测所有用户
df_all_features = df_features.join(df_status, on="phone_no", how="inner")
df_all_pred = model.transform(df_all_features)
df_result = df_all_pred.select("phone_no", col("prediction").alias("label"))
df_result.write.mode("overwrite").saveAsTable("svm_prediction")

# ---------- 4. 8维度用户画像标签 ----------
# 4.1 消费内容标签
df_content = spark.table("mmconsume_billevents_clean") \
    .select("phone_no", "fee_code") \
    .distinct() \
    .withColumn("content_label",
                when(col("fee_code").isin("0J", "0B", "OY"), "直播")
                .when(col("fee_code") == "0X", "应用")
                .when(col("fee_code") == "0T", "付费频道")
                .when(col("fee_code").isin("0W", "0L", "0Z", "0K"), "宽带")
                .when(col("fee_code") == "0D", "点播")
                .when(col("fee_code") == "0H", "回看")
                .when(col("fee_code") == "0U", "有线电视收视费")
                .otherwise("其他"))
window_content = Window.partitionBy("phone_no").orderBy("fee_code")
df_content_final = df_content.withColumn("rn", row_number().over(window_content)) \
    .filter(col("rn") == 1) \
    .select("phone_no", "content_label")

# 4.2 电视消费水平
df_tv_spend = spark.table("mmconsume_billevents_clean") \
    .filter(~col("sm_name").rlike("珠江宽频")) \
    .withColumn("net_pay", col("should_pay").cast("double") - col("favour_fee").cast("double")) \
    .groupBy("phone_no") \
    .agg(avg("net_pay").alias("avg_tv_spend"))
df_tv_level = df_tv_spend.withColumn("tv_spend_label",
                                     when(col("avg_tv_spend") < 26.3, "电视超低消费")
                                     .when((col("avg_tv_spend") >= 26.3) & (col("avg_tv_spend") < 46.3), "电视低消费")
                                     .when((col("avg_tv_spend") >= 46.3) & (col("avg_tv_spend") < 66.3), "电视中等消费")
                                     .when(col("avg_tv_spend") >= 66.3, "电视高消费")
                                     .otherwise("未知")) \
    .select("phone_no", "tv_spend_label")

# 4.3 宽带消费水平
df_bb_spend = spark.table("mmconsume_billevents_clean") \
    .filter(col("sm_name").rlike("珠江宽频")) \
    .withColumn("net_pay", col("should_pay").cast("double") - col("favour_fee").cast("double")) \
    .groupBy("phone_no") \
    .agg(avg("net_pay").alias("avg_bb_spend"))
df_bb_level = df_bb_spend.withColumn("bb_spend_label",
                                     when(col("avg_bb_spend") <= 29, "宽带低消费")
                                     .when((col("avg_bb_spend") > 29) & (col("avg_bb_spend") <= 48), "宽带中消费")
                                     .when(col("avg_bb_spend") > 48, "宽带高消费")
                                     .otherwise("无宽带消费")) \
    .select("phone_no", "bb_spend_label")

# 4.4 业务品牌
df_brand = spark.table("mediamatch_usermsg_clean") \
    .select("phone_no", "sm_name") \
    .distinct() \
    .filter(~col("sm_name").isin("模拟有线电视", "番通")) \
    .withColumn("brand_label",
                when(col("sm_name") == "互动电视", "互动电视")
                .when(col("sm_name") == "数字电视", "数字电视")
                .when(col("sm_name") == "甜果电视", "甜果电视")
                .when(col("sm_name") == "珠江宽频", "珠江宽频")
                .otherwise("其他")) \
    .select("phone_no", "brand_label")

# 4.5 电视入网程度
df_tv_tenure = spark.table("mediamatch_usermsg_clean") \
    .filter(~col("sm_name").rlike("珠江宽频")) \
    .withColumn("tenure", datediff(current_date(), col("open_time").cast("timestamp")) / 365.25) \
    .withColumn("tv_tenure_label",
                when(col("tenure") <= 9, "新用户")
                .when((col("tenure") > 9) & (col("tenure") <= 13), "中等用户")
                .when(col("tenure") > 13, "老用户")
                .otherwise("未知")) \
    .select("phone_no", "tv_tenure_label")

# 4.6 宽带入网程度
df_bb_tenure = spark.table("mediamatch_usermsg_clean") \
    .filter(col("sm_name").rlike("珠江宽频")) \
    .filter(col("force") == "宽带生效") \
    .filter(col("sm_code") == "b0") \
    .withColumn("tenure", datediff(current_date(), col("open_time").cast("timestamp")) / 365.25) \
    .withColumn("bb_tenure_label",
                when(col("tenure") <= 8, "新用户")
                .when((col("tenure") > 8) & (col("tenure") <= 14), "中等用户")
                .when(col("tenure") > 14, "老用户")
                .otherwise("未知")) \
    .select("phone_no", "bb_tenure_label")

# 4.7 挽留标签（来自SVM预测）
df_svm = spark.table("svm_prediction") \
    .withColumn("retain_label",
                when(col("label") == 1.0, "挽留用户")
                .when(col("label") == 0.0, "不挽留用户")
                .otherwise("未知")) \
    .select("phone_no", "retain_label")

# 4.8 主销售品名称
df_order_valid = spark.table("order_index_clean") \
    .filter(col("cost").cast("double") > 0) \
    .filter(col("offername").isNotNull() & (col("offername") != "空包")) \
    .withColumn("effdate", col("effdate").cast("timestamp")) \
    .withColumn("expdate", col("expdate").cast("timestamp")) \
    .withColumn("optdate", col("optdate").cast("timestamp")) \
    .withColumn("current_ts", current_date().cast("timestamp"))

window_order = Window.partitionBy("phone_no").orderBy(col("optdate").desc())
df_order_best = df_order_valid \
    .filter((col("effdate") <= col("current_ts")) & (col("expdate") >= col("current_ts"))) \
    .filter((col("mode_time") == "Y") & (col("offertype") == "0") & (col("prodstatus") == "YY")) \
    .withColumn("rn", row_number().over(window_order)) \
    .filter(col("rn") == 1) \
    .select("phone_no", col("offername").alias("product_label"))

# ---------- 5. 合并所有标签为宽表 ----------
df_users = spark.table("mediamatch_usermsg_clean").select("phone_no").distinct()
df_profile = df_users \
    .join(df_content_final, on="phone_no", how="left") \
    .join(df_tv_level, on="phone_no", how="left") \
    .join(df_bb_level, on="phone_no", how="left") \
    .join(df_brand, on="phone_no", how="left") \
    .join(df_tv_tenure, on="phone_no", how="left") \
    .join(df_bb_tenure, on="phone_no", how="left") \
    .join(df_svm, on="phone_no", how="left") \
    .join(df_order_best, on="phone_no", how="left") \
    .fillna({
        "content_label": "无消费",
        "tv_spend_label": "无电视消费",
        "bb_spend_label": "无宽带消费",
        "brand_label": "未知品牌",
        "tv_tenure_label": "未知",
        "bb_tenure_label": "无宽带",
        "retain_label": "未知",
        "product_label": "无有效产品"
    })

# ---------- 6. 写入 Hive ----------
df_profile.write.mode("overwrite").saveAsTable("user_profile_final")
print("用户画像宽表已生成：user_profile_final")