from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression as MLlibLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from numpy import polyfit
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import numpy as np



def get_spark_session():
    return SparkSession.builder.appName("LINEAR_REGRESSION").getOrCreate()


def read_csv(spark, path):
    return spark.read.format("csv").option("header", "true").option("inferSchema","true").load(path)


def linear_regression_with_numpy(x_data, y_data):

    plt.scatter(x_data, y_data, color='red', s=30)
    plt.xlabel('MONTH_ID')
    plt.ylabel('TOTAL_USERS')
    plt.title('LINEAR REGRESSION')
    p1 = polyfit(x_data, y_data, 1)
    plt.plot(x_data, np.polyval(p1, x_data), 'g-')
    # plt.show()
    return


def linear_regression_with_mllib(original_df):

    processed_df = original_df.select(original_df.MONTH_ID, original_df.TOTAL_USERS.alias('label'))
    # label is dependent variable whose value is predicted by the model

    train, test = processed_df.randomSplit([0.7,0.3])
    # Divide the data into two. One for training the model and the other for testing it

    assembler = VectorAssembler().setInputCols(['MONTH_ID',]).setOutputCol('FEATURES')
    train01 = assembler.transform(train)
    # We only need features and label columns
    train02 = train01.select("features","label")
    # train02.show(truncate=False)

    lr = MLlibLinearRegression()
    model = lr.fit(train02)

    #Testing with dataset extracted from original DF
    test01 = assembler.transform(test)
    test02 = test01.select("features","label")
    test03 = model.transform(test02)
    # test03.show(truncate=False)

    # Erros to measure model accuracy
    evaluator = RegressionEvaluator()
    # print(evaluator.evaluate(test03, {evaluator.metricName: "r2"}))
    # print(evaluator.evaluate(test03, {evaluator.metricName: "mse"}))
    # print(evaluator.evaluate(test03, {evaluator.metricName: "rmse"}))
    # print(evaluator.evaluate(test03, {evaluator.metricName: "mae"}))

    return


def predict_values(processed_df):

    # Another option to get DF column values.
    month_id_array = processed_df.select('MONTH_ID').rdd.flatMap(lambda x: x).collect()
    users_array = processed_df.select('TOTAL_USERS').rdd.flatMap(lambda x: x).collect()

    independet_variable = np.array(month_id_array).reshape((-1,1))
    dependet_variable = np.array(users_array)

    model = LinearRegression()
    model.fit(independet_variable, dependet_variable)
    model = LinearRegression().fit(independet_variable, dependet_variable)
    r_sq = model.score(independet_variable, dependet_variable)
    # print("coefficient of determination: ")
    # print(r_sq)

    months_to_be_predicted= []
    for i in range(13, 25):
        months_to_be_predicted.append(i)

    month_id = np.array(months_to_be_predicted)
    predictions = model.predict(np.array(months_to_be_predicted).reshape((-1,1)))
    plt.bar(month_id, predictions)
    plt.xlabel("MONTH ID")
    plt.ylabel("NUMBER OF USERS")
    plt.show()

    return predictions


def main():

    spark = get_spark_session()

    # DATASET PRE-PROCESSING
    original_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Spark/data_to_be_analyzed/users_per_month.csv")
    processed_df = original_df.drop("MONTH").withColumnRenamed("ID", "MONTH_ID")

    #------------------------------------------------------------------------------------------------
    # PERFORM LINEAR REGRESSION
    month_id_list = processed_df.toPandas()['MONTH_ID'].values.tolist()
    total_users_list = processed_df.toPandas()['TOTAL_USERS'].values.tolist()
    # linear_regression_with_numpy(month_id_list, total_users_list)

    # ------------------------------------------------------------------------------------------------
    # GENERATE LINEAR REGRESSION MODEL TO BE USED FOR PREDICTION
    linear_regression_with_mllib(processed_df)

    # ------------------------------------------------------------------------------------------------
    #PERFORM PREDICTION
    values_predicted = predict_values(processed_df)
    print(type(values_predicted))


if __name__ == '__main__':
    main()