/usr/hdp/current/spark2-client/bin/spark-submit\
    --master yarn\
    --deploy-mode client\
    --num-executors 8\
    --driver-memory 16g\
    --executor-memory 8g\
    --executor-cores 2\
    --conf spark.yarn.maxAppAttempts=1\
    --conf spark.driver.maxResultSize=8g\
    --archives env.tar.gz#ARCHIVE\
    <local_path>/script.py