docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/addon-jars/postgresql-42.7.1.jar \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark/spark-events \
  /opt/spark-apps/db_analysis.py
