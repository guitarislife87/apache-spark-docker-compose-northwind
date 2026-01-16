docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2G \
  --total-executor-cores 6 \
  /opt/spark-apps/parallel_processing.py

