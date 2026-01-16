docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/sales_analysis.py
ls -la spark-data/output/
cat spark-data/output/product_sales/part-*.csv

