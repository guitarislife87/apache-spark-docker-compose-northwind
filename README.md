# Adam's Apache Playground

Started as learning how to run Apache Spark, Airflow, and Superset and what it can do. Expanded into practicing data warehousing with the Northwind Database.

## Get Started

```
cd jars
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
cd ..
docker compose up -d
./run-northwind-analysis.sh
```

## Resources

[Apache Spark + Docker Compose Tutorial](https://clynt.com/blog/data-engineering/apache-spark/apache-spark-with-docker#quick-start-single-node-spark-cluster)

[Northwind Seed Data](https://github.com/pthom/northwind_psql/blob/master/northwind.sql)
