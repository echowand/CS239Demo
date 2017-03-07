Clean and build package:
```
sbt clean; sbt package
```

Run:
```
spark-submit --class  "LogisticRegressionDemo" --master "local[4]" target/scala-2.11/simple-project_2.11-1.0.jar > out.txt
spark-submit --class  "DecisionTreeClassificationDemo" --master "local[4]" target/scala-2.11/simple-project_2.11-1.0.jar > out1.txt
spark-submit --class  "DecisionTreeRegressionDemo" --master "local[4]" target/scala-2.11/simple-project_2.11-1.0.jar > out2.txt
spark-submit --class  "KMeansDemo" --master "local[4]" target/scala-2.11/simple-project_2.11-1.0.jar > outKmeans.txt
```

References:
[Apache Spark](https://github.com/apache/spark)
[MapR Demo](https://github.com/mapr/mapr-demos)
