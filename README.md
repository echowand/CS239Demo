Clean and build package:
```
sbt clean; sbt package
```

Run:
```
spark-submit --class  "LogisticRegressionDemo" --master "local[4]" target/scala-2.11/cs239-demo_2.11-1.0.jar > out.txt 
spark-submit --class  "DecisionTreeClassificationDemo" --master "local[4]" target/scala-2.11/cs239-demo_2.11-1.0.jar > out1.txt
spark-submit --class  "DecisionTreeRegressionDemo" --master "local[4]" target/scala-2.11/cs239-demo_2.11-1.0.jar > out2.txt
spark-submit --class  "KMeansDemo" --master "local[4]" target/scala-2.11/cs239-demo_2.11-1.0.jar > outKmeans.txt
```

Data:
The input click data is stored in a CSV file, structured as follows:
```
EventID,CustID,AdClicked,Localtime
0,109,"ADV_FREE_REFERRAL","2014-12-18 08:15:16"
```

Transforming it to a training file called *features.txt* that contains all of the points for training one or more classifiers, which will serve as input into prediction about which customers are most likely to click on the ad based on their past behaviors. The 0 or 1 at the beginning is our label and indicates if this customer clicked on the particular ad, and the remainder of the line lists the values for each feature. [label, morn, aft, eve, night, mobile]
```
0 1:0.31 2:0.25 3:0.16 4:0.28 5:0.53 6:0.93
1 1:0.35 2:0.15 3:0.24 4:0.26 5:0.85 6:0.92
0 1:0.27 2:0.21 3:0.25 4:0.27 5:0.52 6:0.98
```

```
for k, v in custdata.collect():
    unique, morn, aft, eve, night, mobile = v
    tot = float(morn + aft + eve + night)

    # see if this user clicked on a 1-day special reduced Gold rate
    clicked = 1 if sortedclicks.lookup(k)[0] > 0 else 0
   
   # write a row of training data, starting with the target value
    training_row = [
                morn / tot,
                aft / tot,
                eve / tot,
                night / tot,
                mobile / tot ]
    trainfile.write("%d" % clicked)

    # write the individual features
    # the libSVM format wants features to start with 1
    for i in range(1, len(training_row) + 1):
        trainfile.write(" %d:%.2f" % (i, training_row[i - 1]))
    trainfile.write("\n")
```



References:
Some code and data are from these websites: 
- [Apache Spark](https://spark.apache.org/docs/latest/ml-guide.html)
- [MapR Demo](https://github.com/mapr/mapr-demos)
- [MapR Tutorial](https://www.mapr.com/blog/classifying-customers-mllib-and-spark)
