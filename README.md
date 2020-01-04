# FMDB-Project

## Before Starting

1. Compile the code via ```sbt clean package```
2. Execute the code in bluewordextraction subproject (make sure that the paths in the file are correct):
    ```
    $SPARK_HOME/spark-submit --class "ConvertBWToDF" target/scala-2.11/graphxtest_2.11-0.1.jar
    ```
3. Move the resulting json-file to ```data``` and rename it to ```bluewords.json```
4. Execute code in ```ConvertBluewordsToGraphDataframe``` (make sure that path variable ```FM1920HOME``` is set correctly)
5. Rename the json-files in ```data/nodes``` and ```data/edges``` to ```nodes.json``` and ```edge.json``` respectively
6. Use the files to execute experiments on a Graph, i.e. by using the following command (adapt the paths in each file respectively):
    ```
    $SPARK_HOME/spark-submit --class "Graphxtest" --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 target/scala-2.11/graphxtest_2.11-0.1.jar
    ```