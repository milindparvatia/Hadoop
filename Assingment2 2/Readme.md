# Assignment2 Part 2 Scala Mevan Project

## Cmds used to execute .jar

In these cmds, the input and outputs paths are used as arguments passed at the cmd line arg(1), arg(2).

###Task1.1 Pair :

> spark-submit --class streaming.NetworkWordCount --master yarn --deploy-mode client Assingment2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/milind/input hdfs:///user/milind/output/subtaskA hdfs:///user/milind/output/subtaskB hdfs:///user/milind/output/subtaskC

Notes: 

1) I have used arg(0) as input and arg(1),arg(2),arg(3) as outputs respectively.
2) I have put 20 seconds delay in streaming so that input can process and we can download output before Hadoop rewrite new stream.
3) I have also provided my outputs in archive just incase.
4) I have added my saveTextfile function without overwrite but it has error.

