# Task 1 – Compute Co-occurrence Matrix

## Converting .gz to .txt

mapReduce was taking very long time so, I have converted the .gz file to txt using this command to speedup processing

```hadoop fs -cat s3a://commoncrawl/crawl-data/CC-MAIN-2018-17/segments/1524125936833.6/wet/CC-MAIN-20180419091546-20180419111546-00036.warc.wet.gz | gzip -d | hadoop fs -put - /user/milind/commoncrawl.txt```


## Cmds used to execute .jar

In these cmds, the input and outputs paths are used as arguments passed at the cmd line arg(1), arg(2).

###Task1.1 Pair :
> hadoop jar Assingment2_Task1-1.0-SNAPSHOT.jar mapReduce.Pairs /user/milind/commoncrawl.txt /user/milind/Output_Pair ~/

###Task1.1 Strip:
```hadoop jar Assingment2_Task1-1.0-SNAPSHOT.jar mapReduce.Stripes /user/milind/commoncrawl.txt /user/milind/output_Strip ~/```

###Task1.2 Pair Relative:
```hadoop jar Assingment2_Task1-1.0-SNAPSHOT.jar mapReduce.PairsRelative /user/milind/commoncrawl.txt /user/milind/output_Pair_Relative ~/```

###Task1.2 Relative:
```hadoop jar Assingment2_Task1-1.0-SNAPSHOT.jar mapReduce.StripesRelative /user/milind/commoncrawl.txt /user/milind/output_Strip_Relative ~/```

Notes: 

1) I was able to test these cods on small file Melbourne, 3-littlepigs, RMIT-1 to test if they works or not.
2) I am limiting tokens with size with less or equal to 100 words in them and discarding all the remaining lines. 

## Performance Analysis

I was only able to run stripes Relative without getting hangged..

| Nodes3 | CPU Miliseconds |
| ------ | ------ |
| Map task- 0002-m_000000 | 3.66e+6 |
| Map task- 0002-m_000001 | 2.82e+6 |
| Map task- 0002-m_000002 | 574200 |
| Reduce task- 0004-r_000000 | 3.66e+6 |
| Reduce task- 0004-r_000001 | 1.5e+6 |
| Reduce task- 0002-r_000002 | 372600 |


### Analysis

The mapReduce was taking quite long time to perform mapping, it took 1 hour to perform this single task and other tasks when run with common crawler input, terminal got hannged.

