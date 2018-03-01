#### Spark Data Source V2 example

This example is intended to demonstrate typical interactions with the Data Source API V2. 

Lucene indices are used for persistent storage to make the challenge of integration realistic while still simple.

The local file system is currently used to simplify the code. 

When writing data
* store each partition as an individual Lucene index in a subdirectory of the same local directory shared by all executors.

When reading data
* push predicates down to the Lucene engine as a [BooleanQuery](https://lucene.apache.org/core/7_2_1/core/org/apache/lucene/search/BooleanQuery.html)
* prune columns to retrieve only explicitly requested Lucene document fields 

##### Running locally

* ```mvn clean test``` to build the data source and execute a few test queries against it