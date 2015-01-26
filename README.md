# Spark clustering algorithms
Implemntation of [DBSCAN](https://en.wikipedia.org/wiki/DBSCAN) and [K-means](https://en.wikipedia.org/wiki/K-means_clustering) clustering algorithms in Scala using Spark framework. Algorithms deal only with two dimensional (*x* and *y*) data.

### DBSCAN
Program arguments: `<input_file> <min_points_in_cluster> <epsilon>`

### KMeans
Program arguments: `<input_file> <number_of_clusters> <converge_distance>`

### Dataset
Sample dataset file is included - *data.txt*.

### Running 
- When launching on a cluster refer to [Spak official documentation](https://spark.apache.org/docs/latest/ec2-scripts.html).
- In order to run on local machine use `-Dspark.master=local` VM option.