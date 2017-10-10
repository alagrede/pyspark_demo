# Download Pyspark docker
```
docker pull rpresle/pyspark:1.4.0
```

# Launch image
Create folder structure: __pyspark_demo/src__ and __pyspark_demo/data__
_(Don't forget to replace $PWD)_

```
docker run -it -v $PWD/pyspark_demo/src/:/app/src -v $PWD/pyspark_demo/data/:/app/data rpresle/pyspark:1.4.0 bash
```

## Link on data
Download data on __data/__ directory

http://www.rpresle.totoandco.eu/wp-content/uploads/2015/07/crimes_chicago_2012_2015.csv


# Run the job
``` 
spark-submit --master local[4] /app/src/CrimeTreatment.py
```

