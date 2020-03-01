# Parallel Processing Call Data

This is a project that was created as a final assignment for a Data Intensive Computing course in May 2019. The goal of this project was to take CDR call data and find out how many minutes in total each phone number spent calling others. To do this I used a java implemented version of Hadoop loaded in the IntelliJ IDE to map/reduce the call time data. CodeExplanation.pdf gives a more detailed explanation of how the code achieves this goal. Testdata.txt gives an example of what format the program expects the input to come in and results.txt gives an example of what the results will look like.

### Getting Started 

To run this code you must integrate your IDE to import Hadoop libraries so that it can replicate a multi-node processing cluster. This will include manually configuring the input/output files in the IDE settings so that the java code knows what data to read in and the destination to submit the output file. Once this is done download and move the CDRsort.java file into the IDE and run the file. 

### Built With 

* IntelliJ - IDE
* Hadoop -  framework for distributed processing of large data sets using clusters 

### Authors 

* Kyle Dennison - KyleDennison
