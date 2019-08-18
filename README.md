# stream-scorer

This package helps implement a streaming prediction pipeline on Kafka Stream. This means the streaming component can read
data from an input topic, do some preprocessing, load the model and predict and store the results back to another output
topic. Any project that is designed as a Kafka Streaming Application can extend this class irrespective of they have a
predictive component or not. The only requirement is to write any specific logic that is needed in your use case.

**Motivation**

The motivation behind this project is many of the use cases that require a NRT serving prefer Kafka as the recommended 
technology for such streaming applications. Since many of the steps are common in these applications such as reading the
data, loading the model, making the prediction and storing the results back to another topic, etc. I decided to develop 
a generic architecture that does all these heavy-lifting so that a subsequent stream application development much more easier and faster. The idea 
is with minimal configuration changes and minimal code changes, one should have a full-blown production ready ML prediction 
streaming application up and running in no time.

**Salient Features**
1. **On-Demand A/B Testing**

Let's say you want to direct some of your records to be predicted by a model and rest by another model
or a different version of the same model. You can pass `model_uuid` (or `model_tag`) in your Kafka message either in the
Message Header or in the Message Payload. The streaming component has a filter and it filters the record out if the `model_uuid`
doesn't match and only records that have a matching `model_uuid` will go through the prediction pipeline. If `model_uuid` is
not provided, then all the records will go through the prediction pipeline.

2. **Prediction Logging for continuous monitoring**

The code will also log all the raw input, features, prediction, model_uuid, model_tag and timestamp in a log topic. This
information can be used for continuous monitoring once the model is deployed in production to check the distribution of
input features, accuracy and other performance metrics of the model etc.

The project has mainly two components:

1. KafkaStreamingScorerService
    This will take care of establishing Kafka connection and running a streaming application
2. StreamScorer
    This will take care of the ML predicting pipeline tasks such as pre-processing, model loading, predicting, result
    dispatching etc.

Please refer the example template to know how to create a Streaming Scorer application within minutes. The example mainly
talks about implementing a streaming application that uses H2O's POJO and wrapper classes. But it can work well for other
models as well.

### Stream Scorer Service Design
![High Level Design of Stream Scorer Service](images/generic_stream_service_design.png?raw=true "High Level Design of Stream Scorer Service")

## Run Book

### Creating a new streaming application

You must create an input topic and out topic for this streaming application to work. If you are using H2O to build model,
make sure you have built the model and downloaded the POJO and jar files.

#### Pre-requisites

Copy the below files from [deployables](deployables/) folder.
1. **application.properties**
2. **logback.xml**
3. **stream-scorer.jar**

   *Note: This document assumes we are building our project using maven.*

1.  Add the stream-scorer.jar to your local maven repository using the below command.

```sh
# Dfile is the location of your local directory where you kept these two jars
mvn install:install-file -Dfile=<<location of your jar>>//stream-scorer.jar -DgroupId=com.ml.stream -DartifactId=stream-scorer -Dversion=1.0.0 -Dpackaging=jar
```

2. Create your Model class. 
Refer this [section](#Using-H2O-Models-with-this-code) to know how to create a Model class.

3. Create a new maven project `ExampleScorer` using your choice of editor

   *Note: Eclipse and IntelliJ provide option to create a Maven project.*

4. Add the following entry to your pom.xml
```xml
    <dependency>
       <groupId>com.ml.stream</groupId>
       <artifactId>stream-scorer</artifactId>
       <version>1.0.0</version>
    </dependency>
```
5. Create a new class that extends StreamScorer.

*Note: If your application has just one model and uses the default output topic schema
then you wouldn't need to create an extended class* 

```java
import StreamScorer;

public class ExampleStreamScorer extends StreamScorer
```

6. Implement and Override the below method:

```java
    @Override
    protected GenericRecord createOutputRecord(GenericRecord value, Prediction pred){
        // Instantiating the GenericRecord class.
        GenericRecord outRecord = AvroUtil.createRecordFromAnother(outputSchema, value);
        // The above command will set the values from input record `value`.
        // Set the predicted value
        outRecord.put(getPredictionColumn(), pred.getValue());
        return outRecord;
    }
```

Tip: If you want to create your own output record, you can use something similar below:

```java
@Override
    protected GenericRecord createOutputRecord(GenericRecord value, Prediction pred){
        // Instantiating the GenericRecord class.
        // create a map of output record
        HashMap<String, Object> values = new HashMap<String, Object>();
        values.put("Id", "1234");
        values.put("Predicted_Value", pred.getValue());
        GenericRecord outRecord = AvroUtil.createGenericRecordWithSchema(outputSchema, values);
        return outRecord;
    }
```
There are multiple ways you can create the output record, if your output record contains fields and values from your input
topic, you can use the method `AvroUtil.createRecordFromAnother` to populate some of the output record values. If you do
not override this method, it will automatically create an output record with model features and prediction as the fields.

7. Create a launcher class for your application. You can name anything.
Check KafkaStreamingServiceLauncher class in here.
```java
public class ExampleServiceLauncher
```

8. Add a `main()` function in the above class
```java
import com.ml.stream.kafkastream.KafkaStreamingScorerService;
import KafkaStreamingServiceBootstrap;
import H2OModel;
import Model;

    public static void main(String[] args){

        // Call the boot strap to set the Properties because both Scorer and Scorer service will need
        // the properties
    	Properties props = new KafkaStreamingServiceBootstrap(args).getAppProperties();
    	
        // create your model wrapper class. The below code is for creating a H2O model.
        Model model = new H2OModel("GLM_model_R_1234", "Example_UseCase_Model", "GLM_model_R_1234");

        // create your scorer processor using the class that you've already created
        ExampleStreamScorer scorer = new ExampleStreamScorer(props, model);

    	// start the service
	    KafkaStreamingScorerService scorerService = new KafkaStreamingScorerService(props, scorer);

	    scorerService.run();

    }
```

9. Edit `application.properties` file to enter input topic, output topic and other kafka connection information. Make sure
you go through the comments and understand the use of each field in the property file.

10. Build your project. You may also run `mvn clean install` from command prompt from the root directory where you have saved `pom.xml`
or do the same from the maven menu options provided by Eclipse or IntelliJ.

**Other Notes:**

**Creating other processing logic streaming application**

1. If you intend to use this architecture for any other purpose other than prediction, you can just extends the
`KafkaStreamingScorerService` class and override the `task()` method.

```java
public class ExampleKafkaStreamingScorerService extends KafkaStreamingScorerService
```

2. Implement Override the task() method

```java
    @Override
    public GenericRecord task(GenericRecord value){
        logger.info("Loading the specific task...");
        // instead of the scorer you can add your application logic here
        //GenericRecord result = this.scorer.run(value);
        //return result;
    }
```

**Using multiple models in Scorer class**

If you want to use multiple models in your scorer class. You can extend the 
`loadModels()` function. An example:

```java
@Override
protected void loadModels(){
    // create your model wrapper here
    this.modelA = new H2OModel("GLM_model_R_1234", "Example_UseCase_Model", "GLM_model_R_1234");
    this.addModel(this.modelA);
}
```

## Building the package

You can run the Build using the IDE features or run `mvn clean install` as described above.

Your output will be for example `example-stream-application.jar`.

## Running the application from shell 
You can create a simple `.sh` file with following command.
```sh
#!/bin/bash
nohup java -Dlogback.configurationFile=logback.xml -jar example-stream-application.jar >/dev/null 2>&1 &
```

## Using H2O Models with this code
If you are using H2O models in this streaming application code as your model. Make sure you build the POJO and Jar file of
your model. Below are steps to create POJO and jar files. Please refer to H2O's website to know more about how to download a POJO.

*Steps:*
1. Compile GLM_model_R_1234.java first. It will generate all classes. For compiling run
   ```sh
   javac -cp h2o-genmodel.jar GLM_model_R_1234.java
   ```
2. Make jar out of classes produced (example-model.jar). Run
    ```sh
    jar cf example-model.jar *.class
    ```
3. Add the above packages to your local maven repository by running the below command.
    *Note: This must be done in order to add these projects as dependecies in `pom.xml`.*
```sh
# Dfile is the location of your local directory where you kept these two jars
mvn install:install-file -Dfile=<<location of your jar>>//h2o-genmodel.jar -DgroupId=com.ml.stream -DartifactId=h2o-genmodel -Dversion=1.0.0 -Dpackaging=jar
mvn install:install-file -Dfile=<<location of your jar>>//example-model.jar -DgroupId=com.ml.stream -DartifactId=example-model -Dversion=1.0.0 -Dpackaging=jar
```

4. Go to your `ExampleScorer` project's POM.xml and add the following:
    *Make sure you change the groupid and artifactid for your case.*

```xml
    <!-- ML Model jar  needs to be added. Please add these dependency in local maven before building
    Jars and steps are available in info folder-->
       <dependency>
           <groupId>com.ml.stream</groupId>
           <artifactId>h2o-genmodel</artifactId>
           <version>1.0.0</version>
       </dependency>
       <dependency>
           <groupId>com.ml.stream</groupId>
           <artifactId>example-model</artifactId>
           <version>1.0.0</version>
           <scope>runtime</scope>
       </dependency>
```

## Contributing and Making Changes

This is a common component and you are free to make any changes to it make it better. Once the changes are tested, you
can build the package using the steps above.

Make sure that you add the newly built jar file to your local maven repository using the command below to get the changes
reflected in your application that extends this class.

```sh
# Dfile is the location of your local directory where you kept these two jars
mvn install:install-file -Dfile=<<location of your jar>>//stream-scorer.jar -DgroupId=com.ml.stream -DartifactId=stream-scorer -Dversion=1.0.0 -Dpackaging=jar
```
