# BigDataIntroTest
A test project written in scala for introduction to Spark, Kafka, Cassandra & Hadoop! 

You Can Find The Requested Report [Here](https://github.com/AliAMA/BigDataIntroTest/blob/master/Report.xlsx) 
### Project Structure

    .
    .
    ├── spark                                        # Module Containing Log Producers & Streaming Jobs 
    ├── create_keyspace_tables.cql                   # Cql File For Creating Cassandra Keyspace & Tables
    ├── Report.xlsx                                  # Requested Report For Timing of Implementation
    └── README.md
    
#### Spark Module Structure

        .
        ├── ...
        ├── scala                                   # Source Root Dir
        │   ├── clickstream                         # Log Producer App for Click Events
        │   ├── config                              # Module Config Objects (config is written in HOCON format) 
        │   └── domain                              # Domain Objects
        │   └── impressionstream                    # Log Producer App for Impression Events
        │   └── streaming                           # Spark Jobs for Recieving from Kafka & Persist in Cassandra
        │   └── utils                               # Utilities (functions for creating spark context, ...) 
        └── ...
