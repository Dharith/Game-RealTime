name := "Game-RealTime"

version := "0.1"

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.2.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"


