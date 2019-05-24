Follow these steps to deploy and run JASMINE application.

Firstly, S3 buckets must to be created.

On AWS S3 console:

  • create S3 bucket "jasmine-spark-cluster";
  • create "config" directory into "jasmine-spark-cluster" bucket;
  • upload "config.json" into "config" directory;
  • create "app" directory into "jasmine-spark-cluster" bucket;
  • upload application jar into "app" directory;

  • create S3 bucket "jasmine-utils";
  • upload "start-ec2-nifi-instance.sh" into "jasmine-utils" bucket;
  • upload "load-hdfs-info.sh" into "jasmine-utils" bucket;
  • upload "hdfs-info-extractor.sh" into "jasmine-utils" bucket;

  • create S3 bucket "jasmine-data";
  • upload project's csv files into "jasmine-data" bucket;
  • create "hdfs/conf" directory into "jasmine-data" bucket;

  • create S3 bucket "jasmine-nifi-nar";
  • upload .nar files into "jasmine-nifi-nar" bucket.

It's now important to configure appropriate security group to allow communication between systems.

On AWS console:

  • create "emr" security group properly configured;
  • create "nifi" security group properly configured.

Let's create cluster for Spark and Hadoop environment.

On AWS EMR console:

  • Go to Create Cluster through advanced options;
  • Set emr-5.23.0 with Spark and Hadoop flagged;
  • Set software settings loading JSON from S3;
  • When required, select EC2 key pair;
  • Set "emr" as security group for Master and Core&Task.

Before creating NiFi, connect to EMR cluster as suggested in "Summary" cluster page and run the following lines:

  aws s3 cp s3://jasmine-utils/hdfs-info-extractor.sh hdfs-info.sh
  chmod 764 hdfs-info.sh
  ./hdfs-info.sh
  rm hdfs-info.sh
  exit

Let's create NiFi instance.
On AWS EC2 console launch EC2 instance (e.g. m4-large) and connect to it.

Once connected, run the following lines:

  sudo yum update -y
  aws s3 cp s3://jasmine-utils/start-ec2-nifi-instance.sh start-nifi.sh
  chmod 764 start-nifi.sh
  ./start-nifi.sh
  exit

Now, you must connect again to the instance in order to create Redis running the following:

  docker run -d -p 6379:6379 --name=redis redis

Spark and HDFS are now up to EMR while NiFi and Redis are running on EC2 instance.

To deploy MongoDB, Nginx and NodeJS, go into the directory where jasmine_cluster.sh file is located and run the following:

  ./jasmine_cluster.sh --public-key YOUR_PUBLIC_KEY

Then follow instruction to be aware that cluster is ready.
When Kops cluster is ready, run

  ./jasmine_deploy.sh.

Now the architecture is ready!
Let's run the application.

On AWS EMR cluster interface, go to Step:

  • Add Step (as Spark Application);
  • In Spark-Submit field insert:
      --class MAIN_CLASS --num-executors NUM_EXECUTORS --executor-cores NUM_CORES --executor-memory MEMORY
  • Find application jar in S3 bucket created previously.

Once completed, add step and wait until the application has completed.
