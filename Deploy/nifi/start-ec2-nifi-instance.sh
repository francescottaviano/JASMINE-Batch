# INSTALL JAVA
# install java 1.8.0
sudo yum install -y java-1.8.0-openjdk*
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.201.b09-0.amzn2.x86_64/jre/
export PATH=$PATH:$JAVA_HOME/bin

# INSTALL NIFI
wget http://it.apache.contactlab.it/nifi/1.9.2/nifi-1.9.2-bin.tar.gz
tar -xzf nifi-1.9.2-bin.tar.gz && \
    rm nifi-1.9.2-bin.tar.gz
mv nifi-1.9.2 nifi

# add customp components
# reverse geocoding component
aws s3 cp s3://jasmine-nifi-nar/nifi-reversegeocoding-nar-1.0-SNAPSHOT.nar nifi-reversegeocoding-nar-1.0-SNAPSHOT.nar
mv nifi-reversegeocoding-nar-1.0-SNAPSHOT.nar ./nifi/lib/nifi-reversegeocoding-nar-1.0-SNAPSHOT.nar
# filter component
aws s3 cp s3://jasmine-nifi-nar/nifi-file-filter-nar-1.0-SNAPSHOT.nar nifi-file-filter-nar-1.0-SNAPSHOT.nar
mv nifi-file-filter-nar-1.0-SNAPSHOT.nar ./nifi/lib/nifi-file-filter-nar-1.0-SNAPSHOT.nar
# reformat csv component
aws s3 cp s3://jasmine-nifi-nar/nifi-reformat-csv-nar-1.0-SNAPSHOT.nar nifi-reformat-csv-nar-1.0-SNAPSHOT.nar
mv nifi-reformat-csv-nar-1.0-SNAPSHOT.nar ./nifi/lib/nifi-reformat-csv-nar-1.0-SNAPSHOT.nar

# INSTALL DOCKER
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user

# start nifi
./nifi/bin/nifi.sh start

aws s3 cp s3://jasmine-utils/load-hdfs-info.sh load-hdfs-info.sh
chmod 744 load-hdfs-info.sh
./load-hdfs-info.sh
