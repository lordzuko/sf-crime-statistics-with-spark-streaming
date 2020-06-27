conda install -c anaconda pip
pip install -r requirements.txt
mkdir startup
echo 'schema.registry.url=http://localhost:8081' >> /etc/kafka/connect-distributed.properties
systemctl start confluent-zookeeper > startup/startup.log 2>&1
systemctl start confluent-kafka > startup/startup.log 2>&1
systemctl start confluent-schema-registry > startup/startup.log 2>&1
systemctl start confluent-kafka-rest > startup/startup.log 2>&1
systemctl start confluent-kafka-connect > startup/startup.log 2>&1
systemctl start confluent-ksql > startup/startup.log 2>&1

