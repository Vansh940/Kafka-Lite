$env:BROKER_ID="1"
$env:BROKER_PORT="9092"
$env:ZK_CONNECT="localhost:2181"
$env:LOG_DIR="C:\kafka-lite-data\broker-1"

java -jar broker-server\target\broker-server-1.0.0.jar