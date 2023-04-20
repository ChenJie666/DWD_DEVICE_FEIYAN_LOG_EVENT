`./flink run -m yarn-cluster -ynm DWD_DEVICE_FEIYAN_LOG_EVENT -p 3 -ys 3 -yjm 1024 -ytm 2000m -d -c com.iotmars.compass.DeviceChangeLogApp -yqu default /opt/jar/DWD_DEVICE_FEIYAN_LOG_EVENT-1.0-SNAPSHOT.jar`
`./flink run \
 -s hdfs://192.168.101.193:8020/flink/checkpoint/DWD_DEVICE_FEIYAN_LOG_EVENT/33d611061dff4b482dbe1fa548f8abbe/chk-4227/_metadata \
 -m yarn-cluster -ynm DWD_DEVICE_FEIYAN_LOG_EVENT_prod -p 2 -ys 1 -yjm 1024 -ytm 4096m \
 -d -c com.iotmars.compass.DeviceChangeLogApp -yqu default \
 -yD metrics.reporter.promgateway.class=org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter \
 -yD metrics.reporter.promgateway.host=192.168.101.174 -yD metrics.reporter.promgateway.port=9091 \
 -yD metrics.reporter.promgateway.jobName=flink-metrics- \
 -yD metrics.reporter.promgateway.randomJobNameSuffix=true \
 -yD metrics.reporter.promgateway.deleteOnShutdown=false \
 -yD metrics.reporter.promgateway.groupingKey="instance=DWD_DEVICE_FEIYAN_LOG_EVENT_prod" \
 /opt/jar/prod/DWD_DEVICE_FEIYAN_LOG_EVENT-1.0-SNAPSHOT.jar`