package rabid

case class ServerConf(host: String, port: Short)

case class ConnectionConf(user: String, password: String, vhost: String)

case class QosConf(prefetchSize: Int, prefetchCount: Short)

case class RabidConf(server: ServerConf, connection: ConnectionConf, qos: QosConf)
