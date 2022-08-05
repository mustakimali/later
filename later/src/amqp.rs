use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer,
};

pub struct Client {
    address: String,
    //routing_key: String,
}

impl Client {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            //routing_key: routing_key.to_string(),
        }
    }

    pub async fn new_consumer(
        &self,
        routing_key: &str,
        worker_id: i32,
    ) -> anyhow::Result<Consumer> {
        let connection =
            Connection::connect(&self.address, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        let _ = channel
            .queue_declare(
                routing_key,
                QueueDeclareOptions {
                    durable: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        let consumer = channel
            .basic_consume(
                routing_key,
                &format!("later-consumer-{}", worker_id),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(consumer)
    }

    pub async fn new_publisher(&self) -> anyhow::Result<Channel> {
        let connection =
            Connection::connect(&self.address, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        Ok(channel)
    }

    pub async fn ack(&self, delivery: Delivery) -> anyhow::Result<()> {
        Ok(delivery.ack(BasicAckOptions::default()).await?)
    }

    pub async fn nack_requeue(&self, delivery: Delivery) -> anyhow::Result<()> {
        Ok(delivery
            .nack(BasicNackOptions {
                requeue: false,
                ..Default::default()
            })
            .await?)
    }
}
