use crate::{bg_job_server::sleep_ms, encoder};
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer,
};
use serde::Serialize;

static EXCHANGE_NAME: &str = "later_dx";

pub struct Client {
    address: String,
    routing_key: String,
}

pub struct Publisher {
    channel: Channel,
    routing_key: String,
}

impl Publisher {
    pub async fn publish(&self, payload: impl Serialize) -> anyhow::Result<()> {
        let message_bytes = encoder::encode(payload)?;

        self.channel
            .basic_publish(
                EXCHANGE_NAME,
                &self.routing_key,
                BasicPublishOptions::default(),
                &message_bytes,
                BasicProperties::default(),
            )
            .await?;

        Ok(())
    }

    pub async fn ensure_consumer(&self) -> anyhow::Result<()> {
        let mut consumer_count = 0;
        while consumer_count == 0 {
            consumer_count = declare_get_queue(&self.channel, &self.routing_key)
                .await?
                .consumer_count();
            if consumer_count > 0 {
                break;
            }
            sleep_ms(50).await;
        }

        Ok(())
    }
}

impl Client {
    pub fn new(address: &str, routing_key: &str) -> Self {
        Self {
            address: address.to_string(),
            routing_key: routing_key.to_string(),
        }
    }

    pub async fn new_consumer(&self, worker_id: i32) -> anyhow::Result<Consumer> {
        let connection =
            Connection::connect(&self.address, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        let _ = channel
            .exchange_declare(
                EXCHANGE_NAME,
                lapin::ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let _q = declare_get_queue(&channel, &self.routing_key).await?;

        channel
            .queue_bind(
                &self.routing_key,
                EXCHANGE_NAME,
                &self.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        let consumer = channel
            .basic_consume(
                &self.routing_key,
                &format!("later-consumer-{}", worker_id),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(consumer)
    }

    pub async fn new_publisher(&self) -> anyhow::Result<Publisher> {
        let connection =
            Connection::connect(&self.address, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        Ok(Publisher {
            channel,
            routing_key: self.routing_key.clone(),
        })
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

async fn declare_get_queue(channel: &Channel, routing_key: &str) -> anyhow::Result<lapin::Queue> {
    Ok(channel
        .queue_declare(
            routing_key,
            QueueDeclareOptions {
                durable: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?)
}
