use crate::bg_job_server::sleep_ms;
use async_std::stream::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};

use super::{MqClient, MqConsumer, MqPayload, MqPublisher};

static EXCHANGE_NAME: &str = "later_dx";

pub struct RabbitMq {
    address: String,
}

pub(crate) struct Publisher {
    channel: Channel,
    routing_key: String,
}

pub(crate) struct Consumer {
    inner: lapin::Consumer,
}

pub(crate) struct Payload(Delivery);

impl RabbitMq {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl MqPayload for Payload {
    async fn ack(&self) -> anyhow::Result<()> {
        Ok(self.0.ack(BasicAckOptions::default()).await?)
    }

    async fn nack_requeue(&self) -> anyhow::Result<()> {
        Ok(self
            .0
            .nack(BasicNackOptions {
                requeue: false,
                ..Default::default()
            })
            .await?)
    }

    fn data(&self) -> &[u8] {
        &self.0.data
    }
}

#[async_trait::async_trait]
impl MqConsumer for Consumer {
    async fn next(&mut self) -> Option<anyhow::Result<Box<dyn MqPayload>>> {
        self.inner.next().await.map(|delivery| {
            delivery
                .map_err(anyhow::Error::from)
                .map(|d| Box::new(Payload(d)) as Box<dyn MqPayload>)
        })
    }
}

#[async_trait::async_trait]
impl MqPublisher for Publisher {
    async fn publish(&self, payload: &[u8]) -> anyhow::Result<()> {
        let message_bytes = payload;

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

    async fn ensure_consumer(&self) -> anyhow::Result<()> {
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

#[async_trait::async_trait]
impl MqClient for RabbitMq {
    async fn new_consumer(
        &self,
        routing_key: &str,
        worker_id: i32,
    ) -> anyhow::Result<Box<dyn MqConsumer>> {
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

        let _q = declare_get_queue(&channel, routing_key).await?;

        channel
            .queue_bind(
                routing_key,
                EXCHANGE_NAME,
                routing_key,
                QueueBindOptions::default(),
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

        Ok(Box::new(Consumer { inner: consumer }))
    }

    async fn new_publisher(&self, routing_key: &str) -> anyhow::Result<Box<dyn MqPublisher>> {
        let connection =
            Connection::connect(&self.address, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        Ok(Box::new(Publisher {
            channel,
            routing_key: routing_key.to_string(),
        }) as Box<dyn MqPublisher>)
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
