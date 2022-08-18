use lapin::types::FieldTable;

pub mod amqp;

#[async_trait::async_trait]
pub trait MqClient: Send + Sync {
    async fn new_consumer(
        &self,
        routing_key: &str,
        worker_id: i32,
    ) -> anyhow::Result<Box<dyn MqConsumer>>;
    async fn new_publisher(&self, routing_key: &str) -> anyhow::Result<Box<dyn MqPublisher>>;
}

#[async_trait::async_trait]
pub trait MqPublisher: Send + Sync {
    async fn publish(&self, payload: &[u8]) -> anyhow::Result<()>;
    async fn ensure_consumer(&self) -> anyhow::Result<()>;
    async fn has_consumer(&self) -> anyhow::Result<bool>;
}

#[async_trait::async_trait]
pub trait MqConsumer: Send + Sync {
    async fn next(&mut self) -> Option<anyhow::Result<Box<dyn MqPayload>>>;
}

#[async_trait::async_trait]
pub trait MqPayload: Send + Sync {
    async fn ack(&self) -> anyhow::Result<()>;
    async fn nack_requeue(&self) -> anyhow::Result<()>;
    fn get_headers(&self) -> Option<FieldTable>; // ToDo: do not leak lapin
    fn data(&self) -> &[u8];
}
