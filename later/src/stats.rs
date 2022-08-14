use crate::{
    models::{Job, Stage},
    mq::{MqClient, MqConsumer, MqPublisher},
    storage::Storage,
};

pub(crate) enum Event<'j> {
    NewJob(&'j Job),
    JobTransitioned(Stage, &'j Job),
}

#[async_trait::async_trait]
pub(crate) trait EventsHandler {
    async fn new_event() -> (); // Infallible
}

#[allow(dead_code)]
pub struct Stats {
    publisher: Box<dyn MqPublisher>,
}

impl Stats {
    pub(crate) async fn new(
        main_routing_key: &str,
        storage: &'static Box<dyn Storage>,
        mq_client: &Box<dyn MqClient>,
    ) -> anyhow::Result<Self> {
        let routing_key = format!("{}-stats", main_routing_key);
        let publisher = mq_client.new_publisher(&routing_key).await?;
        let mut consumer = mq_client.new_consumer(&routing_key, 1).await?;

        tokio::spawn(async move {
            let _ = handle_stat_events(&mut consumer, storage).await;
        });

        Ok(Self { publisher })
    }
}

async fn handle_stat_events(
    consumer: &mut Box<dyn MqConsumer>,
    storage: &Box<dyn Storage>,
) -> anyhow::Result<()> {
    while let Some(_) = consumer.next().await {}

    Ok(())
}
