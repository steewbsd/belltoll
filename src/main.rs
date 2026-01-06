use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::StreamExt;
use irc::client::Sender;
use irc::client::data::Config;
use irc::proto::Command;
use serenity::all::ChannelId;
use serenity::all::ExecuteWebhook;
use serenity::all::Http;
use serenity::all::Message;
use serenity::all::Ready;
use serenity::all::Webhook;
use serenity::async_trait;
use serenity::model::webhook;
use serenity::prelude::*;
use tokio::spawn;
use tokio::sync::Notify;

#[derive(Debug)]
enum RelayDirection {
    INVALID,
    IRC2DIS,
    DIS2IRC,
}

struct RelayMessage {
    contents: String,
    direction: RelayDirection,
    author: String,
}

struct MessageBuffer {
    pending_relay_messages: VecDeque<RelayMessage>,
}

struct RelayNotify {
    notify: Notify,
}

impl Default for RelayNotify {
    fn default() -> Self {
        RelayNotify {
            notify: Notify::new(),
        }
    }
}

impl Default for RelayMessage {
    fn default() -> Self {
        RelayMessage {
            contents: String::new(),
            direction: RelayDirection::INVALID,
            author: String::new()
        }
    }
}

impl Default for MessageBuffer {
    fn default() -> Self {
        MessageBuffer {
            pending_relay_messages: VecDeque::new(),
        }
    }
}

impl TypeMapKey for MessageBuffer {
    type Value = Arc<RwLock<MessageBuffer>>;
}

impl TypeMapKey for RelayNotify {
    type Value = Arc<RelayNotify>;
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        // open the shared lock as write
        let data = ctx.data.read().await;
        let buffer_lock = data.get::<MessageBuffer>().unwrap().clone();

        if msg.author.bot {
            return;
        };

        {
            let mut relay_buffer = buffer_lock.write().await;
            // create a new message with the received discord message contents
            let mut new_message = RelayMessage::default();
            new_message.contents = msg.content;
            new_message.direction = RelayDirection::DIS2IRC;
            new_message.author = msg.author.name;
            // push the pending message to the relay buffer
            relay_buffer.pending_relay_messages.push_back(new_message);
        }
        println!("Added discord message to buffer.");
        {
            let notify = data.get::<RelayNotify>().unwrap().clone();
            notify.notify.notify_one();
        }
    }

    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

async fn relay_consumer(
    buffer: Arc<RwLock<MessageBuffer>>,
    notify: Arc<RelayNotify>,
    http: Arc<Http>,
    webhook: Webhook,
    sender: Sender,
) {
    loop {
        // await for new relay pending events
        notify.notify.notified().await;
        let pending: RelayMessage;
        {
            let mut buffer_lock = buffer.write().await;
            pending = buffer_lock.pending_relay_messages.pop_front().unwrap();
        }
        println!(
            "Received message to relay: {}, {:?}",
            pending.contents, pending.direction
        );
        match pending.direction {
            RelayDirection::IRC2DIS => {
                // let chanid = ChannelId::new(591954698664149044);
                // chanid.say(http.clone(), pending.contents).await.unwrap();
                let builder = ExecuteWebhook::new().content(pending.contents).username(pending.author);
                webhook.execute(&http, false, builder).await.expect("Could not execute webhook.");
            }
            RelayDirection::DIS2IRC => {
                let unpingable_name = pending.author.clone();
                let (first, rest) = unpingable_name.split_at(1);
                let mut unpingable_name = String::new();
                unpingable_name.push_str(first);
                unpingable_name.push_str("​");
                unpingable_name.push_str(rest);
                
                let response = format!("<{}>: {}", unpingable_name, pending.contents);
                sender.send_privmsg("##steew", response).unwrap();
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() {
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");

    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
    let mut discord_client = serenity::Client::builder(&token, intents)
        .event_handler(Handler)
        .await
        .expect("Err creating client");

    let buffer_reference = Arc::new(RwLock::new(MessageBuffer::default()));
    let notify = Arc::new(RelayNotify::default());

    // set the arbitrary data mutex as write
    {
        let mut data = discord_client.data.write().await;
        data.insert::<MessageBuffer>(buffer_reference.clone());
        data.insert::<RelayNotify>(notify.clone());
    }

    let shared_http = discord_client.http.clone();
    let shared_notify = notify.clone();
    let shared_buffer_reference = buffer_reference.clone();

    let webhook = Webhook::from_url(&shared_http, "").await.unwrap();

    // spawn discord client
    let _client_handle = spawn(async move {
        if let Err(why) = discord_client.start().await {
            eprintln!("Client error: {why:?}");
        }
    });

    let config = Config {
        nickname: Some("belltoll".to_owned()),
        server: Some("irc.libera.chat".to_owned()),
        channels: vec!["##steew".to_owned()],
        ..Default::default()
    };

    let mut irc_client = irc::client::Client::from_config(config).await.unwrap();
    irc_client.identify().unwrap();

    let mut irc_stream: irc::client::ClientStream = irc_client.stream().unwrap();

    // spawn relay consumer
    let _relay_handle = spawn(async move {
        relay_consumer(
            shared_buffer_reference,
            shared_notify,
            shared_http,
            webhook,
            irc_client.sender(),
        )
        .await;
    });

    while let Ok(Some(message)) = irc_stream.next().await.transpose() {
        let msg_clone = message.clone();
        match message.command {
            Command::PRIVMSG(_, contents) => {
                {
                    let uname = msg_clone.source_nickname().unwrap();
                    let mut buffer = buffer_reference.write().await;
                    let mut new_message = RelayMessage::default();
                    new_message.contents = contents;
                    new_message.direction = RelayDirection::IRC2DIS;
                    new_message.author.push_str(uname);
                    buffer.pending_relay_messages.push_back(new_message);
                    notify.notify.notify_one();
                }
                println!("Added IRC message to buffer");
            }
            _ => {}
        }
    }
}
