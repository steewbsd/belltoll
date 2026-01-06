use std::collections::HashMap;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;

use futures_util::StreamExt;
use ini::Ini;
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
use serenity::prelude::*;
use tokio::spawn;
use tokio::sync::Notify;

#[derive(Debug)]
enum RelayDirection {
    INVALID,
    IRC2DIS(String),
    DIS2IRC(ChannelId),
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

struct RelayAssoc {
    // stores the Discord - IRC channel bridge associations
    bridge_assoc: HashMap<ChannelId, Vec<String>>,
    // stores the webhook URL for the discord channels
    chid_webhook_assoc: HashMap<ChannelId, String>,
}

impl Default for RelayAssoc {
    fn default() -> Self {
        RelayAssoc {
            bridge_assoc: HashMap::new(),
            chid_webhook_assoc: HashMap::new()
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
            new_message.direction = RelayDirection::DIS2IRC(msg.channel_id);
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
    assoc: RelayAssoc
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
            RelayDirection::IRC2DIS(chan) => {
                // let chanid = ChannelId::new(591954698664149044);
                // chanid.say(http.clone(), pending.contents).await.unwrap();
                let builder = ExecuteWebhook::new().content(pending.contents).username(pending.author);
                webhook.execute(&http, false, builder).await.expect("Could not execute webhook.");
            }
            RelayDirection::DIS2IRC(chan) => {
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

async fn irc_producer(
    mut irc_client: irc::client::Client,
    buffer_reference: Arc<RwLock<MessageBuffer>>,
    notify: Arc<RelayNotify>
) {
    let mut irc_stream: irc::client::ClientStream = irc_client.stream().unwrap();

    while let Ok(Some(message)) = irc_stream.next().await.transpose() {
        let msg_clone = message.clone();
        match message.command {
            Command::PRIVMSG(_, contents) => {
                {
                    let uname = msg_clone.source_nickname().unwrap();
                    let mut buffer = buffer_reference.write().await;
                    let mut new_message = RelayMessage::default();
                    new_message.contents = contents;
                    new_message.direction = RelayDirection::IRC2DIS(String::from_str(msg_clone.response_target().unwrap()).unwrap());
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
#[tokio::main]
async fn main() {
    // Shared data initialization
    // ======================================================================================
    let buffer_reference = Arc::new(RwLock::new(MessageBuffer::default()));
    let notify = Arc::new(RelayNotify::default());
    
    let discord_notify = notify.clone();
    let discord_buffer_reference = buffer_reference.clone();
    let irc_notify = notify.clone();
    let irc_buffer_reference = buffer_reference.clone();

    // Discord initialization
    // ======================================================================================
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
    let mut discord_client = serenity::Client::builder(&token, intents)
        .event_handler(Handler)
        .await
        .expect("Err creating client");

    // set the arbitrary data mutex as write
    {
        let mut data = discord_client.data.write().await;
        data.insert::<MessageBuffer>(discord_buffer_reference);
        data.insert::<RelayNotify>(discord_notify);
    }
    
    // shared http sender for the discord client
    let shared_http = discord_client.http.clone();
    
    // spawn discord client async thread
    let _client_handle = spawn(async move {
        if let Err(why) = discord_client.start().await {
            eprintln!("Client error: {why:?}");
        }
    });

    // TODO: remove and read from file
    let webhook = Webhook::from_url(&shared_http,
                                    "").await.unwrap();

    // ======================================================================================
    // IRC initialization
    let config = Config {
        nickname: Some("belltoll".to_owned()),
        server: Some("irc.libera.chat".to_owned()),
        channels: vec!["##steew".to_owned()],
        ..Default::default()
    };

    let irc_client = irc::client::Client::from_config(config).await.unwrap();
    irc_client.identify().unwrap();
    let irc_sender = irc_client.sender().clone();

    let _irc_handle = spawn(async move {
        irc_producer(irc_client, irc_buffer_reference, irc_notify).await;
    });
    // Configuration file read for channel bridge associations
    // ======================================================================================
    let ini_conf_file = "config.ini";
    let ini = Ini::load_from_file(ini_conf_file).expect("A config.ini file is needed");

    let mut assoc = RelayAssoc::default();
        
    match ini.section(Some("assoc")) {
        Some(section_contents) => {
            for (d_channel, i_channel) in section_contents.iter() {
                let chid = u64::from_str(d_channel).expect("Channel ID is not valid! {d_channel}");
                let irc_chan = String::from_str(i_channel).expect("Channel name is not valid! {i_channel}");
                let mut ircs = Vec::new();
                ircs.push(irc_chan);
                assoc.bridge_assoc.insert(ChannelId::new(chid), ircs);
            }
        },
        None => { panic!("Expected an [assoc] section with bridge associations.") },
    }
    println!("{:?}", assoc.bridge_assoc.keys());
    println!("{:?}", assoc.bridge_assoc.values());

    // ======================================================================================
    // Relay consumer thread spawn
    let _relay_handle = spawn(async move {
        relay_consumer(
            buffer_reference,
            notify,
            shared_http,
            webhook,
            irc_sender,
            assoc
        )
        .await;
    });
}
