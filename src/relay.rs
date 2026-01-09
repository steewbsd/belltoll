use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use irc::client::Sender;
use serenity::all::Cache;
use serenity::all::ChannelId;
use serenity::all::ExecuteWebhook;
use serenity::all::Http;
use serenity::all::Webhook;
use serenity::builder;
use serenity::prelude::*;
use sha1::{Sha1, Digest};
use tokio::sync::Notify;

#[derive(Debug)]
pub enum RelayDirection {
    INVALID,
    IRC2DIS(String),
    DIS2IRC(ChannelId),
}

pub enum MessageType {
    Normal,
    // discord reply, containing the u64 message id that it references, and the author name.
    ReplyDiscord((String, String)),
    // irc reply, containing the message base64 hash it references.
    ReplyIrc(String)
}

pub struct RelayMessage {
    pub contents: String,
    pub direction: RelayDirection,
    pub author: String,
    pub message_type: MessageType 
}

pub struct MessageBuffer {
    pub pending_relay_messages: VecDeque<RelayMessage>,
}

pub struct RelayNotify {
    pub notify: Notify,
}

impl Default for RelayNotify {
    fn default() -> Self {
        RelayNotify {
            notify: Notify::new(),
        }
    }
}

pub struct RelayAssoc {
    // stores the Discord - IRC channel bridge associations
    pub bridge_assoc: HashMap<ChannelId, Vec<String>>,
    // stores the webhook URL for the discord channels
    pub chid_webhook_assoc: HashMap<ChannelId, String>,
}

impl RelayAssoc {
    pub fn find_target(&self, source: RelayDirection) -> RelayDirection {
        match source {
            RelayDirection::IRC2DIS(chan) => {
                for (chid, chan_vec) in self.bridge_assoc.iter() {
                    for c in chan_vec.iter() {
                        if c.eq_ignore_ascii_case(&chan) { return RelayDirection::DIS2IRC(*chid) };
                    }
                }
                return RelayDirection::INVALID;
            },
            RelayDirection::DIS2IRC(chan) => {
                if let Some(target) = self.bridge_assoc.get(&chan) {
                    let irc_target = target.first().unwrap();
                    return RelayDirection::IRC2DIS(irc_target.clone());
                } else { return RelayDirection::INVALID }
            },
            _ => { panic!("Invalid source of message! {source:?}") }
        }
    }
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
            author: String::new(),
            message_type: MessageType::Normal
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

fn format_name(name: &str) -> String {
    // XMPP RFC 3174 implementation
    // get hash
    let mut hasher = Sha1::new();
    hasher.update(name);
    let result = hasher.finalize();
    // obtain the last 16 bits
    let last16 = result.last_chunk::<2>().unwrap();
    let last16_float  = f32::from(((last16[0] as u16) << 8) | (last16[1] as u16));
    let hue = last16_float * 360.0 / 65535.0;
    let hue = hue.ceil();
    let hue: u32 = hue.to_bits() % 87;
    println!("{hue}");
    // insert invisible character to prevent pings
    let (split_name_first, split_name_rest) = name.split_at(1);

    format!("\x03{hue:02}{split_name_first}​{split_name_rest}\x03")
}


pub async fn relay_consumer(
    buffer: Arc<RwLock<MessageBuffer>>,
    notify: Arc<RelayNotify>,
    http: Arc<Http>,
    sender: Sender,
    assoc: RelayAssoc,
    avatars: HashMap<String, String>
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
                let target = assoc.find_target(RelayDirection::IRC2DIS(chan));
                match target {
                    RelayDirection::DIS2IRC(t) => {
                        let webhook = Webhook::from_url(&http, assoc.chid_webhook_assoc.get(&t).expect("Expected a webhook url for channel {t.get()}")).await.unwrap();
                        let builder: ExecuteWebhook;
                        let avatar_url: Option<String> = None;
                        if let Some(avatar_url) = avatar_url {
                            println!("{avatar_url}");
                            builder = ExecuteWebhook::new().content(pending.contents).username(pending.author).avatar_url(avatar_url);
                        } else {
                            builder = ExecuteWebhook::new().content(pending.contents).username(pending.author);
                        }
                        
                        webhook.execute(&http, false, builder).await.expect("Could not execute webhook.");
                    },
                    _ => { panic!("Found no target to send the message to!") }
                }
            }
            RelayDirection::DIS2IRC(chan) => {
                let source_name = format_name(&pending.author);
                let target = assoc.find_target(RelayDirection::DIS2IRC(chan));
                match target {
                    RelayDirection::IRC2DIS(t) => {
                        let response;
                        let message_contents = pending.contents;
                        match pending.message_type {
                            MessageType::ReplyDiscord(tuple) => {
                                let target_reply_user = format_name(&tuple.1);
                                let origin_contents = tuple.0;
                                response = format!("<{source_name} replying to: {target_reply_user}> \"{origin_contents}\"\r\n\t↪ {message_contents}");
                            },
                            MessageType::Normal => {
                                response = format!("<{source_name}> {message_contents}");
                            },
                            _ => {
                                // found invalid message type here!!
                                println!("Invalid reply type found.");
                                return;
                            }
                        }
                        sender.send_privmsg(t, response).unwrap();
                    },
                    _ => { println!("Found no target to send the message to!") }
                }
            }
            _ => {}
        }
    }
}

