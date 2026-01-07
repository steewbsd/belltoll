use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use irc::client::Sender;
use serenity::all::ChannelId;
use serenity::all::ExecuteWebhook;
use serenity::all::Http;
use serenity::all::Webhook;
use serenity::prelude::*;
use tokio::sync::Notify;

#[derive(Debug)]
pub enum RelayDirection {
    INVALID,
    IRC2DIS(String),
    DIS2IRC(ChannelId),
}

pub struct RelayMessage {
    pub contents: String,
    pub direction: RelayDirection,
    pub author: String
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


pub async fn relay_consumer(
    buffer: Arc<RwLock<MessageBuffer>>,
    notify: Arc<RelayNotify>,
    http: Arc<Http>,
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
                let target = assoc.find_target(RelayDirection::IRC2DIS(chan));
                match target {
                    RelayDirection::DIS2IRC(t) => {
                        let webhook = Webhook::from_url(&http, assoc.chid_webhook_assoc.get(&t).expect("Expected a webhook url for channel {t.get()}")).await.unwrap();
                        let builder = ExecuteWebhook::new().content(pending.contents).username(pending.author);
                        webhook.execute(&http, false, builder).await.expect("Could not execute webhook.");
                    },
                    _ => { panic!("Found no target to send the message to!") }
                }
            }
            RelayDirection::DIS2IRC(chan) => {
                let unpingable_name = pending.author.clone();
                let (first, rest) = unpingable_name.split_at(1);
                let mut unpingable_name = String::new();
                unpingable_name.push(char::from_u32(0x03).unwrap());
                unpingable_name.push_str("04");
                unpingable_name.push_str(first);
                unpingable_name.push_str("​");
                unpingable_name.push_str(rest);
                unpingable_name.push(char::from_u32(0x03).unwrap());
                
                let target = assoc.find_target(RelayDirection::DIS2IRC(chan));

                match target {
                    RelayDirection::IRC2DIS(t) => {
                        let response = format!("<{}>: {}", unpingable_name, pending.contents);
                        sender.send_privmsg(t, response).unwrap();
                    },
                    _ => { panic!("Found no target to send the message to!") }
                }
            }
            _ => {}
        }
    }
}

