use std::str::FromStr;
use std::sync::Arc;

use futures_util::StreamExt;
use irc::proto::Command;
use serenity::prelude::*;

use crate::relay::*;

pub async fn irc_producer(
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
