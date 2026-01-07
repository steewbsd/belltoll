use serenity::all::Message;
use serenity::all::Ready;
use serenity::async_trait;
use serenity::prelude::*;

use crate::relay::*;

pub struct Handler;

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

