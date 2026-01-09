use std::hash::Hash;

use serenity::all::Message;
use serenity::all::MessageUpdateEvent;
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

        println!("author id: {}, cache id: {}", msg.author.name, ctx.cache.current_user().name);
        if let Some(_) = msg.webhook_id {
            println!("Same discord author as the bot");
            return;
        };

        {
            let mut relay_buffer = buffer_lock.write().await;
            // create a new message with the received discord message contents
            let mut new_message = RelayMessage::default();
            new_message.contents = msg.content;
            new_message.direction = RelayDirection::DIS2IRC(msg.channel_id);
            new_message.author = msg.author.name;
            // check if message is a reply to also relay it
            if let Some(reply) = msg.referenced_message {
                new_message.message_type = MessageType::ReplyDiscord((reply.content, reply.author.name));
            }
            // push the pending message to the relay buffer
            relay_buffer.pending_relay_messages.push_back(new_message);
        }
        {
            let notify = data.get::<RelayNotify>().unwrap().clone();
            notify.notify.notify_one();
        }
    }

    async fn message_update(&self,
                            ctx: Context,
                            old_if_available: Option<Message>,
                            new_if_available: Option<Message>,
                            event: MessageUpdateEvent)
    {
        // if the original or new message is not available, it doesn't make sense to store it, as the IRC
        // side would have no context of the edit.
        if let None = old_if_available { return };
        if let None = new_if_available { return };
        
        let old = old_if_available.unwrap();
        let new = new_if_available.unwrap();

        // open the shared lock as write
        let data = ctx.data.read().await;
        let buffer_lock = data.get::<MessageBuffer>().unwrap().clone();

        if let Some(_) = old.webhook_id {
            println!("Discarding webhook edit.");
            return;
        };
        
        {
            let mut relay_buffer = buffer_lock.write().await;
            // create a new message with the received discord message contents
            let mut new_message = RelayMessage::default();
            new_message.direction = RelayDirection::DIS2IRC(new.channel_id);
            new_message.author = new.author.name;

            let edit_message = format!("edited: \"{}\"\r\n\t↪ {}", old.content, new.content);
            new_message.contents = edit_message;

            // push the pending message to the relay buffer
            relay_buffer.pending_relay_messages.push_back(new_message);
        }
        {
            let notify = data.get::<RelayNotify>().unwrap().clone();
            notify.notify.notify_one();
        }
        
    }

    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

