use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use futures_util::StreamExt;
use ini::Ini;
use irc::client::data::Config;
use irc::proto::Command;
use serenity::all::ChannelId;
use serenity::all::Settings;
use serenity::all::Webhook;
use serenity::prelude::*;
use tokio::spawn;

mod relay;
mod relay_discord;
mod relay_irc;

use relay::*;
use relay_discord::*;
use relay_irc::*;

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

    let mut cache_settings = Settings::default();
    cache_settings.max_messages = 10_000;
    
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT | GatewayIntents::GUILDS | GatewayIntents::GUILD_MEMBERS;
    let mut discord_client = serenity::Client::builder(&token, intents)
        .cache_settings(cache_settings)
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
    let shared_cache = discord_client.cache.clone();
    
    let avatars: HashMap<String, String> = HashMap::new();

    // avatar url initialization

    // spawn discord client async thread
    let _client_handle = spawn(async move {
        if let Err(why) = discord_client.start().await {
            eprintln!("Client error: {why:?}");
        }
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
                let chid = ChannelId::new(chid);
                assoc.bridge_assoc.insert(chid, ircs.clone());
                // =========================
            }
        },
        None => { panic!("Expected an [assoc] section with bridge associations.") },
    }

    // find the webhook urls now
    match ini.section(Some("webhook")) {
        Some(section_contents) => {
            for (chid_str, webhook_url) in section_contents.iter() {
                let chid = u64::from_str(chid_str).expect("Channel ID is not valid! {chid_str}");
                let webhook_url = String::from_str(webhook_url).expect("Webhook URL is not valid! {webhook_url}");
                assoc.chid_webhook_assoc.insert(ChannelId::new(chid), webhook_url);
            }
        },
        None => { panic!("Expected a [webhook] section with webhook associations.") },
    }

    
    println!("{:?}", assoc.bridge_assoc.keys());
    println!("{:?}", assoc.bridge_assoc.values());
    println!("Webhooks:");
    println!("{:?}", assoc.chid_webhook_assoc.keys());
    println!("Avatar URLs:");
    println!("{:?}", avatars.values());

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

    for v in assoc.bridge_assoc.values() {
        for c in v.iter() {
            irc_client.send_join(c).expect("Could not join channel {c}");
        }
    }

    let _irc_handle = spawn(async move {
        irc_producer(irc_client, irc_buffer_reference, irc_notify).await;
    });

    // ======================================================================================
    // Relay consumer thread spawn
    let _relay_handle = spawn(async move {
        relay_consumer(
            buffer_reference,
            notify,
            shared_http,
            irc_sender,
            assoc,
            avatars
        )
        .await;
    });

    loop {}
}
