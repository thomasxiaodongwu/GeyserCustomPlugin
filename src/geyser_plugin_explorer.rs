use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::sync::Mutex;

use agave_geyser_plugin_interface::geyser_plugin_interface::{GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result, SlotStatus};
use log::{info, warn};
use once_cell::sync::Lazy;
use redis::{Commands, Connection, RedisResult};
use serde::{Deserialize, Serialize};
use serde_json;
use simplelog::{Config, LevelFilter, WriteLogger};
use solana_transaction_status::InnerInstructions;

use {
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::{Reward, RewardsAndNumPartitions, TransactionStatusMeta}
};

struct SamplePlugin;

struct RedisClient {
    connection: Connection,
}

impl RedisClient {
    fn new() -> RedisResult<Self> {
        let client = redis::Client::open("redis://3.145.46.242:6379/")?;
        let connection = client.get_connection()?;
        Ok(RedisClient { connection })
    }

    fn store_object(&mut self, key: &str, object: &Option<Vec<InnerInstructions>>) -> Result<()> {
        let json_str = serde_json::to_string(object).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        self.connection.set(key, json_str).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        self.connection.expire(key, 86400).map_err(|e| GeyserPluginError::Custom(Box::new(e)))
    }

}

static REDIS_CLIENT: Lazy<Mutex<RedisClient>> = Lazy::new(|| {
    let client = RedisClient::new().expect("Failed to create Redis client");
    info!("create redis client");
    Mutex::new(client)
});
fn is_signature_empty(signature: &Signature) -> bool {
    *signature == Signature::default()
}

impl Debug for SamplePlugin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for SamplePlugin {
    fn setup_logger(&self, logger: &'static dyn log::Log, level: log::LevelFilter) -> Result<()> {
        WriteLogger::init(
            LevelFilter::Info,
            Config::default(),
            File::create("/root/geyser-plugin/app.log").unwrap(),
        ).unwrap();
        info!("Setup the log");
        Ok(())
    }

    fn name(&self) -> &'static str {
        &"SamplePlugin"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        info!("SamplePlugin has been loaded");
        Ok(())
    }

    fn on_unload(&mut self) {}

    #[allow(unused_variables)]
    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: Slot,
        is_startup: bool,
    ) -> Result<()> {
        Ok(())
    }

    fn notify_end_of_startup(&self) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn notify_transaction(
        &self,
        info: ReplicaTransactionInfoVersions,
        slot: Slot,
    ) -> Result<()> {
        let result = match info {
            ReplicaTransactionInfoVersions::V0_0_1(info) => {
                if !is_signature_empty(&info.signature) {
                    let mut client = REDIS_CLIENT.lock().unwrap();
                    client.store_object(&info.signature.to_string(), &info.transaction_status_meta.inner_instructions)
                } else {
                    Ok(())
                }
            }
            ReplicaTransactionInfoVersions::V0_0_2(info_v2) => {
                if !is_signature_empty(&info_v2.signature) {
                    let mut client = REDIS_CLIENT.lock().unwrap();
                    client.store_object(&info_v2.signature.to_string(), &info_v2.transaction_status_meta.inner_instructions)
                }else {
                    Ok(())
                }
            }
        };

        result.map_err(|e| GeyserPluginError::Custom(Box::new(e)))
    }

    #[allow(unused_variables)]
    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = SamplePlugin;
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}