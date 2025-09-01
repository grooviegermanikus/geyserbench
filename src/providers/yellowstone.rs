use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};
use std::collections::HashSet;
use futures_util::{stream::StreamExt, sink::SinkExt};
use tokio::{sync::broadcast, task};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestPing,
    },
    prelude::SubscribeRequestFilterTransactions,
    tonic::transport::ClientTlsConfig,
};
use yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts;
use crate::{
    config::{Config, Endpoint},
    utils::{Comparator, AccountData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::GeyserProvider;

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        comparator: Arc<Mutex<Comparator>>,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_yellowstone_endpoint(
                endpoint,
                config,
                shutdown_tx,
                shutdown_rx,
                start_time,
                comparator,
            )
                .await
        })
    }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
    comparator: Arc<Mutex<Comparator>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut samples_count = 0;
    let mut accounts_seen: HashSet<Vec<u8>> = HashSet::new();

    let mut log_file = open_log_file(&endpoint.name)?;

    log::info!(
        "[{}] Connecting to endpoint: {}",
        endpoint.name,
        endpoint.url
    );

    let mut client = GeyserGrpcClient::build_from_shared(endpoint.url)?
        .x_token(Some(endpoint.x_token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    log::info!("[{}] Connected successfully", endpoint.name);

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: yellowstone_grpc_proto::geyser::CommitmentLevel = config.commitment.into();

    let mut accounts = HashMap::new();
    accounts.insert(
        "account".to_string(),
        SubscribeRequestFilterAccounts {
            owner: vec![config.account.clone()],
            ..Default::default()
        },
    );

    subscribe_tx
        .send(SubscribeRequest {
            slots: HashMap::default(),
            accounts,
            transactions: HashMap::default(),
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        })
        .await?;

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Account(acc_msg)) => {
                                if let Some(acc) = acc_msg.account {
                                    let acc_pubkey = bs58::encode(&acc.pubkey).into_string();
                                    let owned_pubkey =  bs58::encode(&acc.owner).into_string();

                                    if owned_pubkey == config.account {
                                        let timestamp = get_current_timestamp();

                                        write_log_entry(&mut log_file, timestamp, &endpoint.name, &acc_pubkey)?;

                                        let mut comp = comparator.lock().unwrap();

                                        comp.add(
                                            endpoint.name.clone(),
                                            AccountData {
                                                timestamp,
                                                account_pubkey: acc_pubkey.clone(),
                                                start_time,
                                            },
                                        );

                                        if comp.get_valid_count() == config.n_samples as usize {
                                            log::info!("Endpoint {} shutting down after {} samples seen and {} by all workers",
                                                endpoint.name, samples_count, config.n_samples);
                                            shutdown_tx.send(()).unwrap();
                                            break 'ploop;
                                        }

                                        log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, acc_pubkey);
                                        samples_count += 1;
                                    }
                                }
                            },
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            },
                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        log::error!("[{}] Error receiving message: {:?}", endpoint.name, e);
                        break;
                    },
                    None => {
                        log::info!("[{}] Stream closed", endpoint.name);
                        break;
                    }
                }
            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
