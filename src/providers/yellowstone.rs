use super::GeyserProvider;
use crate::utils::TransactionData;
use crate::{
    config::{Config, Endpoint},
    utils::{get_current_timestamp, open_log_file, write_log_entry, Comparator},
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use std::collections::HashSet;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};
use tokio::{sync::broadcast, task};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter::Filter::Memcmp;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter_memcmp::Data::Base58;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
};
use yellowstone_grpc_proto::{
    geyser::{subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestPing},
    prelude::SubscribeRequestFilterTransactions,
    tonic::transport::ClientTlsConfig,
};

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
    // endpoint name -> tx signature
    let mut tx_seen: HashSet<(String, Vec<u8>)> = HashSet::new();

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

    let accounts_whitelist = match config.account {
        Some(filter_account) => vec![filter_account],
        None => vec![]
    };

    let mut transactions = HashMap::new();
    transactions.insert(
        "transaction".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: accounts_whitelist,
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    subscribe_tx
        .send(SubscribeRequest {
            slots: HashMap::default(),
            accounts: HashMap::default(),
            transactions,
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
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction {
                                    let tx_sig = bs58::encode(&tx.signature).into_string();

                                    let is_first = tx_seen.insert((endpoint.name.clone(), tx.signature.clone()));

                                    if is_first {
                                        let timestamp = get_current_timestamp();

                                        write_log_entry(&mut log_file, timestamp, &endpoint.name, &tx_sig)?;

                                        let mut comp = comparator.lock().unwrap();

                                        comp.add(
                                            endpoint.name.clone(),
                                            TransactionData {
                                                timestamp,
                                                tx_signature: tx_sig.clone(),
                                                start_time,
                                            },
                                        );

                                        if comp.get_valid_count() == config.n_samples as usize {
                                            log::info!("Endpoint {} shutting down after {} samples seen and {} by all workers",
                                                endpoint.name, samples_count, config.n_samples);
                                            shutdown_tx.send(()).unwrap();
                                            break 'ploop;
                                        }

                                        log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, tx_sig);
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
