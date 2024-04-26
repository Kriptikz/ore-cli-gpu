use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::Engine as _;
use ore::{state::Bus, utils::AccountDeserialize};
use ore::{BUS_ADDRESSES, BUS_COUNT};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::{
    client_error::Result as ClientResult,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_program::instruction::Instruction;
use solana_program::keccak::HASH_BYTES;
use solana_program::native_token::LAMPORTS_PER_SOL;
use solana_program::pubkey::Pubkey;
use solana_program::system_instruction;
use solana_sdk::account::ReadableAccount;
use solana_sdk::keccak;
use solana_sdk::program_memory::sol_memcmp;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signer::EncodableKey;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    compute_budget::ComputeBudgetInstruction,
    keccak::{hashv, Hash as KeccakHash},
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use std::path::PathBuf;
use std::str::FromStr;
use std::{
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::sleep,
};

use crate::cu_limits::{CU_LIMIT_CLAIM, CU_LIMIT_MINE};
use crate::utils::{get_proof, get_proof_v2, get_treasury, proof_pubkey};

const SIMULATION_RETRIES: usize = 4;
const HASH_RETRIES: usize = 4;
// Odds of being selected to submit a reset tx
// const RESET_ODDS: u64 = 20;

pub struct WalletQueueMessage {
    pub wallet: (String, Option<(ore::state::Hash, keccak::Hash, u64)>),
}

pub struct TransactionQueueMessage {
    pub wallets: Vec<(String, Option<(ore::state::Hash, keccak::Hash, u64)>)>,
    pub encoded_unsigned_tx: String,
    pub hash_time_elapsed: u64,
}

pub struct TransactionResultMessage {
    pub wallets: Vec<(String, Option<(ore::state::Hash, keccak::Hash, u64)>)>,
    pub sig: String,
    pub tx_time_elapsed: u64,
    pub hash_time_elapsed: u64,
    pub failed: bool,
}

pub struct MinerV2;

impl MinerV2 {
    pub async fn claim(
        rpc_client: Arc<RpcClient>,
        send_interval: u64,
        wallets_directory_string: Option<String>,
        beneficiary: Option<String>,
        priority_fee: u64,
    ) {
        println!("MinerV2 claiming rewards.");
        let mut key_paths = vec![];

        if let Some(wallets_dir) = wallets_directory_string {
            let dir_reader = tokio::fs::read_dir(wallets_dir.clone()).await;
            if let Ok(mut dir_reader) = dir_reader {
                loop {
                    if let Ok(Some(next_entry)) = dir_reader.next_entry().await {
                        key_paths.push(next_entry.path());
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to read miner wallets directory: {}", wallets_dir);
                return;
            }
        }
        let beneficiary = match beneficiary {
            Some(beneficiary) => {
                println!("Claim beneficiary supplied: {}", beneficiary);
                Some(Pubkey::from_str(&beneficiary).expect("Failed to parse beneficiary address"))
            }
            None => None,
        };

        println!("Found {} wallets", key_paths.len());

        for key_path in key_paths.clone() {
            if let Ok(signer) = read_keypair_file(key_path.clone()) {
                println!("Starting claim for \n{}", signer.pubkey().to_string());
                println!("Key path: {}", key_path.to_str().unwrap());

                let proof = get_proof(&rpc_client, signer.pubkey()).await;
                let rewards = proof.claimable_rewards;
                let amount = rewards;

                if amount == 0 {
                    println!("No rewards to claim in this wallet.");
                    continue;
                }

                let token_account = if let Some(beneficiary) = beneficiary {
                    beneficiary
                } else {
                    MinerV2::initialize_ata(
                        rpc_client.clone(),
                        &signer,
                        priority_fee,
                        send_interval,
                    )
                    .await
                };
                println!("Proof: {:?}", proof);
                let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_CLAIM);
                let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
                let ix = ore::instruction::claim(signer.pubkey(), token_account, amount);

                println!("Building tx...");
                let mut tx = Transaction::new_with_payer(
                    &[cu_limit_ix, cu_price_ix, ix],
                    Some(&signer.pubkey()),
                );

                let (hash, last_valid_blockheight) = rpc_client
                    .get_latest_blockhash_with_commitment(rpc_client.commitment())
                    .await
                    .unwrap();

                println!("Signing tx...");
                tx.sign(&[&signer], hash);

                println!("Submitting claim transaction...");
                let send_cfg = RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    encoding: Some(UiTransactionEncoding::Base64),
                    max_retries: None,
                    min_context_slot: None,
                };
                let result = MinerV2::send_and_confirm_transaction(
                    rpc_client.clone(),
                    tx,
                    last_valid_blockheight,
                    send_interval,
                    send_cfg,
                )
                .await;

                match result {
                    Ok((sig, tx_time_elapsed)) => {
                        println!("Success: {}", sig);
                        println!("Took: {} seconds", tx_time_elapsed);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            } else {
                println!(
                    "Failed to read keypair file: {}",
                    key_path.to_str().unwrap()
                );
            }
        }
    }

    pub async fn mine(
        rpc_client: Arc<RpcClient>,
        threads: u64,
        send_interval: u64,
        batch_size: u64,
        wallets_directory_string: Option<String>,
        priority_fee: u64,
        sim_attempts: Option<u64>,
        fee_payer: Option<String>,
    ) {
        println!("MinerV2 Running...");
        let (wallet_queue_sender, mut wallet_queue_reader): (
            mpsc::Sender<WalletQueueMessage>,
            mpsc::Receiver<WalletQueueMessage>,
        ) = tokio::sync::mpsc::channel(100);
        let (tx_queue_sender, mut tx_queue_reader): (
            mpsc::Sender<TransactionQueueMessage>,
            mpsc::Receiver<TransactionQueueMessage>,
        ) = tokio::sync::mpsc::channel(100);
        let (tx_results_sender, mut tx_results_reader): (
            mpsc::Sender<TransactionResultMessage>,
            mpsc::Receiver<TransactionResultMessage>,
        ) = tokio::sync::mpsc::channel(100);

        let mut has_fee_payer = false;
        let fee_payer_string = if let Some(fee_payer) = fee_payer {
            has_fee_payer = true;
            fee_payer
        } else {
            "".to_string()
        };

        if let Some(wallets_dir) = wallets_directory_string {
            let mut key_strings = vec![];
            println!("Reading wallet directory");
            let dir_reader = tokio::fs::read_dir(wallets_dir.clone()).await;
            if let Ok(mut dir_reader) = dir_reader {
                loop {
                    if let Ok(Some(next_entry)) = dir_reader.next_entry().await {
                        if let Ok(signer) = read_keypair_file(next_entry.path().clone()) {
                            let key_string = signer.to_base58_string();
                            key_strings.push(key_string);
                        } else {
                            println!(
                                "Failed to read keypair file: {}",
                                next_entry.path().to_str().unwrap()
                            );
                        }
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to read miner wallets directory: {}", wallets_dir);
                return;
            }

            // tokio spawn threads
            // wallet queue reader thread
            let mut handles = vec![];
            let rpc_client_0 = rpc_client.clone();
            let fee_payer_string_1 = fee_payer_string.clone();
            let wallet_queue_resender = wallet_queue_sender.clone();
            let thread_handle = tokio::spawn(async move {
                let rpc_client = rpc_client_0.clone();
                let batch_size = if batch_size > 5 { 5 } else { batch_size };

                let mut bus = 0;
                let mut keys_bytes_with_hashes_and_proofs: Vec<(String, keccak::Hash, ore::state::Hash, u64)> = Vec::new();
                let mut hash_timer = SystemTime::now();
                loop {
                    let mut hash_failed = false;
                    if let Some(mssg) = wallet_queue_reader.recv().await {
                        let wallet = mssg.wallet.0;
                        let signer = Keypair::from_base58_string(&wallet);
                        //let balance = MinerV2::get_ore_display_balance(&rpc_client, signer.pubkey()).await;
                        MinerV2::register(rpc_client.clone(), &signer, send_interval, priority_fee)
                            .await;
                        println!("\nStarting hash for wallet {}", signer.pubkey());
                        let mut hash_attempts = 0;
                        loop {
                            let treasury = get_treasury(&rpc_client).await;
                            let proof = get_proof(&rpc_client, signer.pubkey()).await;
                            if let Some(last_hash) = mssg.wallet.1 {
                                println!("Wallet last hash: {}", last_hash.0);
                                if proof.hash == last_hash.0 {
                                    println!("Proof has already been hashed, potentially stale rpc data.");
                                    println!("Simulating transaction...");
                                    return Err("DoubleHashed".to_string());

                                    let (_hash, last_valid_blockheight) = rpc_client
                                        .get_latest_blockhash_with_commitment(
                                            rpc_client.commitment(),
                                        )
                                        .await
                                        .unwrap();
                                        let bus = MinerV2::get_bus(&rpc_client, bus)
                                            .await
                                            .expect("Should successfully get bus.");
                                        let bus_rewards = (bus.rewards as f64) / (10f64.powf(ore::TOKEN_DECIMALS as f64));
                                        let ix_mine = ore::instruction::mine(
                                            signer.pubkey(),
                                            BUS_ADDRESSES[bus.id as usize],
                                            last_hash.0.into(), //TODO FIX 
                                            last_hash.2,
                                        );
                                        let tx = Transaction::new_with_payer(&[ix_mine], None);
                                    let sim_res = rpc_client
                                        .simulate_transaction_with_config(
                                            &tx,
                                            RpcSimulateTransactionConfig {
                                                sig_verify: false,
                                                replace_recent_blockhash: true,
                                                commitment: Some(rpc_client.commitment()),
                                                encoding: Some(UiTransactionEncoding::Base64),
                                                accounts: None,
                                                min_context_slot: Some(last_valid_blockheight),
                                                inner_instructions: false,
                                            },
                                        )
                                        .await;
                                    match sim_res {
                                        Ok(sim_res) => {
                                            if let Some(err) = sim_res.value.err {
                                                println!("Simulaton error: {:?}", err);
                                            } else {
                                                println!("Simulaton successful.");
                                                println!("Adding wallet and hash to tx bundler.");
                                                let data = (wallet.clone(),  last_hash.1, last_hash.0, last_hash.2);
                                                keys_bytes_with_hashes_and_proofs.push(data);
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            println!("Simulaton error: {:?}", err);
                                        }
                                    }

                                    hash_failed = true;
                                    let w = WalletQueueMessage {
                                        wallet: (wallet, Some((last_hash.0, last_hash.1, last_hash.2))),
                                    };
                                    if let Ok(_) = wallet_queue_resender.send(w).await {
                                        println!("Sent wallet to back of queue with cached proof.");
                                    } else {
                                        println!("Failed to send wallet to queue.");
                                    }
                                    break;
                                } else {
                                    println!("New hash proof found, starting hasher.");
                                }
                            } else {
                                println!("No last hash. starting hasher.")
                            }
                            let st = wallet.clone();
                            let signer = Keypair::from_base58_string(&st);
                            let hash_and_pubkey = [(
                                solana_sdk::keccak::Hash::new_from_array(proof.hash.0),
                                signer.pubkey(),
                            )];
                            let (next_hash, nonce) = MinerV2::find_next_hash_par(
                                &signer,
                                &treasury.difficulty.into(),
                                &hash_and_pubkey,
                                0,
                            )
                            .await;
                            hash_attempts += 1;
                            println!("{} {}", next_hash, nonce);
                            let data = (wallet.clone(), next_hash, proof.hash, nonce);

                            let proof_ = get_proof(&rpc_client, signer.pubkey()).await;
                            if !MinerV2::validate_hash(
                                ore::state::Hash(next_hash.to_bytes()).into(),
                                proof_.hash.into(),
                                signer.pubkey(),
                                nonce,
                                treasury.difficulty.into(),
                            ) {
                                println!("Hash already validated! An earlier transaction must have landed.");
                                hash_failed = true;
                                // TODO push wallet to back of queue
                                let w = WalletQueueMessage {
                                    wallet: (wallet, Some((proof.hash, next_hash, nonce))),
                                };
                                if let Ok(_) = wallet_queue_resender.send(w).await {
                                    println!("Sent wallet to back of queue with cached proof.");
                                } else {
                                    println!("Failed to send wallet to queue.");
                                }
                                break;
                            } else {
                                keys_bytes_with_hashes_and_proofs.push(data);
                                break;
                            }
                        }
                    }

                    if hash_failed {
                        continue;
                    }

                    if keys_bytes_with_hashes_and_proofs.len() as u64 == batch_size {
                        // build transaction from 5 keys and reset data
                        println!("\nHashing complete.");
                        let hash_time = hash_timer
                            .elapsed()
                            .expect("timer elapsed failed")
                            .as_secs();
                        println!("Building transaction...");
                        // Reset epoch, if needed
                        //let treasury = get_treasury(&rpc_client).await;
                        //let clock = get_clock_account(&rpc_client).await;
                        //let threshold = treasury.last_reset_at.saturating_add(EPOCH_DURATION);
                        // can't use thread_rng() across thread safetly
                        //let mut rng = rand::thread_rng();

                        //if clock.unix_timestamp.ge(&threshold) {
                        //    // There are a lot of miners right now, so randomly select into submitting tx
                        //    if rng.gen_range(0..RESET_ODDS).eq(&0) {
                        //        println!("Sending epoch reset transaction...");
                        //        let signer =
                        //            Keypair::from_base58_string(&keys_bytes_with_hashes[0].0);
                        //        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(
                        //            CU_LIMIT_RESET,
                        //        );
                        //        let cu_price_ix =
                        //            ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
                        //        let reset_ix = ore::instruction::reset(signer.pubkey());
                        //        MinerV2::send_and_confirm(
                        //            &signer,
                        //            rpc_client.clone(),
                        //            &[cu_limit_ix, cu_price_ix, reset_ix],
                        //            false,
                        //            send_interval,
                        //            priority_fee,
                        //        )
                        //        .await
                        //        .ok();
                        //    }
                        //}
                        let wallet_count = keys_bytes_with_hashes_and_proofs.len();
                        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(
                            CU_LIMIT_MINE * wallet_count as u32,
                        );
                        let cu_price_ix =
                            ComputeBudgetInstruction::set_compute_unit_price(priority_fee);

                        let mut ixs = vec![];
                        ixs.push(cu_limit_ix);
                        ixs.push(cu_price_ix);
                        let bus = MinerV2::get_bus(&rpc_client, bus)
                            .await
                            .expect("Should successfully get bus.");
                        let bus_rewards =
                            (bus.rewards as f64) / (10f64.powf(ore::TOKEN_DECIMALS as f64));
                        println!("Will be sending on bus {} ({} ORE)", bus.id, bus_rewards);

                        let mut keypairs = vec![];
                        let mut wallets_batch = vec![];
                        for (key_bytes, next_hash, proof_hash, nonce) in
                            keys_bytes_with_hashes_and_proofs.clone()
                        {
                            wallets_batch.push((key_bytes.clone(), Some((proof_hash, next_hash, nonce))));
                            let signer = Keypair::from_base58_string(&key_bytes);
                            keypairs.push(Keypair::from_base58_string(&key_bytes));
                            let ix_mine = ore::instruction::mine(
                                signer.pubkey(),
                                BUS_ADDRESSES[bus.id as usize],
                                next_hash.into(),
                                nonce,
                            );
                            ixs.push(ix_mine);
                        }

                        let fee_payer = if has_fee_payer {
                            let file_path = PathBuf::from_str(&fee_payer_string_1);
                            println!("File path: {:?}", file_path);
                            match file_path {
                                Ok(f) => {
                                    let key = Keypair::read_from_file(f).unwrap();
                                    println!("Found fee_payer: {}", key.pubkey().to_string());
                                    key
                                }
                                Err(_) => Keypair::from_base58_string(
                                    &keys_bytes_with_hashes_and_proofs[0].0,
                                ),
                            }
                        } else {
                            Keypair::from_base58_string(&keys_bytes_with_hashes_and_proofs[0].0)
                        };

                        let tx =
                            Transaction::new_with_payer(ixs.as_slice(), Some(&fee_payer.pubkey()));

                        println!("Sending unsigned tx to queue...");
                        let serialized_tx = bincode::serialize(&tx).unwrap();
                        let encoded_unsigned_tx = BASE64.encode(serialized_tx);
                        let tqm = TransactionQueueMessage {
                            wallets: wallets_batch.clone(),
                            encoded_unsigned_tx,
                            hash_time_elapsed: hash_time,
                        };
                        if let Ok(_) = tx_queue_sender.send(tqm).await {
                            println!("Sent tx to be processed.");
                        } else {
                            println!(
                                "Failed to send tx to be processed. Tx Queue full? Dev help pls."
                            );
                        }
                        keys_bytes_with_hashes_and_proofs = vec![];
                        hash_timer = SystemTime::now();
                    } else {
                    }

                    bus += 1;

                    if bus >= BUS_COUNT {
                        bus = 0;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            });

            handles.push(thread_handle);

            // tx queue processor thread
            let rpc_client_1 = rpc_client.clone();
            let fee_payer_string_2 = fee_payer_string.clone();
            let thread_handle = tokio::spawn(async move {
                let rpc_client = rpc_client_1.clone();
                loop {
                    if let Some(mssg) = tx_queue_reader.recv().await {
                        let serialized_tx =
                            BASE64.decode(mssg.encoded_unsigned_tx.clone()).unwrap();
                        let mut tx: Transaction = bincode::deserialize(&serialized_tx).unwrap();

                        if let Some(sim_attempts) = sim_attempts {
                            for _i in 0..sim_attempts {
                                let (_hash, last_valid_blockheight) = rpc_client
                                    .get_latest_blockhash_with_commitment(rpc_client.commitment())
                                    .await
                                    .unwrap();

                                let sim_res = rpc_client
                                    .simulate_transaction_with_config(
                                        &tx,
                                        RpcSimulateTransactionConfig {
                                            sig_verify: false,
                                            replace_recent_blockhash: true,
                                            commitment: Some(rpc_client.commitment()),
                                            encoding: Some(UiTransactionEncoding::Base64),
                                            accounts: None,
                                            min_context_slot: Some(last_valid_blockheight),
                                            inner_instructions: false,
                                        },
                                    )
                                    .await;
                                match sim_res {
                                    Ok(sim_res) => {
                                        if let Some(err) = sim_res.value.err {
                                            println!("Simulaton error: {:?}", err);
                                        } else {
                                            println!("Simulaton successful.");
                                        }
                                    }
                                    Err(err) => {
                                        println!("Simulaton error: {:?}", err);
                                    }
                                }
                            }
                        }

                        let (hash, last_valid_blockheight) = rpc_client
                            .get_latest_blockhash_with_commitment(rpc_client.commitment())
                            .await
                            .unwrap();

                        let wallets = mssg.wallets.clone();
                        let fee_payer = if has_fee_payer {
                            let file_path = PathBuf::from_str(&fee_payer_string_2);
                            match file_path {
                                Ok(f) => Keypair::read_from_file(f).unwrap(),
                                Err(_) => Keypair::from_base58_string(&mssg.wallets[0].0),
                            }
                        } else {
                            Keypair::from_base58_string(&mssg.wallets[0].0)
                        };
                        let balance = rpc_client.get_balance(&fee_payer.pubkey()).await;

                        if let Ok(balance) = balance {
                            println!("Fee Payer: {}", fee_payer.pubkey());
                            println!(
                                "Fee Payer balance: {}",
                                (balance as f64) / (LAMPORTS_PER_SOL as f64)
                            );
                            if balance < 300_000 {
                                println!("Fee bayer balance is too low. Transaction may fail.");
                                println!("Please fund fee payer.");
                                println!("Fee Payer: {}", fee_payer.pubkey());
                                println!(
                                    "Fee Payer balance: {}",
                                    (balance as f64) / (LAMPORTS_PER_SOL as f64)
                                );
                            }
                        } else {
                            println!("Failed to load fee payer balance, transaction may fail.");
                        }

                        println!("Signing tx...");

                        tx.partial_sign(&[&fee_payer], hash);

                        for wallet in &wallets {
                            let keypair = Keypair::from_base58_string(&wallet.0);

                            if keypair.pubkey() == fee_payer.pubkey() {
                                continue;
                            } else {
                                tx.partial_sign(&[&keypair], hash);
                            }
                        }

                        println!("Sending tx every {} milliseconds until confirmation or blockhash expires.", send_interval);
                        let send_cfg = RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentLevel::Confirmed),
                            encoding: Some(UiTransactionEncoding::Base64),
                            max_retries: None,
                            min_context_slot: None,
                        };
                        let result = MinerV2::send_and_confirm_transaction(
                            rpc_client.clone(),
                            tx,
                            last_valid_blockheight,
                            send_interval,
                            send_cfg,
                        )
                        .await;

                        match result {
                            Ok((sig, tx_time_elapsed)) => {
                                println!("Transaction Confirmed!");
                                if let Ok(_) = tx_results_sender
                                    .send(TransactionResultMessage {
                                        wallets: mssg.wallets.clone(),
                                        sig: sig.to_string(),
                                        tx_time_elapsed,
                                        hash_time_elapsed: mssg.hash_time_elapsed,
                                        failed: false,
                                    })
                                    .await
                                {
                                } else {
                                    println!(
                                        "Failed to send tx result. Tx Result Queue full? Dev help pls."
                                    );
                                }
                            }
                            Err(e) => {
                                println!("Error: {}", e);
                                if let Ok(_) = tx_results_sender
                                    .send(TransactionResultMessage {
                                        wallets: mssg.wallets.clone(),
                                        sig: "failed".to_string(),
                                        tx_time_elapsed: 0,
                                        hash_time_elapsed: mssg.hash_time_elapsed,
                                        failed: true,
                                    })
                                    .await
                                {
                                    println!("Sent tx result.");
                                } else {
                                    println!(
                                        "Failed to send tx result. Tx Result Queue full? Dev help pls."
                                    );
                                }
                            }
                        }
                    }
                    sleep(Duration::from_millis(500)).await;
                }
            });
            handles.push(thread_handle);

            // tx results thread
            let wallet_queue_sender_1 = wallet_queue_sender.clone();

            let results_pubkeys: Vec<Pubkey> = key_strings
                .iter()
                .map(|key_str| {
                    let key = Keypair::from_base58_string(key_str);
                    key.pubkey()
                })
                .collect();

            println!("Loading All wallet balances");

            let response = rpc_client.get_multiple_accounts(&results_pubkeys).await;

            let mut total_lamports = 0;
            if let Ok(results) = response {
                for res in results {
                    if let Some(acc) = res {
                        total_lamports += acc.lamports();
                    }
                }
            }

            println!("Total Sol: {}", lamports_to_sol(total_lamports));

            println!("Loading ore balances and rewards...");
            let mut total_ore_balance = 0.0;
            let mut total_ore_rewards_claimable = 0.0;
            for pubkey in &results_pubkeys {
                let pubkey = pubkey.to_owned();

                sleep(Duration::from_millis(200)).await;
                let ore_balance = MinerV2::get_ore_display_balance_v2(&rpc_client, pubkey).await;
                total_ore_balance += ore_balance;

                sleep(Duration::from_millis(200)).await;
                let proof = get_proof(&rpc_client, pubkey).await;
                let rewards = proof.claimable_rewards;

                total_ore_rewards_claimable += (rewards as f64) / 1000000000.0;
            }
            println!("Total Ore Balance: {}", total_ore_balance);
            println!("Total Ore Claimable: {}", total_ore_rewards_claimable);

            let thread_handle = tokio::spawn(async move {
                let wallet_queue = wallet_queue_sender_1.clone();
                let mut tx_times = vec![];
                let mut hash_times = vec![];
                let mut total_times = vec![];

                let current_time = SystemTime::now();

                loop {
                    if let Some(mssg) = tx_results_reader.recv().await {
                        if mssg.failed {
                            println!("Transaction failed, adding wallets back into queue.");
                        } else {
                            println!("Transaction was Successfull!");
                            println!("Sig: {}", mssg.sig);
                            println!("Took {} seconds", mssg.tx_time_elapsed);
                            // append running results stats
                            tx_times.push(mssg.tx_time_elapsed);
                            hash_times.push(mssg.hash_time_elapsed);
                            total_times.push(mssg.tx_time_elapsed + mssg.hash_time_elapsed);
                            // log data
                            println!(
                                "Miner run time: {} seconds",
                                current_time.elapsed().unwrap().as_secs()
                            );
                            println!("TX TIMES COUNT: {:?}", tx_times.len());
                            println!("TX TIMES: \n{:?}", tx_times);
                            println!("HASH TIMES: \n{:?}", hash_times);
                            println!("TOTAL TIMES: \n{:?}", total_times);
                        }
                        for wallet in mssg.wallets {
                            let w = WalletQueueMessage { wallet };
                            if let Ok(_) = wallet_queue.send(w).await {
                                println!("Successfully sent wallet to queue. Hashing continue...");
                            } else {
                                println!("Failed to send wallet to queue.");
                            }
                        }
                    }
                    sleep(Duration::from_millis(500)).await;
                }
            });

            println!("Sending wallets to queue");
            for wallet in key_strings {
                let w = WalletQueueMessage {
                    wallet: (wallet, None),
                };
                if let Err(_) = wallet_queue_sender.send(w).await {
                    println!("Failed to send wallet to queue.");
                }
            }

            handles.push(thread_handle);
            for handle in handles {
                handle.await.unwrap();
            }

            return;
        }
    }

    pub async fn send_sol(
        rpc_client: Arc<RpcClient>,
        sender_wallet: String,
        wallets_directory_string: Option<String>,
        send_interval: u64,
        amount: Option<u64>,
    ) {
        let amount = if let Some(a) = amount {
            a
        } else {
            println!("Please provide lamports amount to send `--amount 1_000_000`.");
            return;
        };

        println!("Wallet Path: {}", sender_wallet);
        let sender;
        if let Ok(signer) = read_keypair_file(sender_wallet.clone()) {
            println!(
                "\nLoaded Sender wallet pubkey: \n{}",
                signer.pubkey().to_string()
            );
            sender = signer;
        } else {
            println!("Sender wallet required.");
            return;
        }

        let mut key_paths = vec![];
        if let Some(wallets_dir) = wallets_directory_string {
            let dir_reader = tokio::fs::read_dir(wallets_dir.clone()).await;
            if let Ok(mut dir_reader) = dir_reader {
                loop {
                    if let Ok(Some(next_entry)) = dir_reader.next_entry().await {
                        key_paths.push(next_entry.path());
                    } else {
                        break;
                    }
                }
            } else {
                println!(
                    "Failed to read receiving wallets directory: {}",
                    wallets_dir
                );
                return;
            }
        }

        println!("Found {} wallets", key_paths.len());

        for key_path in key_paths.clone() {
            println!("Wallet Path: {}", key_path.to_str().unwrap());
            if let Ok(signer) = read_keypair_file(key_path.clone()) {
                println!("\nLoaded wallet pubkey: \n{}", signer.pubkey().to_string());

                println!("Send Sol");
                println!("Building Transaction...");

                let ix = system_instruction::transfer(&sender.pubkey(), &signer.pubkey(), amount);
                println!("Signing Transaction...");
                let mut tx = Transaction::new_with_payer(&[ix], Some(&sender.pubkey()));

                let (hash, last_valid_blockheight) = rpc_client
                    .get_latest_blockhash_with_commitment(rpc_client.commitment())
                    .await
                    .unwrap();

                println!("Signing tx...");
                tx.sign(&[&sender], hash);

                println!("Sending Transaction...");
                let send_cfg = RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    encoding: Some(UiTransactionEncoding::Base64),
                    max_retries: None,
                    min_context_slot: None,
                };
                let result = MinerV2::send_and_confirm_transaction(
                    rpc_client.clone(),
                    tx,
                    last_valid_blockheight,
                    send_interval,
                    send_cfg,
                )
                .await;

                match result {
                    Ok((_sig, _tx_time_elapsed)) => {
                        println!("Transaction Confirmed!");
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }

                println!("Checking for next wallet.");
            } else {
                println!(
                    "Failed to read keypair file: {}",
                    key_path.to_str().unwrap()
                );
            }
        }

        println!("Wallets funded!");
    }

    pub async fn wallets(rpc_client: Arc<RpcClient>, wallets_directory_string: Option<String>) {
        let mut key_paths = vec![];
        if let Some(wallets_dir) = wallets_directory_string {
            let dir_reader = tokio::fs::read_dir(wallets_dir.clone()).await;
            if let Ok(mut dir_reader) = dir_reader {
                loop {
                    if let Ok(Some(next_entry)) = dir_reader.next_entry().await {
                        key_paths.push(next_entry.path());
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to read miner wallets directory: {}", wallets_dir);
                return;
            }
        }

        println!("Found {} wallets", key_paths.len());

        for key_path in key_paths.clone() {
            if let Ok(signer) = read_keypair_file(key_path.clone()) {
                println!("\nLoaded wallet pubkey: \n{}", signer.pubkey().to_string());
                println!("Wallet Path: {}", key_path.to_str().unwrap());

                let proof = get_proof_v2(&rpc_client, signer.pubkey()).await;
                match proof {
                    Ok(proof) => {
                        let rewards = proof.claimable_rewards;
                        let amount = rewards;

                        if amount == 0 {
                            println!("No rewards to claim in this wallet.");
                            continue;
                        }

                        let balance =
                            MinerV2::get_ore_display_balance(&rpc_client, signer.pubkey()).await;
                        let rewards = (proof.claimable_rewards as f64)
                            / (10f64.powf(ore::TOKEN_DECIMALS as f64));
                        println!("Balance: {} ORE", balance);
                        println!("Claimable: {} ORE", rewards);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            } else {
                println!(
                    "Failed to read keypair file: {}",
                    key_path.to_str().unwrap()
                );
            }
        }
    }

    pub async fn send_and_confirm_transaction(
        rpc_client: Arc<RpcClient>,
        tx: Transaction,
        last_valid_blockheight: u64,
        send_interval: u64,
        send_cfg: RpcSendTransactionConfig,
    ) -> Result<(Signature, u64), String> {
        let tx_sent_at = SystemTime::now();

        let (tx_result_sender, mut tx_result_receiver): (
            Sender<Result<Signature, String>>,
            Receiver<Result<Signature, String>>,
        ) = mpsc::channel(100);

        // creates channel for getting sigs to confirm
        let (sig_checks_sender, mut sig_checks_receiver): (
            Sender<Result<Signature, String>>,
            Receiver<Result<Signature, String>>,
        ) = mpsc::channel(100);

        // confirmation checks thread
        let c_client = rpc_client.clone();
        let confirms_thread_handle = tokio::spawn(async move {
            let client = c_client;
            let mut sigs: Vec<Signature> = vec![];
            // receive sig_checks and add them to hashmap if new
            loop {
                if let Some(new_sig) = sig_checks_receiver.recv().await {
                    if let Ok(new_sig) = new_sig {
                        let mut is_new = true;
                        for sig in sigs.iter() {
                            if sig.to_string() == new_sig.to_string() {
                                is_new = false;
                            }
                        }

                        if is_new {
                            sigs.push(new_sig);
                        }
                    }
                }
                // really should only have one sig here though
                //for sig in sigs.iter {}
                // confirmation checks
                match client.get_signature_statuses(&sigs).await {
                    Ok(signature_statuses) => {
                        for signature_status in signature_statuses.value {
                            if let Some(signature_status) = signature_status.as_ref() {
                                if signature_status.confirmation_status.is_some() {
                                    let current_commitment =
                                        signature_status.confirmation_status.as_ref().unwrap();
                                    match current_commitment {
                                        TransactionConfirmationStatus::Processed => {}
                                        TransactionConfirmationStatus::Confirmed
                                        | TransactionConfirmationStatus::Finalized => {
                                            println!("Transaction landed!");
                                            println!("STATUS: {:?}", signature_status);
                                            match signature_status.status {
                                                Ok(_) => {
                                                    let _ =
                                                        tx_result_sender.send(Ok(sigs[0])).await;
                                                    return;
                                                }
                                                Err(_) => {
                                                    let _ = tx_result_sender
                                                        .send(
                                                            Err("Transaction Failed.".to_string()),
                                                        )
                                                        .await;
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Handle confirmation errors
                    Err(err) => {
                        println!("{:?}", err.kind().to_string());
                    }
                }

                // hash expiration checks
                let current_blockheight = client.get_block_height().await.unwrap();
                //println!("Last valid blockheight: {}", last_valid_blockheight);
                //println!("Current blockheight: {}", current_blockheight);

                if current_blockheight > last_valid_blockheight {
                    let err = Err("Last valid blockheight exceeded!".to_string());
                    let _ = tx_result_sender.send(err).await;
                    return;
                }
                // sleep 500ms to allow confirmations to potentially land
                sleep(Duration::from_millis(500)).await;
            }
        });

        let client = rpc_client.clone();

        let sender_thread_handle = tokio::spawn(async move {
            let sig_checks_sender1 = sig_checks_sender.clone();

            let tx1 = tx.clone();
            loop {
                let client = client.clone();

                let sig_checks_sender = sig_checks_sender1.clone();
                let tx = tx1.clone();
                tokio::spawn(async move {
                    // send off tx and get sig
                    let sig_checks_sender = sig_checks_sender.clone();

                    if let Ok(sig) = client.send_transaction_with_config(&tx, send_cfg).await {
                        match sig_checks_sender.send(Ok(sig)).await {
                            Ok(_) => {}
                            Err(_) => {
                                return;
                            }
                        }
                    } else {
                        // Program will still keep trying until last_valid_blockheight expires
                        // Transactions that get Err from RPC can still land.
                        // TODO: log errors to see what they are and if any other handling needs to
                        // be done.
                    };
                });

                // sleep 100ms (allowing 10 sends per second)
                sleep(Duration::from_millis(send_interval)).await;
            }
        });

        // wait for a tx result to come through
        let res = tx_result_receiver.recv().await.unwrap();
        confirms_thread_handle.abort();
        sender_thread_handle.abort();
        let tx_time_elapsed = tx_sent_at.elapsed().unwrap().as_secs();

        match res {
            Ok(res) => {
                return Ok((res, tx_time_elapsed));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub async fn register(
        rpc_client: Arc<RpcClient>,
        signer: &Keypair,
        send_interval: u64,
        _priority_fee: u64,
    ) {
        // Return early if miner is already registered
        let proof_address = proof_pubkey(signer.pubkey());
        let client = rpc_client.clone();
        if client.get_account(&proof_address).await.is_ok() {
            return;
        }

        // Sign and send transaction.
        println!("Generating challenge...");
        loop {
            let ix = ore::instruction::register(signer.pubkey());
            let mut tx = Transaction::new_with_payer(&[ix.clone()], Some(&signer.pubkey()));
            let (hash, last_valid_blockheight) = rpc_client
                .get_latest_blockhash_with_commitment(rpc_client.commitment())
                .await
                .unwrap();
            tx.sign(&[&signer], hash);

            println!("Simulating tx...");
            let sim_res = rpc_client
                .simulate_transaction_with_config(
                    &tx,
                    RpcSimulateTransactionConfig {
                        sig_verify: true,
                        replace_recent_blockhash: false,
                        commitment: Some(rpc_client.commitment()),
                        encoding: Some(UiTransactionEncoding::Base64),
                        accounts: None,
                        min_context_slot: Some(last_valid_blockheight),
                        inner_instructions: true,
                    },
                )
                .await;
            match sim_res {
                Ok(sim_res) => {
                    if let Some(err) = sim_res.value.err {
                        println!("Simulaton error: {:?}", err);
                    } else {
                        println!("Simulaton succeeded");
                    }
                }
                Err(err) => {
                    println!("Simulaton error: {:?}", err);
                }
            }

            println!(
                "Sending signed tx every {} milliseconds until Confirmed or blockhash expires...",
                send_interval
            );
            let send_cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Confirmed),
                encoding: Some(UiTransactionEncoding::Base64),
                max_retries: None,
                min_context_slot: None,
            };
            let result = MinerV2::send_and_confirm_transaction(
                rpc_client.clone(),
                tx,
                last_valid_blockheight,
                send_interval,
                send_cfg,
            )
            .await;

            match result {
                Ok((sig, tx_time_elapsed)) => {
                    println!("Success: {}", sig);
                    println!("Took: {} seconds", tx_time_elapsed);
                    break;
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }
    }

    async fn find_next_hash_par(
        signer: &Keypair,
        difficulty: &solana_sdk::keccak::Hash,
        hash_and_pubkey: &[(solana_sdk::keccak::Hash, Pubkey)],
        threads: usize,
    ) -> (KeccakHash, u64) {
        let found_solution = Arc::new(AtomicBool::new(false));
        let solution = Arc::new(Mutex::<(KeccakHash, u64)>::new((
            KeccakHash::new_from_array([0; 32]),
            0,
        )));
        let pubkey = signer.pubkey();

        let mut child = tokio::process::Command::new("/root/bundle/linux")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("nonce_worker failed to spawn");

        if let Some(mut stdin) = child.stdin.take() {
            let mut data_to_send = Vec::new();

            // Add difficulty bytes
            data_to_send.extend_from_slice(difficulty.as_ref());

            // Add hash and pubkey bytes
            for (hash, pubkey) in hash_and_pubkey {
                data_to_send.extend_from_slice(hash.as_ref());
                data_to_send.extend_from_slice(pubkey.as_ref());
            }

            // Optionally prepend the number of threads or any other control data
            // Here, we send the number of threads as the first byte, if required by your application
            let mut final_data = Vec::new();
            final_data.push(0 as u8);
            final_data.extend_from_slice(&data_to_send);

            // Write all bytes in one go
            stdin.write_all(&final_data).await.unwrap();

            // Dropping stdin to close it, signaling the end of input
            drop(stdin);
        }

        let output = child.wait_with_output().await.unwrap().stdout;
        let mut results = vec![];
        let chunks = output.chunks(40);
        for chunk in chunks {
            if chunk.len() < 40 {
                println!("Incomplete data chunk received, length: {}", chunk.len());
                continue; // Skip this chunk or handle it according to your needs
            }

            let hash = solana_sdk::keccak::Hash(chunk[..32].try_into().unwrap());
            let nonce = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
            results.push((hash, nonce));
        }

        results
            .get(0)
            .cloned()
            .ok_or_else(|| "No valid results were found".to_string())
            .expect("REASON")
    }

    pub async fn send_and_confirm(
        signer: &Keypair,
        rpc_client: Arc<RpcClient>,
        ixs: &[Instruction],
        dynamic_cus: bool,
        send_interval: u64,
        priority_fee: u64,
    ) -> Result<(Signature, u64), String> {
        let client = rpc_client.clone();

        // Return error if balance is zero
        let balance = client.get_balance(&signer.pubkey()).await.unwrap();
        if balance <= 0 {
            return Err("Insufficient Sol balance".to_string());
            // return Err(ClientError {
            //     request: None,
            //     kind: ClientErrorKind::Custom("Insufficient SOL balance".into()),
            // });
        }

        // Build tx
        let (_hash, slot) = client
            .get_latest_blockhash_with_commitment(rpc_client.commitment())
            .await
            .unwrap();
        let send_cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Confirmed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: None,
            min_context_slot: None,
        };
        let mut tx = Transaction::new_with_payer(ixs, Some(&signer.pubkey()));

        // Simulate tx
        let mut sim_attempts = 0;
        'simulate: loop {
            let sim_res = client
                .simulate_transaction_with_config(
                    &tx,
                    RpcSimulateTransactionConfig {
                        sig_verify: false,
                        replace_recent_blockhash: true,
                        commitment: Some(rpc_client.commitment()),
                        encoding: Some(UiTransactionEncoding::Base64),
                        accounts: None,
                        min_context_slot: Some(slot),
                        inner_instructions: false,
                    },
                )
                .await;
            match sim_res {
                Ok(sim_res) => {
                    if let Some(err) = sim_res.value.err {
                        println!("Simulaton error: {:?}", err);
                        sim_attempts += 1;
                    } else if let Some(units_consumed) = sim_res.value.units_consumed {
                        if dynamic_cus {
                            println!("Dynamic CUs: {:?}", units_consumed);
                            let cu_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(
                                units_consumed as u32 + 1000,
                            );
                            let cu_price_ix =
                                ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
                            let mut final_ixs = vec![];
                            final_ixs.extend_from_slice(&[cu_budget_ix, cu_price_ix]);
                            final_ixs.extend_from_slice(ixs);
                            tx = Transaction::new_with_payer(&final_ixs, Some(&signer.pubkey()));
                        }
                        break 'simulate;
                    }
                }
                Err(err) => {
                    println!("Simulaton error: {:?}", err);
                    sim_attempts += 1;
                }
            }

            // Abort if sim fails
            if sim_attempts.gt(&SIMULATION_RETRIES) {
                return Err("Sim failed".to_string());
                // return Err(ClientError {
                //     request: None,
                //     kind: ClientErrorKind::Custom("Simulation failed".into()),
                // });
            }
        }

        // Update hash before sending transactions
        let (hash, last_valid_blockheight) = client
            .get_latest_blockhash_with_commitment(rpc_client.commitment())
            .await
            .unwrap();

        // Submit tx
        tx.sign(&[&signer], hash);
        let tx_signed_unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        // let mut sigs = vec![];

        // creates channel for sending the final tx result,
        //     Result will be Ok(sig) or Err("blockhash expired")
        let (tx_result_sender, mut tx_result_receiver): (
            Sender<Result<Signature, String>>,
            Receiver<Result<Signature, String>>,
        ) = mpsc::channel(100);

        // creates channel for getting sigs to confirm
        let (sig_checks_sender, mut sig_checks_receiver): (
            Sender<Result<Signature, String>>,
            Receiver<Result<Signature, String>>,
        ) = mpsc::channel(100);

        // confirmation checks thread
        let c_client = client.clone();
        let confirms_thread_handle = tokio::spawn(async move {
            let client = c_client;
            let mut sigs: Vec<Signature> = vec![];
            // receive sig_checks and add them to hashmap if new
            loop {
                if let Some(new_sig) = sig_checks_receiver.recv().await {
                    if let Ok(new_sig) = new_sig {
                        let mut is_new = true;
                        for sig in sigs.iter() {
                            if sig.to_string() == new_sig.to_string() {
                                is_new = false;
                            }
                        }

                        if is_new {
                            sigs.push(new_sig);
                        }
                    }
                }
                // really should only have one sig here though
                //for sig in sigs.iter {}
                // confirmation checks
                match client.get_signature_statuses(&sigs).await {
                    Ok(signature_statuses) => {
                        for signature_status in signature_statuses.value {
                            if let Some(signature_status) = signature_status.as_ref() {
                                if signature_status.confirmation_status.is_some() {
                                    let current_commitment =
                                        signature_status.confirmation_status.as_ref().unwrap();
                                    match current_commitment {
                                        TransactionConfirmationStatus::Processed => {}
                                        TransactionConfirmationStatus::Confirmed
                                        | TransactionConfirmationStatus::Finalized => {
                                            println!("Transaction landed!");
                                            let _ = tx_result_sender.send(Ok(sigs[0])).await;
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Handle confirmation errors
                    Err(err) => {
                        println!("{:?}", err.kind().to_string());
                    }
                }

                // hash expiration checks
                let current_blockheight = client.get_block_height().await.unwrap();
                if current_blockheight > last_valid_blockheight {
                    let err = Err("Last valid blockheight exceeded!".to_string());
                    let _ = tx_result_sender.send(err).await;
                    return;
                }

                // sleep 500ms to allow confirmations to potentially land
                sleep(Duration::from_millis(500)).await;
            }
        });

        let sender_thread_handle = tokio::spawn(async move {
            let sig_checks_sender = sig_checks_sender.clone();
            loop {
                let sig_checks_sender = sig_checks_sender.clone();
                let tx = tx.clone();
                let client = client.clone();
                tokio::spawn(async move {
                    // send off tx and get sig
                    let sig_checks_sender = sig_checks_sender.clone();

                    if let Ok(sig) = client.send_transaction_with_config(&tx, send_cfg).await {
                        match sig_checks_sender.send(Ok(sig)).await {
                            Ok(_) => {}
                            Err(_) => {
                                return;
                            }
                        }
                    } else {
                        // Program will still keep trying until last_valid_blockheight expires
                        // Transactions that get Err from RPC can still land.
                        // TODO: log errors to see what they are and if any other handling needs to
                        // be done.
                    };
                });
                // sleep 100ms (allowing 10 sends per second)
                sleep(Duration::from_millis(send_interval)).await;
            }
        });

        // wait for a tx result to come through
        let res = tx_result_receiver.recv().await.unwrap();
        confirms_thread_handle.abort();
        sender_thread_handle.abort();
        let tx_finished_unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let tx_time_elapsed = tx_finished_unix_ts - tx_signed_unix_ts;

        match res {
            Ok(res) => {
                return Ok((res, tx_time_elapsed));
            }
            Err(_) => {
                return Err("Blockheight exceeded".to_string());
                // return Err(ClientError {
                //     request: None,
                //     kind: ClientErrorKind::Custom("Blockheight Exceeded for this signed transaction".into()),
                // });
            }
        }

        //return Err(ClientError {
        //    request: None,
        //    kind: ClientErrorKind::Custom("Max retries".into()),
        //});
    }

    pub async fn get_bus(rpc_client: &RpcClient, id: usize) -> ClientResult<Bus> {
        let client = rpc_client;
        let data = client.get_account_data(&BUS_ADDRESSES[id]).await?;
        Ok(*Bus::try_from_bytes(&data).unwrap())
    }

    pub async fn get_ore_display_balance_v2(client: &RpcClient, pubkey: Pubkey) -> f64 {
        let token_account_address =
            spl_associated_token_account::get_associated_token_address(&pubkey, &ore::MINT_ADDRESS);
        match client.get_token_account(&token_account_address).await {
            Ok(token_account) => {
                if let Some(token_account) = token_account {
                    if let Some(amount) = token_account.token_amount.ui_amount {
                        amount
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            Err(_) => 0.0,
        }
    }

    pub async fn get_ore_display_balance(client: &RpcClient, pubkey: Pubkey) -> String {
        let token_account_address =
            spl_associated_token_account::get_associated_token_address(&pubkey, &ore::MINT_ADDRESS);
        match client.get_token_account(&token_account_address).await {
            Ok(token_account) => {
                if let Some(token_account) = token_account {
                    token_account.token_amount.ui_amount_string
                } else {
                    "0.00".to_string()
                }
            }
            Err(_) => "0.00".to_string(),
        }
    }

    pub async fn initialize_ata(
        client: Arc<RpcClient>,
        signer: &Keypair,
        priority_fee: u64,
        send_interval: u64,
    ) -> Pubkey {
        // Build instructions.
        let token_account_pubkey = spl_associated_token_account::get_associated_token_address(
            &signer.pubkey(),
            &ore::MINT_ADDRESS,
        );

        // Check if ata already exists
        if let Ok(Some(_ata)) = client.get_token_account(&token_account_pubkey).await {
            return token_account_pubkey;
        }

        // Sign and send transaction.
        let ix = spl_associated_token_account::instruction::create_associated_token_account(
            &signer.pubkey(),
            &signer.pubkey(),
            &ore::MINT_ADDRESS,
            &spl_token::id(),
        );
        println!("Creating token account {}...", token_account_pubkey);
        match MinerV2::send_and_confirm(
            &signer,
            client.clone(),
            &[ix],
            true,
            send_interval,
            priority_fee,
        )
        .await
        {
            Ok(_sig) => println!("Created token account {:?}", token_account_pubkey),
            Err(e) => println!("Transaction failed: {:?}", e),
        }

        // Return token account address
        token_account_pubkey
    }

    pub fn validate_hash(
        hash: KeccakHash,
        current_hash: KeccakHash,
        pubkey: Pubkey,
        nonce: u64,
        difficulty: KeccakHash,
    ) -> bool {
        // Validate hash correctness
        let hash_ = hashv(&[
            current_hash.as_ref(),
            pubkey.as_ref(),
            nonce.to_le_bytes().as_slice(),
        ]);
        if sol_memcmp(hash.as_ref(), hash_.as_ref(), HASH_BYTES) != 0 {
            return false;
        }

        // Validate hash difficulty
        if hash.gt(&difficulty) {
            return false;
        }

        true
    }
}

fn lamports_to_sol(lamports: u64) -> f64 {
    (lamports as f64) / (LAMPORTS_PER_SOL as f64)
}
