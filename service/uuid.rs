use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::mpsc::{sync_channel, Receiver, RecvError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self};
//use std::time::{Duration, Instant};
//use std::error::Error;
use core::task::Poll;
use std::result::Result;
use std::{env, path::PathBuf};
//use std::sync::{LockResult,MutexGuard};
//use std::io::{Error};//,ErrorKind};

use mysql_async::params;
use mysql_async::prelude::*;
use mysql_async::{IsolationLevel, TxOpts};

//use aws_sdk_dynamodb::{Error};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration, Instant};
//use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio::task;
//use tokio::runtime::Runtime;
use tokio::runtime::Handle;

use uuid::Uuid;

pub struct UuidAdd(pub String, pub oneshot::Sender<Uuid>);
//struct UuidQuery<'a>(&'a str, oneshot::Sender<Uuid>);

pub fn start_uuid_service(
    pool: mysql_async::Pool,
    rt_handle: tokio::runtime::Handle,
    mut add_rx: tokio::sync::mpsc::Receiver<UuidAdd>,
    //    mut query_ch: tokio::sync::mpsc::Receiver<UuidQuery>,
    mut shutdown_ch: broadcast::Receiver<u8>,
) -> task::JoinHandle<i8> {
    let mut uuid_store: HashMap<String, Uuid> = HashMap::new();

    let uid_server = tokio::spawn(async move {
        println!("uuid service...started");
        loop {
            tokio::select! {
                biased; // removes random number generation - normal processing will determine order so select! can follow it.
                Some(request) = add_rx.recv() => {
                                    // create uuid value for id passed in
                                    let uid = uuid_store.entry(request.0).or_insert(Uuid::new_v4());
                                    // pass uid back to requestor via resp channel
                                    request.1.send(*uid);
                                    }

                _ = shutdown_ch.recv() => {

                                    let start = Instant::now();
                                    let mut bat_uuid_store: HashMap<usize,  Vec<(String, Uuid)> > = HashMap::new();
                                    let mut i : usize = 0;
                                    const DOP : usize = 6;

                                    // repackage uuid_store into a batched HashMap
                                    for (k,v) in uuid_store.drain() {
                                        if i < DOP {
                                            bat_uuid_store.entry(i%DOP).or_insert(vec![(k,v)]);   // v.as_urn().encode_lower(&mut Uuid::encode_buffer()).to_owned())]);
                                        } else {
                                            bat_uuid_store.entry(i%DOP).and_modify(|p|p.push((k,v))); //.as_urn().encode_lower(&mut Uuid::encode_buffer()).to_owned())));
                                        }
                                        i+=1;
                                    }
                                    let bat_len = bat_uuid_store.len();

                                    println!("uuid service: duration repackage: {:?} {}", Instant::now().duration_since(start),bat_len);

                                    let (tx1, mut rx1) = tokio::sync::mpsc::channel(5+1);

                                    let start = Instant::now();

                                    for (k,v) in bat_uuid_store.drain() {

                                        let tx_batch_ch = tx1.clone();
                                        let mut conn = pool.get_conn().await.unwrap();


                                        tokio::spawn(async move {

                                            let mut tx_opts = TxOpts::default();
                                            tx_opts.with_consistent_snapshot(true)
                                            .with_isolation_level(IsolationLevel::RepeatableRead);
                                            let mut tx = conn.start_transaction(tx_opts).await.unwrap();
                                            let start = Instant::now();

                                            let result = r#"INSERT INTO rdf_key_map(rdf_key, uuid)
                                                 VALUES (:rdf,:uuid)"#
                                                 .with(v.iter().map(|(key,val)| {
                                                     params!{ "rdf" => key.clone(),
                                                             "uuid" => val, //value.to_string(),
                                                              }
                                                    }))
                                                 .batch(&mut tx)
                                                 .await;

                                            if let Err(err) = result {
                                                     panic!("{}",err);
                                            }

                                            tx.commit().await;

                                            // send complete msg
                                            tx_batch_ch.send(b'x').await;

                                        });
                                    }

                                    // wait for batched inserts to finish
                                    for i in 0..bat_len {
                                        println!("uuid service: Waiting for batch insert to finish...{}",i);
                                        rx1.recv().await;

                                    }
                                    println!("uuid service...shutdown. Duration of mysql load {:?}", Instant::now().duration_since(start));
                                    break; //loop
                }
            }
        }

        0
    });

    uid_server
}
