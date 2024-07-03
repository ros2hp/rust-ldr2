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

pub struct ChildEdgeRec {
    pub puid: Uuid,
    pub sortk: String, // move
    pub cuid: Uuid,    // move
                       // status: String,
}

const COMPLETED: &str = "C";
const ERROR: &str = "E";

const PARENT_EDGE_BATCH_SIZE: usize = 1000;
const PARENT_EDGE_CONCURRENT_TASKS: usize = 8;
// start_child_edge_service not used as too slow. It was thought that a single task large batch write might be quicker
// then each parallel thread writing very small batch writes. It so happens that 22 threads is faster.
pub fn start_child_edge_service(
    pool: mysql_async::Pool,
    rt_handle: tokio::runtime::Handle,
    mut child_edge_ch: tokio::sync::mpsc::Receiver<Vec<ChildEdgeRec>>,
    mut shutdown_ch: broadcast::Receiver<u8>,
) -> task::JoinHandle<i8> {
    //let mut uuid_store: HashMap<String, Uuid> = HashMap::new();
    let mut batch: Vec<ChildEdgeRec> = vec![];

    let child_edge_server = tokio::spawn(async move {
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(PARENT_EDGE_CONCURRENT_TASKS);

        let load_id = "thread_edge";
        let mut cnt = 0;
        let mut async_tasks = 0;
        loop {
            tokio::select! {
                biased;
                Some(mut req_rec) = child_edge_ch.recv() => { // ...recv().await is added by select!

                                batch.append(&mut req_rec);

                                if batch.len() > PARENT_EDGE_BATCH_SIZE {

                                    let mut new_batch : Vec<ChildEdgeRec> = Vec::with_capacity(PARENT_EDGE_BATCH_SIZE);
                                    let exec_batch = std::mem::replace(&mut batch, new_batch);

                                    let tx_batch_ch=tx1.clone();
                                    let mut conn = pool.get_conn().await.unwrap();
                                    async_tasks+=1;
                                    cnt+=1;

                                    if  async_tasks == PARENT_EDGE_CONCURRENT_TASKS {
                                        rx1.recv().await;
                                        async_tasks-=1;
                                    }

                                    tokio::spawn(async move {
                                        let mut tx_opts = TxOpts::default();
                                        tx_opts.with_consistent_snapshot(true)
                                        .with_isolation_level(IsolationLevel::RepeatableRead);
                                        let mut tx = conn.start_transaction(tx_opts).await.unwrap();
                                        let start = Instant::now();

                                        //println!("INSERT INTO test_childedge {}",batch.len());
                                        let result = r#"INSERT INTO test_childedge (puid, sortk, cuid, status) VALUES (:puid,:sk,:cuid,:status)"#
                                        .with( exec_batch.iter().map(|p| {
                                                params! {
                                                    "puid"  => p.puid,
                                                    "sk"    => p.sortk.clone(),
                                                    "cuid"  => p.cuid,
                                                    "status" => "X".to_string(),
                                                }
                                            }))
                                         .batch(&mut tx) // .batch(&mut conn)
                                         .await;

                                         if let Err(err) = result {
                                             panic!("{}",err);
                                         }

                                         let mut dur_ = Instant::now().duration_since(start);
                                         let dur = format!("{:?}",dur_);

                                         let result = r#"INSERT INTO load_log(id,batch_id,duration,status)
                                         VALUES (:id,:batch_id,:duration,:status)"#
                                         .with(params!{ "id" => load_id,
                                                     "batch_id" => cnt,
                                                      "duration" => dur,
                                                      "status" => COMPLETED,
                                                        })
                                         .run(&mut tx)
                                         .await;

                                         if let Err(err) = result {
                                             panic!("{}",err);
                                         }

                                         tx.commit().await;

                                        // send complete msg
                                        tx_batch_ch.send(b'x').await;
                                    });
                              }
                            }

                _ = shutdown_ch.recv() => {
                                    if batch.len() > 0 {

                                            let mut conn = pool.get_conn().await.unwrap();
                                            let mut tx_opts = TxOpts::default();
                                            tx_opts.with_consistent_snapshot(true)
                                            .with_isolation_level(IsolationLevel::RepeatableRead);
                                            let mut tx = conn.start_transaction(tx_opts).await.unwrap();

                                            println!("last test_childedge {}",batch.len());
                                            r#"INSERT INTO test_childedge (puid, sortk, cuid, status) VALUES (:puid,:sk,:cuid,:status)"#
                                            .with( batch.iter().map(|p| {
                                                    params! {
                                                        "puid"  => p.puid,
                                                        "sk"    => p.sortk.clone(),
                                                        "cuid"  => p.cuid,
                                                        "status" => "X".to_string(),
                                                    }
                                                }))
                                             .batch(&mut tx) // .batch(&mut conn)
                                             .await;

                                             tx.commit().await;
                                    }

                                    println!("child_edge service...shutdown");
                                    break;
                }
            }
        }
        println!("child_edge_service...shutdown.");
        0
    });

    child_edge_server
}
