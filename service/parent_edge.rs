use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
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
use uuid::Uuid;

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

use crate::LOAD_THREADS;

const COMPLETED: &str = "C";
const ERROR: &str = "E";

const EDGE_MAX_CONCURRENT_TASK: usize = LOAD_THREADS; // equiv to number of concurrent edge tasks
const EDGE_TASK_BATCH_SIZE: usize = 250;
const DB_BATCH_SIZE: usize = 150;

pub struct EdgeCnt {
    pub uid: Uuid, // type [u8;16] ?
    pub edges: u32,
}

#[derive(Copy, Clone)]
pub struct Rec {
    bid: u32,
    uid: Uuid,
    cnt: u32,
}
impl Default for Rec {
    fn default() -> Rec {
        Rec {
            bid: 0,
            uid: Uuid::nil(),
            cnt: 0,
        }
    }
}

#[derive(Debug)]
struct TestErr;
impl TestErr {
    fn new() -> TestErr {
        TestErr {}
    }
}
impl fmt::Display for TestErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test error ??")
    }
}
impl std::error::Error for TestErr {}

pub fn start_parent_edge_service(
    rt_handle: tokio::runtime::Handle,
    mut edge_ch: tokio::sync::mpsc::Receiver<EdgeCnt>,
    mut shutdown_ch: broadcast::Receiver<u8>,
    mysql_pool: mysql_async::Pool,
) -> task::JoinHandle<i8> {
    let mut edge_store: HashMap<u32, Vec<Uuid>> = HashMap::new();
    let mut max_edges: u32 = 0;

    let edge_server = tokio::spawn(async move {
        loop {
            tokio::select! {
            biased;
            Some(request) = edge_ch.recv() => {
                                if request.edges > max_edges {
                                    max_edges = request.edges
                                }
                                let uid = edge_store.entry(request.edges)
                                .and_modify(|v| v.push(request.uid))
                                .or_insert(vec![request.uid]);
                                }

            _ = shutdown_ch.recv() => {
                                let start = Instant::now();

                                println!("Saving edge count data to mysql...");
                                // let mut cnt = vec![];
                                // let mut edges = 0;
                                // for (k,v) in &edge_store {
                                //     cnt.push(*k);
                                //     edges+=v.len();
                                // }
                                // println!("edge: Total edges in table: {}",edges);
                                // // sort descending...
                                // cnt.sort_by(|a,v| if a > v { std::cmp::Ordering::Less} else { std::cmp::Ordering::Greater});

                                let mut db_batch_id : u32 = 1;
                                let load_id = "p_edge";

                                let task_pool_full = false;

                                let mut rec_cnt : usize = 0;
                                let mut bid_cnt : usize = 0;
                                //let mut batch : [Rec; EDGE_TASK_BATCH_SIZE] =  std::array::from_fn(|_| Rec::default());
                                let mut batch : Vec<Rec> = Vec::with_capacity(EDGE_TASK_BATCH_SIZE);
                                let mut spawned_tasks = 0;
                                let mut async_tasks = 0;

                                let (tx1, mut rx1) = tokio::sync::mpsc::channel(EDGE_MAX_CONCURRENT_TASK+1);

                                // no allocations - all ownership moves or copy for those that support Copy
                                for (k,v) in edge_store.drain() {

                                    for uid in v {

                                         batch.push(Rec{bid: db_batch_id, uid : uid, cnt : k});

                                         rec_cnt+=1;
                                         bid_cnt+=1;
                                         if bid_cnt == DB_BATCH_SIZE {
                                            db_batch_id+=1;
                                            bid_cnt=0;
                                         }

                                        if rec_cnt == EDGE_TASK_BATCH_SIZE  {

                                            // mutli buffer batch - swap batch out and move it into task and keep original batch variable alive.
                                            //let mut new_batch : [Rec; EDGE_TASK_BATCH_SIZE] =  std::array::from_fn(|_| Rec::default());
                                            let mut new_batch : Vec<Rec> = Vec::with_capacity(EDGE_TASK_BATCH_SIZE);
                                            let exec_batch = std::mem::replace(&mut batch, new_batch);

                                            let tx_batch_ch=tx1.clone();
                                            spawned_tasks+=1;
                                            rec_cnt=0;
                                            async_tasks+=1;

                                            // each batch is run in its own tokio::task
                                            let mut conn  = mysql_pool.get_conn().await.unwrap();
                                            let test_err = TestErr::new();

                                            tokio::spawn(async move {

                                                let mut load_errored = false;
                                                let start = Instant::now();
                                                let mut err_msg : String = String::new();
                                                let mut sql_status = COMPLETED;

                                                let mut tx_opts = TxOpts::default();
                                                tx_opts.with_consistent_snapshot(true)
                                                .with_isolation_level(IsolationLevel::RepeatableRead);
                                                let mut tx = conn.start_transaction(tx_opts).await.unwrap();

                                                // let result = tx.exec_batch(r#"INSERT INTO Edge_test (Bid, Uid, cnt) VALUES (:bid,:uid,:cnt)"#,
                                                // exec_batch.iter().map(|p| {
                                                //             params! {
                                                //                 "bid" => p.bid,
                                                //                 "uid" => p.uid,
                                                //                 "cnt" => p.cnt,
                                                //             }
                                                // })).await;

                                                let mut result = r#"INSERT INTO Edge_test (Bid, Uid, cnt) VALUES (:bid,:uid,:cnt)"#
                                                .with( exec_batch.iter().map(|p| {
                                                            params! {
                                                                "bid" => p.bid,
                                                                "uid" => p.uid,
                                                                "cnt" => p.cnt,
                                                            }
                                                        }))
                                                .batch(&mut tx) // change from conn both support ToConnection Trait
                                                .await;

                                                // test code for throwing an error.
                                                // if spawned_tasks == 4 {
                                                //     result=Err(mysql_async::Error::Other(Box::new(test_err)));
                                                // }

                                                if let Err(err) = result {

                                                    tx.rollback().await;

                                                    sql_status = ERROR;
                                                    err_msg = format!("Error in edge load. {}",err);
                                                    let mut tx_opts = TxOpts::default();
                                                    tx_opts.with_consistent_snapshot(true)
                                                    .with_isolation_level(IsolationLevel::RepeatableRead);
                                                    tx = conn.start_transaction(tx_opts).await.unwrap();
                                                }
                                                let mut dur_ = Instant::now().duration_since(start);
                                                let dur = format!("{:?}",dur_);

                                                // send complete msg
                                                let result = r#"INSERT INTO load_log(id,batch_id,duration,status,error_msg)
                                                        VALUES (:id,:batch_id,:duration,:status,:err_msg)"#
                                                        .with(params!{ "id" => load_id,
                                                                      "batch_id" => spawned_tasks,
                                                                      "duration" => dur,
                                                                      "status" => sql_status,
                                                                      "err_msg" => err_msg,
                                                                      })
                                                        .run(&mut tx)
                                                        .await;

                                                let r = tx.commit().await;
                                                if let Err(err) = r {
                                                    panic!("err in commit");
                                                }

                                                // send complete msg
                                                tx_batch_ch.send(b'x').await;

                                            }); // drop conn

                                            if async_tasks == EDGE_MAX_CONCURRENT_TASK {
                                                // wait for a task to complete
                                                rx1.recv().await;
                                                async_tasks-=1;
                                                //spawned_tasks=EDGE_MAX_CONCURRENT_TASK;
                                            }
                                        }
                                    }
                                }

                                if rec_cnt > 0 {
                                    let mut conn  = mysql_pool.get_conn().await.unwrap();
                                    let tx_batch_ch=tx1.clone();
                                    let mut tx_opts = TxOpts::default();
                                    tx_opts.with_consistent_snapshot(true)
                                    .with_isolation_level(IsolationLevel::RepeatableRead);
                                    let mut tx = conn.start_transaction(tx_opts).await.unwrap();
                                    async_tasks+=1;

                                        r#"INSERT INTO Edge_test (Bid, Uid, cnt) VALUES (:bid,:uid,:cnt)"#
                                        .with( batch.iter().take(rec_cnt).map(|p| {
                                                            params! {
                                                                "bid" => p.bid,
                                                                "uid" => p.uid,
                                                                "cnt" => p.cnt,
                                                            }
                                                        }))
                                        .batch(&mut tx)
                                        .await;
                                        tx.commit().await;

                                        // send complete message
                                        tx_batch_ch.send(b'x').await;
                                }
                                // wait for async tasks to finish...
                                for i in 0..async_tasks {
                                    println!("edge: end waiting for async task to finish...");
                                    rx1.recv().await;
                                    println!("edge: end wait over...");
                                }
                                let end = Instant::now();
                                println!("edge: duration {:?}", end.duration_since(start));
                                println!("edge service...shutdown");
                                break;
                            }

            }
        }
        println!("parent_edge_service...shutdown.");
        0
    });

    edge_server
}
