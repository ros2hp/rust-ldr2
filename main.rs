#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused)]
// #![feature(repr128)]
// #![feature(hash_set_entry)]
//extern crate aws_sdk_dynamodb;

mod service;
mod types; // types module is private to main, however only pub items in types are accessible to main.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::mpsc::{sync_channel, Receiver, RecvError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self};
//use std::time::{Duration, Instant}; // tokio instead 
//use std::error::Error;
use std::result::Result;
use std::{env, path::PathBuf};

//use std::sync::{LockResult,MutexGuard};
//use std::io::{Error};//,ErrorKind};
//
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::WriteRequest;
use aws_sdk_dynamodb::Client as DynamoClient;
//
// use crate::aws_smithy_runtime_api::client::result::SdkError;
// use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
//
use clap::Parser;
//use aws_sdk_dynamodb::{Error};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration, Instant};
//use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio::task;
//use tokio::runtime::Runtime;
use tokio::runtime::Handle;

use uuid::Uuid;

use mysql_async::params;
use mysql_async::prelude::*;
use mysql_async::{IsolationLevel, TxOpts};
// use sqlx::mysql::MySqlPool;
// use sqlx::pool::PoolOptions;

//extern crate lazy_static;
//extern crate aws_types;

//use lazy_static::lazy_static;

const BATCH_SIZE: usize = 25;      // nodes in batch
const LOAD_THREADS: usize = 12;

fn get_default_log_path() -> PathBuf {
    let key = "DATA_DIR";
    match env::var(key) {
        Ok(val) => {
            let mut path = PathBuf::from(val);
            path.push("test.rdf");
            path
        }
        Err(e) => {
            println!("couldn't find environment var {key}: {e}");
            std::process::exit(2)
        }
    }
}

#[derive(Debug, Parser)]
struct Opt {
    /// Rdf file name
    #[arg(short, long)]
    graph: String,
    #[arg(short,long,default_value=get_default_log_path().into_os_string())]
    file: PathBuf,
}

//fn main() -> Result<(), Box<dyn std::error::Error>> {

#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
    let region = "us-east-1";
    let table_name = "RustGraph.dev.2";

    let Opt { graph, file } = Opt::parse();

    let start = Instant::now();
    // let dbenv = env::var("DATABASE_URL");
    // if let Err(err) = dbenv {
    //     return Err(From::from(err));
    // }
    //let rt = Runtime::new().unwrap();
    let rt_handle = Handle::current();

    // ================================
    // Create a a mysql connection pool
    // ================================   
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let host = "mysql8.???.us-east-1.rds.amazonaws.com";
    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(host)
            .user(Some("???"))
            .pass(Some("?????"))
            .db_name(Some("???"))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    let mut conn = pool.get_conn().await.unwrap();
   
    r#"truncate table test_childedge"#.ignore(&mut conn).await?;   
    r#"truncate table Edge_test"#.ignore(&mut conn).await?; 
    r#"truncate table load_log"#.ignore(&mut conn).await?; 
    r#"truncate table rdf_key_map"#.ignore(&mut conn).await?; 
    
    let _start_1 = Instant::now(); 
    // ========================
    // Create a Dynamodb Client
    // ========================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    let graph = "Movies".to_string();
    // ===============================
    // Fetch Graph Types from Dynamodb
    // ===============================
    let (node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?; 
    println!("Node Types:");
    // let nodetypes = type_caches.node_types.clone();
    //for t in ty_r.0.iter() {
    for t in node_types.0.iter() {
        println!("Node type {} [{}]    reference {}",t.get_long(),t.get_short(),t.is_reference());
        for attr in t {
            println!("attr.name [{}] dt [{}]  c [{}]",attr.name,attr.dt, attr.c);
        }
    }
    
    // create broadcast channel to shutdown services
    let (shutdown_broadcast_ch, mut shutdown_recv_ch) = broadcast::channel(1); // broadcast::channel::<u8>(1);
    
    // start UUID service
    let (uuid_ch, mut add_rx) = tokio::sync::mpsc::channel(250);
    let (child_edge_ch, mut child_edge_rx) = tokio::sync::mpsc::channel(250);
    let (edge_ch, mut edge_rx) = tokio::sync::mpsc::channel(250);
    // let (query_tx, mut query_rx) = tokio::sync::mpsc::channel(16);
    
    // start Retry service (handles failed putitems) 
    let (retry_ch, mut retry_rx) = tokio::sync::mpsc::channel(LOAD_THREADS * 2);
    let mut retry_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let retry_service = service::retry::start_service(
        dynamo_client.clone(),
        retry_rx,
        retry_ch.clone(),
        retry_shutdown_ch,
        table_name,
    );
    // start uuid service - repo of node uuid values
    let mut uuid_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let uuid_service = service::uuid::start_uuid_service(
        mysql_pool.clone(),
        rt_handle.clone(),
        add_rx,
        uuid_shutdown_ch,
    );

    // start child_edge service
    let mut child_edge_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let child_edge_service = service::child_edge::start_child_edge_service(
        mysql_pool.clone(),
        rt_handle.clone(),
        child_edge_rx,
        child_edge_shutdown_ch,
    );
    
    // start parent_edge service
    let mut parent_edge_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let parent_edge_service = service::parent_edge::start_parent_edge_service(
        rt_handle.clone(),
        edge_rx,
        parent_edge_shutdown_ch,
        mysql_pool.clone(),
    );

    println!("\nfile [{:?}]", file);

    let f = File::open(file)?; // .expect("Should open Open file");

    // mpsc - multi-producer-single-consumer need to be changed to spmc, single-producer-multi-consumer
    // using multiple consumers (channel readers) reading from a single channel sync'd via a mutex.
    let (sender, receiver) = sync_channel::<NODES>((LOAD_THREADS) * 2);
    let arc_load_recvr = Arc::new(std::sync::Mutex::new(receiver));

    // start database load tasks (implemented as OS threads) - reads rdf data from channel
    println!("Start background services ... each reading from channel");
    let hdls = start_db_load_tasks(
        arc_load_recvr,
        graph_short_name,
        type_caches.ty_c.clone(),
        type_caches.attr_ty_s.clone(),
        type_caches.ty_short_nm.clone(),
        type_caches.ty_long_nm.clone(),
        uuid_ch,
        child_edge_ch,
        edge_ch,
        retry_ch,
        &rt_handle,
        mysql_pool.clone(),
        dynamo_client,
    );

    // syncrhonous rdf reader - passes rdf data on channel to be processed by database loader threads. 
    // Implements a spmc channel communication communication pattern, as opposed to the standard mpsc pattern. 
    process_rdf_file(f, sender);

    // wait for background services to finish
    println!("Waiting for threads to finish...");
    for h in hdls {
        println!(" waiting for db_load threads to finish...");
        let _ = h.join(); //h.await; //join();
    }

    println!("threads finished");
    sleep(Duration::from_millis(2000)).await;
    println!("*** Duration before services shutdown: {:?} secs", Instant::now().duration_since(start).as_secs());
    println!("Waiting for support services to finish...");
    shutdown_broadcast_ch.send(0);
    uuid_service.await;
    retry_service.await;
    parent_edge_service.await;
    child_edge_service.await;
    
    println!("*** Duration after services shutdown: {:?} secs", Instant::now().duration_since(start).as_secs());
    println!("All support services shutdown.");
    // shutdown mysql pool
    mysql_pool.disconnect();
    
    println!("Exit program");

    Ok(()) 
}

#[derive(Debug)]
struct Rdf {
    n: i64, // line number in Rdf file
    //    s: String, // shortName  (blank-node-name) "_a" representing a UUID - conversion takes place just before loading into db
    p: String, // two types of entries: 1) __type 2) Name of attribute in the type.
    o: String, // typeName  or data (scalar, flag/list, shortName for UUID )
}

//#[derive(Debug)]
struct Node {
    id: String,        // supposed to be line # in rdf file
    pkey: String,      // subject value - repeated for each node rdf
    node_type: String, // object where predict = "__type" e.g. "Person", "Film"
    lines: Vec<Rdf>, // s(pkey)-p(attribute name for a node_type)-o(value)for scalar rdf or child node uuid for the particular attribute e.g. director.film (uuids of films)
                     //err    : Vec<Eerror> // used by verification process to record any errors
}


impl Default for Node {
    fn default() -> Node {
        Node {
            id: String::new(), // with_capacity(20),
            pkey: String::new(),
            node_type: String::new(),
            lines: Vec::new(),
        }
    }
}

type NODES = [Node;BATCH_SIZE];

fn start_db_load_tasks(
    alr: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<NODES>>>,
    graph_short_name: String,
    tycache: Arc<HashMap<String, types::TyAttrBlock>>,
    attr_ty_s_: Arc<HashMap<types::AttrTyS, String>>,
    ty_short_nm_: Arc<HashMap<String, String>>,
    ty_long_nm_: Arc<HashMap<String, String>>,
    uuid_ch: tokio::sync::mpsc::Sender<service::uuid::UuidAdd>,
    child_edge_ch: tokio::sync::mpsc::Sender<Vec<service::child_edge::ChildEdgeRec>>,
    edge_ch: tokio::sync::mpsc::Sender<service::parent_edge::EdgeCnt>,
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    rt_handle: &tokio::runtime::Handle,
    mysql_pool: mysql_async::Pool,
    dynamo_client: DynamoClient,
) -> Vec<std::thread::JoinHandle<()>> {
    //tokio::task::JoinHandle<()>> { //::std::thread::JoinHandle<()>> {
    let mut jhandles: Vec<std::thread::JoinHandle<_>> = Vec::new();
    //let mut jhandles: Vec<tokio::task::JoinHandle<_>> = Vec::new();

    for i in 0..LOAD_THREADS {
        let ii: usize = i;
        let alr_c = alr.clone();
        let tycache_c = tycache.clone();
        let uuid_ch_ = uuid_ch.clone();
        let child_edge_ch_ = child_edge_ch.clone();
        let edge_ch_ = edge_ch.clone();
        let retry_ch_ = retry_ch.clone();
        let handle = rt_handle.clone();
        //let dbconn = mysql_pool.get_conn()?; // error: cannot use the `?` operator in a function that returns `Vec<std::thread::JoinHandle<()>>`
        let pool = mysql_pool.clone();
        let client = dynamo_client.clone();
        let graph_sname = graph_short_name.clone();
        let attr_ty_s = attr_ty_s_.clone();
        let ty_short_nm = ty_short_nm_.clone();
        let ty_long_nm = ty_long_nm_.clone();

        println!(" start process_node_batch......{}", i);
        let handle = std::thread::spawn(move || {
            //td::thread::spawn( move || {    // task::spawn(move {
            process_node_batch(
                ii,
                alr_c,
                graph_sname,
                tycache_c,
                attr_ty_s,
                ty_short_nm,
                ty_long_nm,
                uuid_ch_,
                child_edge_ch_,
                edge_ch_,
                retry_ch_,
                handle,
                pool,
                &client,
            );
            //.await;
        });

        jhandles.push(handle);
    }

    jhandles
}

fn process_rdf_file(
    f: File,
    chan: std::sync::mpsc::SyncSender<NODES>, //chan: SyncSender<Box<[Node]>>, //
) -> Result<(), Box<dyn std::error::Error>> {
    let mut node_batch: NODES = std::array::from_fn(|_| Node::default());
    // let box_array: Box<[Node]> = Box::new(array);
    // let mut node_batch: [Box<[Node]>; 1] = [box_array];
    //let  mut node_batch: Box<[Node]> = Box::new(array);
    // let mut node_batch: [Box<[Node]>; 1] = [box_array];

    let mut nodes: usize = 0; //  batch index
    let mut prev_s = "__".to_string();
    let mut s: &str;
    let mut p: &str;
    let mut o: &str;
    let mut cnt: u8 = 0;
    //let mut cn: &mut Node = &mut node_batch[0][0];
    let mut cn: &mut Node = &mut node_batch[0];

    let br = BufReader::new(f); // creates a BufRead (trait) type

    //let eod = Error::new(ErrorKind::Other, "oh no!")

    for line_result in br.lines() {
        // br.lines() returns an Option<Result<String>> type that is impl interator trait
        let line = line_result?;

        if line.len() == 0 {
            continue; // blank line
        }

        line.trim_start();

        //println!("process_rdf_file: line [{line}]");
        // let vs = line.split_ascii_whitespace().take(3).collect::<Vec<&str>>();  // split line imto s p o
        // let mut vs = line.split_ascii_whitespace().take(3);
        // s = &vs.next().expect("expected subject when reading rdf line")[2..]; // String -> &str -> [u8] -> String, (no allocation after bf.lines)
        // p = vs.next().expect("expected predicate when reading rdf line");
        // o = vs
        //     .next()
        //     .expect("expected object value when reading rdf line");

        let term: usize = 0;
        let term2: usize = 0;

        s = "uninitialised";
        o = s;
        p = s;
        line.trim_start();

        if let Some(mut i) = line.find(' ') {
            s = &line[2..i]; // skip start string "_:"
            i += 1;
            loop {
                if let Some(mut ii) = line[i..].find(' ') {
                    if ii == 0 {
                        i += 1;
                        continue;
                    }
                    p = &line[i..i + ii];
                    i += ii;

                    //if let Some(term) = line[i..].find(|c: char| c.is_whitespace() || c == '_' )
                    if let Some(term) = line[i..].find('"') {
                        i += term + 1; // move past "
                        if let Some(term2) = line[i..].find('"') {
                            o = &line[i..term2 + i];
                        }
                    } else if let Some(term) = line[i..].find("_:") {
                        let ii = i;
                        i += term + 2; // move past _:
                        if let Some(term2) = line[i..].find(' ') {
                            o = &line[i..term2 + i];
                        }
                    } else if let Some(n) = line[i + 1..].find(' ') {
                        o = &line[i + 1..i + n - 1];
                    } else {
                        o = &line[i..];
                    }
                }
                break;
            }
        }
        //println!("spo : [{s}]. [{p}]  [{o}]");

        if prev_s == "__" {
            prev_s = s.to_string();
        }

        //println!("s, prev_s nodes {} {} {}",s, prev_s, nodes );
        if s != prev_s {
            nodes += 1;
            if nodes ==BATCH_SIZE {
                // process batches asynchronously
                let array: NODES = std::array::from_fn(|_| Node::default());
                // mv ownership of node array to nbatch
                let nbatch = std::mem::replace(&mut node_batch, array);

                // channel takes ownership of nbatch. Blocks when channel capacity is reached.
                // send on channel consumed by batch workers
                if let Err(x) = chan.send(nbatch) {
                    // TODO: try await
                    panic!("error: sending nbatch on chan channel - {}", x);
                }
                nodes = 0;
            }
            let cn_ = &mut node_batch;
            cn = &mut cn_[nodes];
            prev_s = s.to_owned();
        }

        match p {
            "__type" | "__TYPE" => {
                cn.pkey = s.to_owned();
                //cn.node_type.push_str(&o[1..o.len() - 1]); // remove "  = Person
                cn.node_type.push_str(&o);
                cn.id.push_str(s); // trim  _:
                                   // println!(">> xxd  line: type = [{:?}]   {:?}", cn.node_type, cn.id);
            }

            _ => {
                if o.is_char_boundary(1) {
                    o = match &o[0..1] {
                        // "\"" => (&o[1..]).trim_end_matches('\''), // trim ' from both ends  &o[2..].rtrim_end_matches("\""),
                        "_" => &o[2..],
                        _ => o,
                    };
                }

                cn.lines.push(Rdf {
                    n: 0,
                    // s: s.to_owned(),
                    p: p.to_owned(),
                    o: o.to_owned(),
                }); // ownership of stings passed to Lines struct
            }
        }
    }
    if nodes !=BATCH_SIZE {
        let array: NODES = std::array::from_fn(|_| Node::default());
        // mv ownership of node array to nbatch
        let nbatch = std::mem::replace(&mut node_batch, array);

        //println!("send batch {} batch size {}", nodes, nbatch.len());
        // channel takes ownership of nbatch. blocks when channel capacity reached.
        if let Err(x) = chan.send(nbatch) {
            println!(" is erro on send nbatch - {}", x);
        }
    }
    Ok(())
}

//struct Flatten_Value<T>(T) ;

// struct Rdf {
//     n: i64,    // line number in Rdf file
//     s: String, // shortName  (blank-node-name) "_a" representing a UUID - conversion takes place just before loading into db
//     p: String, // two types of entries: 1) __type 2) Name of attribute in the type.
//     o: String, // typeName  or data (scalar, flag/list, shortName for UUID )
// }
// struct Node {
//     id: String,         // supposed to be line # in rdf file
//     pkey: String,       // subject value - repeated for each node rdf
//     node_type: String,    // object where predict = "__type" e.g. "Person", "Film"
//     lines: Vec<Rdf>,    // s(pkey)-p(attribute name for a node_type)-o(value)for scalar rdf or child node uuid for the particular attribute e.g. director.film (uuids of films)
//     //err    : Vec<Eerror> // used by verification process to record any errors
// }
// impl Default for Node {
//     fn default() -> Node {
//         Node {
//             id: String::new(), // with_capacity(20),
//             pkey: String::new(),
//             node_type: String::new(),
//             lines: Vec::new(),
//         }
//     }
// }
struct FlattenRdf<'a> {
    //    pkey: &'a str,      // subject
    ty: Option<&'a types::TyAttrD>,   // attribute type. Why is the Option?
    sortk: String,
    value_str: Option<&'a str>,
    value_vstr: Option<Vec<&'a str>>, // used for Dynamo List type and graph edges (uidpreds)
                                      //

                                      //attributes : &'a types::block::TyAttrD<'a>     // sourced from graph type atrtibutes based on node type from rdf data
}

impl<'a> FlattenRdf<'a> {
    fn new(ty: Option<&'a types::TyAttrD>) -> FlattenRdf<'a> {
        //, attrs : &types::block::TyAttrD<'a>) -> FlattenRdf<'a> {
        FlattenRdf {
            ty: ty,
            sortk: gen_sortk(ty),
            value_str: None,
            value_vstr: None, // used for Dynamo List types and graph edges
        }
    }
}

//type Vec<WriteRequest> = Vec<WriteRequest>;

// type NODES = [Node;BATCH_SIZE];
const MIN_EDGE_BATCH_SIZE: u32 = 5;
const DYNAMO_BATCH_SIZE: usize = 25;
// ggty: gograph types
fn process_node_batch<'a>(
    i: usize,
    alr: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<NODES>>>,
    graph_short_name: String,
    graph_types: Arc<HashMap<String, types::TyAttrBlock>>,
    attr_ty_s_: Arc<HashMap<types::AttrTyS, String>>,
    ty_short_nm_: Arc<HashMap<String, String>>,
    ty_long_nm_: Arc<HashMap<String, String>>,
    uuid_ch: tokio::sync::mpsc::Sender<service::uuid::UuidAdd>,
    child_edge_ch: tokio::sync::mpsc::Sender<Vec<service::child_edge::ChildEdgeRec>>,
    edge_ch: tokio::sync::mpsc::Sender<service::parent_edge::EdgeCnt>,
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    rt_handle: tokio::runtime::Handle,
    pool: mysql_async::Pool, //mut mysql_conn : mysql::PooledConn,
    dynamo_client: &DynamoClient,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut bat_w_req: Vec<WriteRequest> = vec![];

    println!("> process_node_batch {} running", i + 1);
    let dur = Duration::from_millis(50);

    // its a service - run forever until channel closed
    loop {
        //let batch: Result<Box<[Node]>, RecvError>;
        let batch: Result<NODES, std::sync::mpsc::RecvError>; // Option<NODES>; // Result<NODES,tokio::sync::mpsc::RecvError>;
        {
            let mut guard = alr.lock(); //  wait on channel for some data...
            batch = guard.unwrap().recv();
        }

        if batch.is_err() {
            // if batch.is_none() {
            println!(
                "> process_node_batch {} error on read of channel - break",
                i + 1
            );

            rt_handle.block_on(async move {
                println!(
                    "bat_w_req - channel closed: save dynamo batch {}",
                    bat_w_req.len()
                );
                if bat_w_req.len() > 0 {
                    persist_batch(dynamo_client, bat_w_req, &retry_ch).await;
                }
            });
            break;
        }
        let mut err_cnt: u8 = 0;
        let mut found = false;

        // for each node in batch
        for node in batch.unwrap().into_iter() {
            if node.pkey.len() == 0 {
                continue;
            }
            //println!("> process_node_batch process node {}", node.pkey.to_string());
            // get node attributes (n_attrs)
            let node_attrs = graph_types.get(&node.node_type);
            if node_attrs.is_none() {
                println!("error: process_node_batch: graph_types.get() for node.id [{}]. node pkey [{}], node_type [{}]",node.id, node.pkey, node.node_type);
                panic!("no type data for node type {} found", &node.node_type)
            }
            let attrs = node_attrs.unwrap().0.as_slice();

            //                        attr_name
            let mut flat_rdf_map: HashMap<&str, FlattenRdf> = HashMap::new();

            for line in &node.lines {
                found = false;

                for attr in attrs {
                    // match attribute name against rdf predicate
                    if attr.name != line.p {  // spo
                        continue;
                    }
                    found = true;
                    // println!(
                    //     "dt line:attr.name [{}] attr.dt [{}] line {:?}",
                    //     attr.name, attr.dt, line
                    // );

                    match attr.dt.as_str() {
                        // "Bl", .... TODO
                        "S" => {
                            //println!("S: {}", line.o);
                            // parse rdf object as i64
                            let mut frdf = FlattenRdf::new(Some(attr)); //, attr);
                            frdf.value_str = Some(line.o.as_str());
                            flat_rdf_map.insert(&attr.name, frdf);
                        }

                        "I" => {
                            // parse rdf object as i64
                            let v = line.o.parse::<i64>();
                            if v.is_ok() {
                                let mut frdf = FlattenRdf::new(Some(attr));
                                frdf.value_str = Some(line.o.as_str());
                                flat_rdf_map.insert(&attr.name, frdf);
                            } else {
                                println!("error: cannot convert '{}' to i64", line.o);
                                err_cnt += 1;
                                if err_cnt > 5 {
                                    std::process::exit(-1);
                                }
                            }
                        }

                        "F" => {
                            // parse line object as f64
                            let v = line.o.parse::<f64>();
                            if v.is_ok() {
                                let mut frdf = FlattenRdf::new(Some(attr));
                                frdf.value_str = Some(line.o.as_str());
                                flat_rdf_map.insert(&attr.name, frdf);
                            } else {
                                println!("error: cannot convert '{}' to i64", line.o);
                                err_cnt += 1;
                                if err_cnt > 5 {
                                    std::process::exit(-1);
                                }
                            }
                        }

                        "LI" => {
                            let vi64 = line.o.parse::<i64>();
                            if vi64.is_ok() {
                                let vi64_ = vi64.clone();
                                flat_rdf_map
                                    .entry(&attr.name[..])
                                    .and_modify(|v| {
                                        v.value_vstr.as_mut().unwrap().push(line.o.as_str())
                                    })
                                    .or_insert_with(|| {
                                        let mut frdf = FlattenRdf::new(Some(attr));
                                        frdf.value_vstr = Some(vec![line.o.as_str()]);
                                        frdf
                                    });
                            } else {
                                println!("error: cannot convert '{}' to i64", line.o);
                                err_cnt += 1;
                                if err_cnt > 5 {
                                    std::process::exit(-1);
                                }
                            }
                        }

                        "LF" => {
                            let vf64 = line.o.parse::<f64>();

                            if vf64.is_ok() {
                                let vf64_ = vf64.clone();
                                flat_rdf_map
                                    .entry(&attr.name[..])
                                    .and_modify(|v| {
                                        v.value_vstr.as_mut().unwrap().push(line.o.as_str())
                                    })
                                    .or_insert_with(|| {
                                        let mut frdf = FlattenRdf::new(Some(attr));
                                        frdf.value_vstr = Some(vec![line.o.as_str()]);
                                        frdf
                                    });
                            } else {
                                println!("error: cannot convert '{}' to f64", line.o);
                                err_cnt += 1;
                                if err_cnt > 5 {
                                    std::process::exit(-1);
                                }
                            }
                        }

                        //
                        "Nd" | "SS" | "LS" => {
                            // _:d Friends _:abc .
                            // _:d Friends _:b .
                            // _:d Friends _:c .
                            // need to convert n.Obj to [UID]
                            flat_rdf_map
                                .entry(&attr.name[..])
                                .and_modify(|v| v.value_vstr.as_mut().unwrap().push(&line.o))
                                .or_insert_with(|| {
                                    let mut frdf = FlattenRdf::new(Some(attr));
                                    frdf.value_vstr = Some(vec![line.o.as_str()]);
                                    frdf
                                });
                            //println!("len: {}",flat_rdf_map[&attr.name[..]].value_vstr.as_ref().unwrap().len());
                        }

                        _ => {
                            panic!(" match not found ")
                        }
                    }
                    if !found {
                        if !attr.n && attr.dt != "Nd" {
                            panic!(
                                "Not null type attribute %q must be specified in node {}",
                                attr.name
                            );
                            //node.Err = append(node.Err, err)
                        }
                    }
                }
            }

            //println!("flat_rdf_map.len = {}",flat_rdf_map.len());
            // add T# entry
            // let mut frdf = FlattenRdf::new(None); //, attr);
            // frdf.value_str = Some(&node.node_type);
            // flat_rdf_map.insert("__type__", frdf);

            let mut child_edge: Vec<service::child_edge::ChildEdgeRec> = vec![];
            let attr_ty_s = attr_ty_s_.clone();          // NOT USED
            let ty_short_nm = ty_short_nm_.clone();

            bat_w_req = rt_handle.block_on(async {   // no move. bat_w_req moved automatically as it is moved in one of the dynamo API's
                //let  mut conn  = unsafe {pool.get_conn().await.unwrap_unchecked()};
                let conn_ = pool.get_conn().await;
                if let Err(err) = conn_ {
                    panic!("Fatal error on get_conn() : {}", err);
                }
                let mut conn = conn_.unwrap();

                let mut edges: usize = 0;
                let (send_ch, resp_ch) = oneshot::channel();
                let msg = service::uuid::UuidAdd(node.pkey.clone(), send_ch);
                uuid_ch.send(msg).await; // await because its a buffered channel - wait when buffer full

                 let puid = match resp_ch.await {
                    Ok(puid) => puid, // keep as Uuid, binary [u8,16]
                    Err(e) => panic!("error - puid send channel closed {} ", e),
                };
                
                // aggregate edge data so it can be applied to database in the following step
                for (_, v) in &flat_rdf_map {
                    match v.ty {
                        None => continue,
                        Some(attr) if attr.dt != "Nd" => continue,
                        Some(x) => {},
                    }

                    for s in (*v).value_vstr.as_ref().unwrap() {
                        let (send_ch, resp_ch) = oneshot::channel();
                        let msg = service::uuid::UuidAdd(s.to_string(), send_ch); // to_string will keep deref'ing until &str is the target of the method call
                        uuid_ch.send(msg).await;
                        let cuid = match resp_ch.await {
                            Ok(cuid) => cuid, // keep as Uuid, binary [u8,16] //.to_string(),
                            Err(e) => panic!("error in uuid_ch send - child send channel closed "),
                        };
                        //println!("nodeid  puid cid  cuid = {}  {} | {}  {}", node.pkey, puid, s, cuid);
                        let mut sk: String = graph_short_name.clone(); // TODO: replace with short graph name
                        sk.push('|');
                        sk.push_str(&v.sortk);
                        
                        //println!("xx insert ty = {:?}",ty_short_nm.get(&node.node_type[..]));

                        child_edge.push(service::child_edge::ChildEdgeRec {
                            puid: puid, // puid is [u8;16] so Copy applies
                            sortk: sk,  // xown -> move sk to sortk
                            cuid: cuid, // Uuid is Copy
                        });
                    } //
                }
                // preserve edge data to a SQL database to be consumed during a downstream process
                
                let edges : u32 = child_edge.len().try_into().unwrap();
                 
              if edges > MIN_EDGE_BATCH_SIZE    {
               
                    // apply immediately to db as part of this thread.
                    let mut tx_opts = TxOpts::default();
                    tx_opts.with_consistent_snapshot(true)
                    .with_isolation_level(IsolationLevel::RepeatableRead);
                    let mut tx = conn.start_transaction(tx_opts).await.unwrap();
                    
                    let result = r#"INSERT INTO test_childedge (puid, sortk, cuid, status) VALUES (:puid,:sk,:cuid,:status)"#
                    .with( child_edge.iter().map(|p| {
                            params! {
                                "puid"  => puid,     // Uuid [u8,16] is Copy
                                "sk"    => p.sortk.clone(),
                                "cuid"  => p.cuid,  // Uuid [u8,16] is Copy 
                                "status" => "X".to_string(),
                            }
                        }))
                     .batch(&mut tx) // .batch(&mut conn)
                     .await;
                    
                     if let Err(err) = result {
                          panic!("{}",err);
                     }
                     
                     tx.commit().await;
  
                } else if edges > 0 {
                     
                     // defer to a batch insert task.
                     child_edge_ch.send(child_edge).await;
                }
                
                if edges > 0 {
                    // defer parent node insert to a batch task
                    edge_ch.send(service::parent_edge::EdgeCnt {uid: puid,edges: edges }).await;
                }
                ///vh0wJh8aTJmKfWh2f6+uHg== GoGraph.dev.7
                //
                // dynamdb - put node metadata 
                //
                let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                let puid_ = puid.clone();
                let puid_b = Blob::new(puid_.as_bytes());
                let puid_b8 = Blob::new(&puid_.as_bytes()[..8]);
                
                // m|P
                let mut pfx_node_type = graph_short_name.clone();
                pfx_node_type.push('|');
                pfx_node_type.push_str(ty_short_nm.get(&node.node_type[..]).unwrap());         
                
                
                let mut sk =  graph_short_name.clone();
                sk.push('|');
                sk.push_str("T#");
                
	
				let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                let put = put.item("PK", AttributeValue::B(puid_b.clone()))
                .item("SK", AttributeValue::S(sk.to_string()))
                .item("graph",AttributeValue::S(graph_short_name.clone()))
                .item("isNode",AttributeValue::S("Y".to_owned()))
                .item("Ty",AttributeValue::S(ty_short_nm.get(&node.node_type).unwrap().to_owned()));
                
                bat_w_req = save_item(&dynamo_client, bat_w_req, &retry_ch, put).await;
                //
                // dynamdb - put node scalar data
                //
                for (k,nv) in flat_rdf_map {
                
                    if nv.ty.unwrap().dt == "Nd" {
                        continue
                    }
		            let mut sortk = String::new();
		            sortk.push_str(graph_short_name.as_str());
		            sortk.push('|');
		            sortk.push_str(&nv.sortk);
		                
	                // include graph (short) name with attribute name in index
	                let mut attr_key = String::new();
	                attr_key.push_str(&k);
	                attr_key.push('#');
	                attr_key.push_str(&node.node_type);
	                
		            let mut pfx_attr_name = String::new(); 
		            pfx_attr_name.push_str(graph_short_name.as_str());
		            pfx_attr_name.push('|');
		            pfx_attr_name.push_str(&k);
		            //  appending node Type short name e.g. m|name|P
		            //  pfx_attr_name.push('|');
		            //  pfx_attr_name.push_str(ty_short_nm.get(&node.node_type[..]).unwrap());

                    // load programs only deals with static data. Edge add is added in the "attach" program.

                    let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                    let put = put.item("PK", AttributeValue::B(puid_b.clone()))
                    .item("SK", AttributeValue::S(sortk));
            
                    if nv.ty.is_none() {
                        if nv.value_str.is_none() {
                             panic!("ty is none {} {} No-value",k, nv.sortk);
                        } else {
                            panic!("ty is none {} {} {}",k, nv.sortk, nv.value_str.unwrap());
                        }
                        continue;
                    }
                    
                    let put = match nv.ty.unwrap().dt.as_str() {

                        "I","F" => {
                            // adopt this pattern for storing nullable types.
                            // I and F whichboth use the N attribute (value AttributeValue::N).
                            // On reading from db either
                            // 1. use type system to determine whether to read N as an I or F 
                            // 2. use TyA attribute which stores either an I or F. So type details are stored in the db 
                            // Also, use either
                            // 1. Nul attribute to indicate NULL value in associated N attribute
                            // 2. store AttributeValue::Null in place of AttributeValue::N in N attribute
                            match nv.value_str {
                                None => { 
                                        if !nv.ty.nullable  {
                                            panic!("Data error: attribute {} is not nullable", k)
                                        }
                                		put.item("P", AttributeValue::S(pfx_attr_name))
				                        .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
                                        .item("Nul", AttributeValue::Null(true));
				                        match nv.ty.dt {
                                            "I" => { put.item("N", AttributeValue::Null(true) },
                                            "F" => { put.item("N", AttributeValue::Null(true) },
                                        }
                                Some(_) => {
                                		put.item("N", AttributeValue::S(nv.value_str)
                                		.item("P", AttributeValue::S(pfx_attr_name))
				                        .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				                        .item("Nul", AttributeValue::Null(false));
				                        match nv.ty.dt {
                                            "I" => { put.item("N", AttributeValue::S("I".to_owned()) },
                                            "F" => { put.item("N", AttributeValue::S("F".to_owned()) },
                                        }
                                }
                            };

                            ,
                            
				        "S" => {
				            let put = match nv.ty.unwrap().ix.as_str() {

				                "FTg"|"ftg" => {
				                     put.item("P", AttributeValue::S(pfx_attr_name))
				                     .item("S", AttributeValue::S(nv.value_str.unwrap().to_owned()))
				                     .item("E", AttributeValue::S("S".to_string()))
				                    }   
				                "FT"|"ft" => {
				                    put.item("S",AttributeValue::S(nv.value_str.unwrap().to_owned()) )
				                    .item("E", AttributeValue::S("S".to_owned()))
				                    }
				                _ => {
				                     put.item("P", AttributeValue::S(pfx_attr_name))
				                     .item("S", AttributeValue::S(nv.value_str.unwrap().to_owned()))
				                    } 
				            };
				            put.item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("S".to_owned()))
				            }
				            
				        // TODO: "LS", "LN", "LB", "LBl" 
				        
				        "B" => {
                            put.item("B", AttributeValue::S(nv.value_str.unwrap().to_owned()))
                            .item("P", AttributeValue::S(pfx_attr_name))
                            .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("B".to_owned()))
				            } 
				            
				        "Bl" => {
                            put.item("Bl", AttributeValue::S(nv.value_str.unwrap().to_owned()))
                            .item("P", AttributeValue::S(pfx_attr_name))
                            .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("Bl".to_owned()))
				            }
				        
				        "LS" => {
				            let mut ls : Vec<AttributeValue>= vec![];
				            for v in nv.value_vstr.unwrap() {
				                ls.push(AttributeValue::S(v.to_owned()))
				            }
                            put.item("LS", AttributeValue::L(ls))
                            .item("P", AttributeValue::S(pfx_attr_name))
                            .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("LS".to_owned()))
				            }
				        
				        "LI" => {
				            let mut ls : Vec<AttributeValue>= vec![];
				            for v in nv.value_vstr.unwrap() {
				                ls.push(AttributeValue::N(v.to_owned()))
				            }
                            put.item("LI", AttributeValue::L(ls))
                            .item("P", AttributeValue::S(pfx_attr_name))
                            .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("LI".to_owned()))
				            }
				            
				        "LF" => {
				            let mut ls : Vec<AttributeValue>= vec![];
				            for v in nv.value_vstr.unwrap() {
				                ls.push(AttributeValue::N(v.to_owned()))
				            }
                            put.item("LS", AttributeValue::L(ls))
                            .item("P", AttributeValue::S(pfx_attr_name))
                            .item("Ty", AttributeValue::S(pfx_node_type.to_string()))
				            .item("TyA", AttributeValue::S("LF".to_owned()))
				            }
				            
				        _ => {put}

                    };
                            
                    bat_w_req = save_item(&dynamo_client, bat_w_req, &retry_ch, put).await;
                }
                
                bat_w_req
            });
        }
    }
    println!("db_load_process finished....");
    Ok(())
}

async fn save_item(
    dynamo_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    put : PutRequestBuilder ) -> Vec<WriteRequest> {
                        
    match put.build() {
        Err(err) => {
                    println!("error in write_request builder: {}",err);
            }
        Ok(req) =>  {
                    bat_w_req.push(WriteRequest::builder().put_request(req).build());
            }
    } 
    if bat_w_req.len() == DYNAMO_BATCH_SIZE {
        bat_w_req = persist_batch(dynamo_client, bat_w_req, retry_ch).await;
    }
    bat_w_req
}

async fn persist_batch(
    dynamo_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
) -> Vec<WriteRequest> {
    let bat_w_outp = dynamo_client
        .batch_write_item()
        .request_items("RustGraph.dev.2", bat_w_req)
        .send()
        .await;

    match bat_w_outp {
        Err(err) => {
            panic!(
                "Error in Dynamodb batch write in persist_batch() - {}",
                err
            );
        }
        Ok(resp) => {
            if resp.unprocessed_items.as_ref().unwrap().values().len() > 0 {
                // send unprocessed writerequests on retry channel
                for (k, v) in resp.unprocessed_items.unwrap() {
                    println!("persist_batch, unprocessed items..delay 1secs");
                    sleep(Duration::from_millis(1000)).await;
                    let resp = retry_ch.send(v).await;

                    if let Err(err) = resp {
                        panic!("Error sending on retry channel : {}", err);
                    }
                }

                // TODO: aggregate batchwrite metrics in bat_w_output.
                // pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
                // pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
            }
        }
    }
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}

fn gen_sortk(at: Option<&types::TyAttrD>) -> String {
    let mut s = String::new();
    let s = match at {
        Some(at) => {
            s.push_str("A#");
            if at.dt == "Nd" {
                s.push_str("G#:");
                s.push_str(at.c.as_ref());
            } else {
                s.push_str(at.p.as_ref());
                s.push_str("#:");
                s.push_str(at.c.as_ref());
            }
            s
        }
        None => "T#".to_string(),
    };
    s
}
