extern crate pretty_bytes;

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate rusoto_core;
extern crate rusoto_kinesis;

extern crate futures;
extern crate spmc;

extern crate env_logger;
#[macro_use]
extern crate log;

use std::{env, io};
use std::sync::Arc;
use std::time::Instant;
use std::error::Error;

use rusoto_core::Region;
use rusoto_core::reactor::{CredentialsProvider, RequestDispatcher};
use rusoto_kinesis::{Kinesis, KinesisClient, ListStreamsInput, PutRecordsInput,
                     PutRecordsRequestEntry};

#[derive(Serialize)]
struct FauxLog {
    msg: &'static str,
}

type DefaultKinesisClient = Arc<KinesisClient<CredentialsProvider, RequestDispatcher>>;

// I don't want to leak ARNs in to public code, so this little ditty pulls the name out of AWS
fn get_kinesis_stream_name(thing: &DefaultKinesisClient) -> io::Result<String> {
    let streams_response = thing.list_streams(&ListStreamsInput {
        exclusive_start_stream_name: None,
        limit: None,
    });

    let streams = match streams_response.sync() {
        Ok(output) => output,
        Err(list_err) => {
            println!("oh no");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("could not list Kinesis streams {:?}", list_err),
            ));
        }
    };

    if streams.stream_names.len() > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "I can only auto-discover one Kinesis stream",
        ));
    }

    Ok(streams.stream_names[0].clone())
}

const TEST_BUF : &str = r##"<14>Dec 13 17:45:02 SANTA-CLAUS-W764.blerg.com nxWinEvt: {"EventTime":"2017-12-19 17:45:02","Hostname":"fake-hostname","Keywords":-9214364837600034816,"EventType":"AUDIT_SUCCESS","SeverityValue":2,"Severity":"INFO","EventID":4656,"SourceName":"Microsoft-Windows-Security-Auditing","ProviderGuid":"{54849625-5478-4994-A5BA-3E3B0328C30D}","Version":1,"Task":12804,"OpcodeValue":0,"RecordNumber":7613465324,"ProcessID":892,"ThreadID":908,"Channel":"Security","AccessReason":"-","AccessMask":"0x2","PrivilegeList":"-","RestrictedSidCount":"0","ProcessName":"C:\\Windows\\System32\\svchost.exe","EventReceivedTime":"2017-12-19 17:52:27","SourceModuleName":"eventlog","SourceModuleType":"im_msvistalog"}
"##;

fn main() {
    env_logger::init().unwrap();

    let args: Vec<String> = env::args().collect();
    let mut arg_iter = args.iter();
    let _ = arg_iter.next();

    let num_threads: usize = if let Some(n) = arg_iter.next() {
        n.parse().unwrap()
    } else {
        1
    };

    let num_puts: usize = if let Some(n) = arg_iter.next() {
        n.parse().unwrap()
    } else {
        10
    };

    let puts_size: usize = if let Some(n) = arg_iter.next() {
        n.parse().unwrap()
    } else {
        500
    };

    let client = Arc::new(KinesisClient::simple(Region::UsWest2));

    let stream_name = get_kinesis_stream_name(&client).unwrap();

    info!(
        "testing kinesis put_records num_threads={} num_puts={} puts_size={} stream_name={}",
        num_threads, num_puts, puts_size, stream_name
    );

//    kinesis_pipeline_threadpool(client, stream_name, num_threads, puts_size)
    kinesis_deep_futures_pipeline(client, stream_name, num_threads, puts_size)

//    let hs: Vec<std::thread::JoinHandle<()>> = (0..num_threads)
//        .map(|_| {
//            let client = client.clone();
//            let stream_name = stream_name.clone();
//            std::thread::spawn(move || {
//                //                do_thread_sync(client, stream_name, num_puts, puts_size)
//                kinesis_pipeline_threadpool(client, stream_name, num_puts, puts_size)
//            })
//        })
//        .collect();
//    for h in hs {
//        h.join().unwrap();
//    }
}

struct FauxData {
    serialize_data: Vec<u8>,
    n: usize,
}

impl FauxData {
    fn new() -> Self {
        FauxData {
            serialize_data: serde_json::to_vec(&FauxLog { msg: TEST_BUF }).unwrap(),
            n: 0,
        }
    }
}

impl Iterator for FauxData {
    type Item = PutRecordsRequestEntry;
    fn next(&mut self) -> Option<PutRecordsRequestEntry> {
        self.n += 1;
        Some(PutRecordsRequestEntry {
            data: self.serialize_data.clone(),
            explicit_hash_key: None,
            partition_key: format!("{}", self.n),
        })
    }
}

fn kinesis_deep_futures_pipeline(
    client: DefaultKinesisClient,
    stream_name: String,
    num_puts: usize,
    puts_size: usize,
) {
    use futures::sync::mpsc::{channel, spawn};
    use futures::{Future, Sink, Stream};
    use futures::stream::Sender;

    let data = FauxData::new();

    let (mut tx, mut rx) = channel(1);

    std::thread::spawn(move || {
        let puts = rx.chunks(500).map(|batch: Vec<PutRecordsRequestEntry>| {
            let client = Arc::new(KinesisClient::simple(Region::UsWest2));
            Ok(client.put_records(&PutRecordsInput {
                records: batch,
                stream_name: stream_name.clone(),
            }))
        }).buffer_unordered(200);

        for put_res in puts.wait() {
            if let Ok(put) = put_res {
                info!("hey, something is done");
            }
        }
    });

    let tx = std::rc::Rc::new(std::cell::RefCell::new(tx));

    for datum in data {
        loop {
            match tx.borrow_mut().try_send(datum.clone()) {
                Ok(_) => break,
                Err(e) => continue
            }
        }
    }
}