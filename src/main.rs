extern crate pretty_bytes;

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate rusoto_core;
extern crate rusoto_kinesis;

extern crate env_logger;
#[macro_use]
extern crate log;

use std::{io,env};
use std::sync::Arc;
use std::time::{Instant};
use std::error::Error;

use rusoto_core::Region;
use rusoto_core::reactor::{CredentialsProvider, RequestDispatcher};
use rusoto_kinesis::{Kinesis, KinesisClient, ListStreamsInput, PutRecordsRequestEntry, PutRecordsInput};

#[derive(Serialize)]
struct FauxLog {
    msg: &'static str
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
        1000
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

    let hs: Vec<std::thread::JoinHandle<()>> = (0..num_threads)
        .map(|_| {
            let client = client.clone();
            let stream_name = stream_name.clone();
            std::thread::spawn(move || {
                let client = Arc::new(KinesisClient::simple(Region::UsWest2));
                let start = Instant::now();
                let res = send_to_kinesis_sync(client, stream_name, num_puts, puts_size);
                match res {
                    Ok(bytes) => {
                        info!("successfully sent to kinesis ({} bytes, {} seconds)", bytes, start.elapsed().as_secs());
                    },
                    Err(e) => {
                        error!("failed to send to kinesis: {:?}", e.description());
                    }
                };
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
}

fn send_to_kinesis_sync(client: DefaultKinesisClient, stream_name: String, num_puts: usize, puts_size: usize) -> Result<usize,rusoto_kinesis::PutRecordsError> {
    let serialize_data = serde_json::to_vec(&FauxLog{ msg: TEST_BUF })?;
    let logs : Vec<PutRecordsRequestEntry> = (0..puts_size).map(|n|{
        PutRecordsRequestEntry {
            data: serialize_data.clone(),
            explicit_hash_key: None,
            partition_key: format!("{}",n)
        }
    }).collect();

    let approx_size = serialize_data.len() * num_puts;

    for _ in 0..num_puts {
        client.put_records(&PutRecordsInput{
            records: logs.clone(),
            stream_name: stream_name.clone(),
        }).sync()?;
    };

    Ok(approx_size)
}