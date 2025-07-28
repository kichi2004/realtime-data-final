use server::observation_points::{load_observation_points, ObservationPoint};
use chrono::NaiveDate;
use csv::Reader;
use server::*;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

// 1日ごとの時間間隔（ミリ秒）
const INTERVAL_MILLS: u64 = 32; // 16
// 取得する年（10 年単位）
const DECADES: [u32; 5] = [1980, 1990, 2000, 2010, 2020];

fn compress_data(date: NaiveDate, data: &Vec<ObservationPointData>) -> Vec<u8> {
    let mut res = Vec::new();
    res.push(data.len() as u8);
    let days = date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    res.extend_from_slice(&(days.num_days() as u16).to_be_bytes());

    for row in data {
        res.extend_from_slice(&row.compress());
    }

    res
}


fn run_process(mut stream: TcpStream, observation_points: &Vec<ObservationPoint>) -> Result<(), anyhow::Error> {
    let observation_points: Vec<_> = observation_points.iter().filter(|x| x.path().is_dir()).collect();
    for decade in DECADES {
        let mut readers = observation_points.iter().map(|point| {
            let mut path = point.path();
            path.push(decade.to_string() + ".csv");
            let reader = Reader::from_path(path).unwrap();
            (point.id(), reader)
        }).collect::<Vec<_>>();
        let mut records = readers.iter_mut()
            .map(|(n, reader)| (*n, reader.records()))
            .collect::<Vec<_>>();
        'outer: loop {
            thread::sleep(Duration::from_millis(INTERVAL_MILLS));
            let mut date = None;
            let mut rows = Vec::new();
            for (n, reader) in records.iter_mut() {
                let value = reader.next();
                if let Some(data) = value {
                    let data = data?;
                    if data[1].is_empty() { continue; }

                    date.get_or_insert(NaiveDate::parse_from_str(&data[0], "%Y/%m/%d")?);
                    rows.push(ObservationPointData::new(*n, &data[1], &data[2], &data[3]));
                } else {
                    break 'outer;
                }
            }
            let data_to_send = compress_data(date.unwrap(), &rows);
            stream.write_all(&data_to_send)?;
        }
    }
    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    let observation_points = load_observation_points("data/observation.csv")?;

    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, PORT))?;
    println!("Listening on {}", listener.local_addr()?);

    loop {
        let (socket, addr) = listener.accept()?;
        println!("Accepted connection from {}", addr);
        if run_process(socket, &observation_points).is_ok() {
            println!("Completed")
        } else {
            println!("Disconnected")
        }
    }
}
