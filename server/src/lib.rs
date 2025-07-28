use chrono::NaiveDate;

pub mod observation_points;


// ポート番号
pub const PORT: u16 = 6051;

#[derive(Debug)]
pub struct ObservationPointData {
    id: u32,
    average: i32,
    max: i32,
    min: i32,
}

impl ObservationPointData {
    pub fn point_id(&self) -> u32 {
        self.id
    }
    pub fn average(&self) -> f64 {
        self.average as f64 / 10.0
    }
    pub fn max(&self) -> f64 {
        self.max as f64 / 10.0
    }
    pub fn min(&self) -> f64 {
        self.min as f64 / 10.0
    }

    fn parse_temp(s: &str) -> i32 {
        let mut res = 0;
        let mut flag = 1;
        for c in s.chars() {
            match c {
                '-' => { flag = -1; continue; }
                '.' => { continue; }
                '0'..='9' => { res = res * 10 + (c as i32 - '0' as i32); }
                _ => unreachable!(),
            }
        }
        res * flag
    }

    pub fn new(id: u32, average: &str, max: &str, min: &str) -> Self {
        Self {
            id,
            average: Self::parse_temp(average),
            max: Self::parse_temp(max),
            min: Self::parse_temp(min),
        }
    }

    pub fn compress(&self) -> [u8; 6] {
        let mut res = [0u8; 6];
        let id = (self.id - 40000) as u16;
        let ave = (self.average + 512) as u32 & 0x03FF;
        let max = (self.max + 512) as u32 & 0x03FF;
        let min = (self.min + 512) as u32 & 0x03FF;
        let temperature = (max << 20) | (min << 10) | ave;

        let (r_id, r_temp) = res.split_at_mut(2);
        r_id.copy_from_slice(&id.to_be_bytes());
        r_temp.copy_from_slice(&temperature.to_be_bytes());

        res
    }

    pub(crate) fn decompress(data: &[u8; 6]) -> Self {
        let (first, last) = data.split_at(2);
        let id = u16::from_be_bytes(first.try_into().unwrap()) as u32 + 40000;

        let temperature = u32::from_be_bytes(last.try_into().unwrap());
        let max = ((temperature >> 20) & 0x03FF).cast_signed() - 512;
        let min = ((temperature >> 10) & 0x03FF).cast_signed() - 512;
        let average = (temperature & 0x03FF).cast_signed() - 512;

        Self { id, average , max, min}
    }
}

pub fn decompress_data(mut data: &[u8]) -> (NaiveDate, Vec<ObservationPointData>) {
    let len = *data.split_off_first().unwrap() as usize;
    let days = data.split_off(..2).unwrap();
    let days = u16::from_be_bytes(days.try_into().unwrap());
    let mut res = Vec::with_capacity(len);
    for _ in 0..len {
        let tmp = data.split_off(..6).unwrap();
        res.push(ObservationPointData::decompress(tmp.try_into().unwrap()));
    }

    (NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(days as i64), res)
}
