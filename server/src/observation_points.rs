use csv::ReaderBuilder;
use std::path::{Path, PathBuf};
use serde::Serialize;

#[derive(Clone, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ObservationPoint {
    id: u32,
    prefecture: String,
    name: String,
    name_kana: String,
    info_name: String,
    address: String,
    latitude: f32,
    longitude: f32,
    altitude: i32,
    is_prefecture_center: bool,
}

impl ObservationPoint {
    pub fn id(&self) -> u32 { self.id }
    pub fn prefecture(&self) -> &str { &self.prefecture }
    pub fn name(&self) -> &str { &self.name }
    pub fn name_kana(&self) -> &str { &self.name_kana }
    pub fn info_name(&self) -> &str { &self.info_name }
    pub fn address(&self) -> &str { &self.address }
    pub fn latitude(&self) -> f32 { self.latitude }
    pub fn longitude(&self) -> f32 { self.longitude }
    pub fn altitude(&self) -> i32 { self.altitude }
    pub fn is_prefecture_center(&self) -> bool { self.is_prefecture_center }
    pub fn path(&self) -> PathBuf {
        let mut ret = PathBuf::from("data");
        ret.push(&self.name);
        ret
    }

    fn parse_coordinate(degrees: u32, minutes: f32) -> f32 {
        degrees as f32 + minutes / 60.0
    }

    fn from_csv_row(row: &Vec<&str>) -> Self {
        Self {
            prefecture: row[0].to_string(),
            id: row[1].parse().unwrap(),
            name: row[3].to_string(),
            name_kana: row[4].to_string(),
            info_name: row[5].to_string(),
            address: row[6].to_string(),
            longitude: Self::parse_coordinate(row[9].parse().unwrap(), row[10].parse().unwrap()),
            latitude: Self::parse_coordinate(row[7].parse().unwrap(), row[8].parse().unwrap()),
            altitude: row[11].parse().unwrap(),
            is_prefecture_center: row[12] == "1",
        }
    }
}

pub fn load_observation_points<P: AsRef<Path>>(path: P) -> anyhow::Result<Vec<ObservationPoint>> {
    let mut reader = ReaderBuilder::new().from_path(path)?;
    let mut observation_points = vec![];
    for row in reader.records() {
        let row = row?;
        let point = ObservationPoint::from_csv_row(&row.into_iter().collect());
        observation_points.push(point)
    }
    Ok(observation_points)
}
