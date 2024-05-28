wit_bindgen::generate!();

use exports::jabratech::component_agg_ticker::agg_ticker::{Guest, AggregatedTickers};
use wasi::logging::logging::*;
use wasi::random::random::*;
use wasmcloud::messaging::*;
use serde::ser::{ Serializer, SerializeStruct};
use serde::Serialize;
use serde_json;
use std::vec::Vec;

struct AggTicker;

impl Guest for AggTicker {
    fn aggregate(instrument_name: String) -> Result<AggregatedTickers, String> {
        todo!("Implement the single aggregate method")
    }

    fn aggregate_and_publish(instrument_name: String) {
        log(
            Level::Debug,
            "Received instrument name",
            &instrument_name,
        );
        

        // TODO: Figure out if we can query the list of linked exchanges via ticker-provider link
        // TODO: Retrieve all ticker data and merge into one list

        // sort the best bid and best ask list by passing the merged list to the aggregate function
        let agg_tickers = aggregate(&instrument_name);

        // publish the aggregated tickers
        publish(instrument_name, agg_tickers);
    }
}

// TODO: Add merged list of tickers as input
fn aggregate(instrument_name: &str) -> AggregatedTickers {

    // TODO: Implement the aggregation logic

    // For now, we will generate random data
    let agg_tickers = AggregatedTickers {
        instrument_name: instrument_name.to_string(),
        strike: 68000.0,
        kind: "Call".to_string(),
        delta: get_random_f64(),
        bids: vec![
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
        ],
        asks: vec![
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
            (get_random_f64(), get_random_f64(), get_random_f64(), get_random_f64(), "Dummy".to_string()),
        ],
        timestamp: 0,
    };

    agg_tickers
}

fn publish(instrument_name: String, agg_tickers: AggregatedTickers) {
    let subject = format!("agg-ticker.{}", instrument_name);

    let json_string = serde_json::to_string(&agg_tickers).unwrap();
    let _ = consumer::publish(&types::BrokerMessage {
        subject: subject,
        reply_to: None,
        body: json_string.into(),
    });
}

fn get_random_f64() -> f64 {
    let random_u64 = get_random_u64();
    let random_f64 = random_u64 as f64;
    random_f64
}


// Create a struct to represent the bid/ask tuple for serialization
#[derive(Serialize)]
struct BidAsk {
    price: f64,
    size: f64,
    volume: f64,
    value: f64,
    description: String,
}

// Manually implement Serialize for AggregatedTickers
impl Serialize for AggregatedTickers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AggregatedTickers", 7)?;
        state.serialize_field("instrument_name", &self.instrument_name)?;
        state.serialize_field("strike", &self.strike)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("delta", &self.delta)?;
        
        let bids: Vec<(String, String, String, String, String)> = self.bids.iter().map(|b| (
            format!("{:.2}", b.0),
            format!("{:.2}", b.1),
            format!("{:.2}", b.2),
            format!("{:.2}", b.3),
            b.4.clone(),
        )).collect();
        state.serialize_field("bids", &bids)?;
        
        let asks: Vec<(String, String, String, String, String)> = self.asks.iter().map(|a| (
            format!("{:.2}", a.0),
            format!("{:.2}", a.1),
            format!("{:.2}", a.2),
            format!("{:.2}", a.3),
            a.4.clone(),
        )).collect();
        state.serialize_field("asks", &asks)?;
        
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

export!(AggTicker);
