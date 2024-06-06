wit_bindgen::generate!();

use std::vec::Vec;

use crate::jabratech::common::types::Currency;
use crate::jabratech::common::types::InstrumentKind;
use crate::jabratech::provider_ticker::ticker::get_tickers;
use crate::jabratech::provider_ticker::ticker::Ticker;

use exports::jabratech::component_agg_ticker::agg_ticker::AggregatedTickers;
use exports::jabratech::component_agg_ticker::agg_ticker::Guest;

use serde::ser::SerializeStruct;
use serde::ser::Serializer;
use serde::Serialize;
use serde_json;
use wasi::logging::logging::log;
use wasi::logging::logging::Level;
use wasmcloud::messaging::consumer;
use wasmcloud::messaging::consumer::BrokerMessage;

struct AggTicker;

impl Guest for AggTicker {
    fn aggregate(instrument_name: String) -> Result<AggregatedTickers, String> {
        let (tickers, instrument_parts) = get_ticker_data_from_provider(&instrument_name)?;
        let agg_tickers = aggregate_tickers(instrument_name.clone(), tickers, instrument_parts);

        Ok(agg_tickers)
    }

    fn aggregate_and_publish(instrument_name: String) {
        match get_ticker_data_from_provider(&instrument_name) {
            Ok((tickers, instrument_parts)) => {
                let agg_tickers =
                    aggregate_tickers(instrument_name.clone(), tickers, instrument_parts);
                publish_aggregated(instrument_name, agg_tickers)
            }
            Err(e) => {
                log(Level::Error, "Failed to get ticker data from provider", &e);
            }
        }
    }
}

type Ccy<'a> = &'a str;
type Expiry<'a> = &'a str;
type OptionType<'a> = &'a str;
type Instrument<'a> = (Ccy<'a>, Expiry<'a>, f64, OptionType<'a>);

fn parse_instrument_name<'a>(instrument_name: &'a str) -> Result<Instrument<'a>, String> {
    let instrument_name_parts: Vec<&str> = instrument_name.split('-').collect();

    // Validate the number of parts
    // if instrument_name_parts.len() != 4 {
    //     return Err("Invalid instrument name format".to_string());
    // }

    let ccy = instrument_name_parts[0];
    let expiry = instrument_name_parts[1];
    let strike = instrument_name_parts[2]
        .parse::<f64>()
        .map_err(|e| format!("Failed to parse strike: {}", e))?;
    let option_type = instrument_name_parts[3];

    Ok((ccy, expiry, strike, option_type))
}

/// Fetch ticker data from the ticker provider
fn get_ticker_data_from_provider(
    instrument_name: &str,
) -> Result<(Vec<Ticker>, Instrument), String> {
    let instrument_parts = parse_instrument_name(&instrument_name)?;
    let currency_ticker = &instrument_parts.0;
    let kind_indicator = &instrument_parts.3;

    let currency = Currency {
        ticker: currency_ticker.to_string(),
        name: None,
        is_active: None,
        decimals: None,
        display_scale: None,
    };

    let instrument_kind = if kind_indicator.contains("C") || kind_indicator.contains("P") {
        InstrumentKind::Opt
    } else {
        InstrumentKind::Spot
    };

    let tickers = get_tickers(&currency, instrument_kind).map_err(|e| e.to_string())?;

    Ok((
        // Filter the tickers by instrument name
        tickers
            .into_iter()
            .filter(|ticker| ticker.instrument_name == instrument_name)
            .collect(),
        instrument_parts,
    ))
}

/// Aggregate the tickers to generate aggregated ticker data
fn aggregate_tickers(
    instrument_name: String,
    tickers: Vec<Ticker>,
    instrument_parts: Instrument,
) -> AggregatedTickers {
    // Aggregating best bid and ask prices and amounts
    let mut best_bids = Vec::new();
    let mut best_asks = Vec::new();

    for ticker in &tickers {
        best_bids.push((
            ticker.best_bid_price,
            ticker.best_bid_amount,
            ticker.bid_iv,
            ticker.index_price,
            ticker.instrument_name.clone(),
        ));
        best_asks.push((
            ticker.best_ask_price,
            ticker.best_ask_amount,
            ticker.ask_iv,
            ticker.index_price,
            ticker.instrument_name.clone(),
        ));
    }

    // Sorting bids and asks. This sorts in descending order of price for bids and ascending order for asks
    best_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    best_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    // Taking top 5 bids and asks for the aggregation
    let top_bids = best_bids.into_iter().take(5).collect();
    let top_asks = best_asks.into_iter().take(5).collect();

    let agg_tickers = AggregatedTickers {
        instrument_name: instrument_name.to_string(),
        strike: instrument_parts.2,
        kind: tickers
            .first()
            .map_or("Unknown".to_string(), |t| t.state.clone()),
        delta: tickers
            .first()
            .and_then(|t| t.greeks.as_ref())
            .map_or(0.0, |g| g.delta.unwrap_or(0.0)),
        bids: top_bids,
        asks: top_asks,
        timestamp: tickers.first().map_or(0, |t| t.timestamp),
    };

    agg_tickers
}

/// Publish the aggregated tickers
fn publish_aggregated(instrument_name: String, agg_tickers: AggregatedTickers) {
    let subject = format!("agg-ticker.{}", instrument_name);
    let json_string = serde_json::to_string(&agg_tickers).unwrap();
    let _ = consumer::publish(&BrokerMessage {
        subject,
        reply_to: None,
        body: json_string.into(),
    });
}

/// Create a struct to represent the bid/ask tuple for serialization
#[allow(unused)]
#[derive(Serialize)]
struct BidAsk {
    price: f64,
    size: f64,
    volume: f64,
    value: f64,
    description: String,
}

/// Manually implement Serialize for AggregatedTickers
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

        let bids: Vec<(String, String, String, String, String)> = self
            .bids
            .iter()
            .map(|b| {
                (
                    format!("{:.2}", b.0),
                    format!("{:.2}", b.1),
                    format!("{:.2}", b.2),
                    format!("{:.2}", b.3),
                    b.4.clone(),
                )
            })
            .collect();
        state.serialize_field("bids", &bids)?;

        let asks: Vec<(String, String, String, String, String)> = self
            .asks
            .iter()
            .map(|a| {
                (
                    format!("{:.2}", a.0),
                    format!("{:.2}", a.1),
                    format!("{:.2}", a.2),
                    format!("{:.2}", a.3),
                    a.4.clone(),
                )
            })
            .collect();
        state.serialize_field("asks", &asks)?;

        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

export!(AggTicker);
