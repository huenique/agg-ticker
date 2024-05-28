wit_bindgen::generate!();

use exports::jabratech::component_agg_ticker::agg_ticker::{Guest, AggregatedTickers};
use wasi::logging::logging::*;
use wasmcloud::messaging::*;

struct AggTicker;

impl Guest for AggTicker {
    fn aggregate(_instrument_name: String) -> Result<AggregatedTickers, String> {
        todo!("Implement the single aggregate method")
    }

    fn aggregate_and_publish(_instrument_name: String) {

        let agg_tickers = AggregatedTickers {
            instrument_name: "BTC-TEST-INSTRUMENT".to_string(),
        };

        // TODO: Figure out if we can query the list of linked exchanges via ticker-provider
        // Retrieve all ticker and put into a list
        // sort the best bid and best ask list
        // publish the best bid and best ask list
        publish(agg_tickers)
    }
}

fn publish(agg_tickers: AggregatedTickers) {
    let subject = "agg-ticker".to_string();
    let body = "Hello, world from NATS!".to_string();
    let _ = consumer::publish(&types::BrokerMessage {
        subject: subject,
        reply_to: None,
        body: body.into(),
    });
}

export!(AggTicker);
