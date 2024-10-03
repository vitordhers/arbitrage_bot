use std::{collections::HashMap, result, time::Duration};

use reqwest::Error;
use serde::Deserialize;
use tokio::time::sleep;

const BINANCE_FEE_RATE: f64 = 0.001;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let symbol = Symbol::default();

    let binance_order_book = fetch_binance_order_book(symbol).await?;
    let mb_order_book = fetch_mb_order_book(symbol).await?;

    let mut balance = get_default_balance();

    if let Some((_, action)) = check_arbitrage(binance_order_book, mb_order_book, symbol) {
        // execute trade
        let balance_result = take_trade_action(action, balance).await?;
        balance = balance_result;
    }

    println!("current balance = {:?}", balance);

    Ok(())
}

async fn fetch_binance_order_book(symbol: Symbol) -> Result<OrderBook, Error> {
    let symbol = symbol.get_binance_symbol_param();
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=1",
        symbol
    );
    let response = reqwest::get(&url).await?;
    let order_book: BinanceOrderBookData = response.json().await?;
    let order_book: OrderBook = order_book.into();
    Ok(order_book)
}

async fn fetch_mb_order_book(symbol: Symbol) -> Result<OrderBook, Error> {
    let symbol = symbol.get_mb_symbol_param();
    let url = format!(
        "https://www.mercadobitcoin.net/api/{}/orderbook?limit=1",
        symbol
    );
    let response = reqwest::get(&url).await?;
    let order_book: MBOrderBookData = response.json().await?;
    let order_book: OrderBook = order_book.into();
    Ok(order_book)
}

fn check_arbitrage(
    binance_order_book: OrderBook,
    mb_order_book: OrderBook,
    symbol: Symbol,
) -> Option<(f64, TradeAction)> {
    let binance_ask = binance_order_book.asks.first()?;
    let mb_bid = mb_order_book.bids.first()?;
    let best_ask_binance = binance_ask.price;
    let best_bid_mb = mb_bid.price;

    let mb_ask = mb_order_book.asks.first()?;
    let binance_bid = binance_order_book.bids.first()?;
    let best_ask_mb = mb_ask.price;
    let best_bid_binance = binance_bid.price;

    // calculate fees

    if best_bid_mb > best_ask_binance {
        let costless_profit = best_bid_mb - best_ask_binance;
        println!("costless_profit {}", costless_profit);
        let binance_cost = best_ask_binance * binance_ask.qty * BINANCE_FEE_RATE;
        let mb_cost = best_bid_mb * mb_bid.qty * get_mb_fee_rate(best_bid_mb, mb_bid.qty);
        let costs = -binance_cost - mb_cost;
        let profit = costless_profit + costs;
        println!("profit {}", profit);
        if profit >= 0.0 {
            return Some((
                profit,
                TradeAction::ShortMb {
                    ask_price: best_ask_binance,
                    bid_price: best_bid_mb,
                    qty: f64::min(binance_ask.qty, mb_bid.qty),
                    symbol,
                    costs,
                },
            ));
        }
    } else if best_bid_binance > best_ask_mb {
        let costless_profit = best_bid_binance - best_ask_mb;
        println!("costless_profit {}", costless_profit);

        let binance_cost = best_bid_binance * binance_bid.qty * BINANCE_FEE_RATE;
        let mb_cost = best_ask_mb * mb_ask.qty * get_mb_fee_rate(best_ask_mb, mb_ask.qty);
        let costs = -binance_cost - mb_cost;
        let profit = costless_profit + costs;
        println!("profit {}", profit);

        if profit >= 0.0 {
            return Some((
                profit,
                TradeAction::ShortBinance {
                    ask_price: best_ask_mb,
                    bid_price: best_bid_binance,
                    qty: f64::min(binance_bid.qty, mb_ask.qty),
                    symbol,
                    costs,
                },
            ));
        }
    }

    None
}

enum TradeAction {
    ShortBinance {
        ask_price: f64,
        bid_price: f64,
        qty: f64,
        symbol: Symbol,
        costs: f64,
    },
    ShortMb {
        ask_price: f64,
        bid_price: f64,
        qty: f64,
        symbol: Symbol,
        costs: f64,
    },
}

fn get_default_balance() -> HashMap<Currency, f64> {
    let mut balances: HashMap<Currency, f64> = HashMap::new();
    balances.insert(Currency::BRL, 50_000.0);
    balances.insert(Currency::BTC, 0.0);
    balances.insert(Currency::ETH, 0.0);
    balances.insert(Currency::USDT, 0.0);
    balances
}

async fn take_trade_action(
    action: TradeAction,
    current_balance: HashMap<Currency, f64>,
) -> Result<HashMap<Currency, f64>, Error> {
    let (ask_price, bid_price, qty, symbol, costs) = match action {
        TradeAction::ShortBinance {
            ask_price,
            bid_price,
            qty,
            symbol,
            costs,
        } => {
            let _ = short_binance(symbol, qty, bid_price).await?;
            let _ = long_mb(symbol, qty, ask_price).await?;
            (ask_price, bid_price, qty, symbol, costs)
        }
        TradeAction::ShortMb {
            ask_price,
            bid_price,
            qty,
            symbol,
            costs,
        } => {
            let _ = short_mb(symbol, qty, bid_price).await?;
            let _ = long_binance(symbol, qty, ask_price).await?;
            (ask_price, bid_price, qty, symbol, costs)
        }
    };

    match symbol {
        Symbol::BTCBRL => {
            let balance_brl = current_balance.get(&Currency::BRL).unwrap();
            let balance_btc = current_balance.get(&Currency::BTC).unwrap();
            let balance_brl = balance_brl - (qty * ask_price) - costs;
            let balance_btc = balance_btc + qty;
            let mut current_balance = current_balance.clone();
            current_balance.insert(Currency::BRL, balance_brl);
            current_balance.insert(Currency::BTC, balance_btc);
            Ok(current_balance)
        }
        Symbol::USDTBRL => {
            let balance_brl = current_balance.get(&Currency::BRL).unwrap();
            let balance_usdt = current_balance.get(&Currency::USDT).unwrap();
            let balance_brl = balance_brl - (qty * ask_price) - costs;
            let balance_usdt = balance_usdt + qty;
            let mut current_balance = current_balance.clone();
            current_balance.insert(Currency::BRL, balance_brl);
            current_balance.insert(Currency::USDT, balance_usdt);
            Ok(current_balance)
        }
        Symbol::ETHBRL => {
            let balance_brl = current_balance.get(&Currency::BRL).unwrap();
            let balance_eth = current_balance.get(&Currency::ETH).unwrap();
            let balance_brl = balance_brl - (qty * ask_price) - costs;
            let balance_eth = balance_eth + qty;
            let mut current_balance = current_balance.clone();
            current_balance.insert(Currency::BRL, balance_brl);
            current_balance.insert(Currency::ETH, balance_eth);
            Ok(current_balance)
        }
    }
}

async fn short_binance(symbol: Symbol, qty: f64, price: f64) -> Result<(), Error> {
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn long_binance(symbol: Symbol, qty: f64, price: f64) -> Result<(), Error> {
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn short_mb(symbol: Symbol, qty: f64, price: f64) -> Result<(), Error> {
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn long_mb(symbol: Symbol, qty: f64, price: f64) -> Result<(), Error> {
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

fn get_mb_fee_rate(price: f64, qty: f64) -> f64 {
    let deal_total = price * qty;

    match deal_total {
        t if t <= 500_000.0 => 0.007,
        t if t > 500_000.0 && t <= 10_000_00.0 => 0.006,
        t if t > 10_000_000.0 && t <= 20_000_00.0 => 0.005,
        t if t > 20_000_000.0 && t <= 50_000_00.0 => 0.0045,
        t if t > 50_000_000.0 && t <= 100_000_00.0 => 0.004,
        t if t > 100_000_000.0 && t <= 200_000_00.0 => 0.003,
        t if t > 200_000_000.0 => 0.0025,
        _ => unreachable!(),
    }
}

#[derive(Clone, Copy, Deserialize, Debug)]
struct Data {
    qty: f64,
    price: f64,
}

impl Data {
    fn new(price: f64, qty: f64) -> Self {
        Self { qty, price }
    }
}

#[derive(Debug, Deserialize)]
struct OrderBook {
    bids: Vec<Data>,
    asks: Vec<Data>,
}

impl OrderBook {
    pub fn new_from_string(bids: Vec<[String; 2]>, asks: Vec<[String; 2]>) -> Self {
        let bids = bids
            .into_iter()
            .map(|b| {
                Data::new(
                    b.get(0).unwrap().parse::<f64>().unwrap(),
                    b.get(1).unwrap().parse::<f64>().unwrap(),
                )
            })
            .collect::<Vec<Data>>();
        let asks = asks
            .into_iter()
            .map(|a| {
                Data::new(
                    a.get(0).unwrap().parse::<f64>().unwrap(),
                    a.get(1).unwrap().parse::<f64>().unwrap(),
                )
            })
            .collect::<Vec<Data>>();

        Self { bids, asks }
    }

    fn new_from_f64(bids: Vec<[f64; 2]>, asks: Vec<[f64; 2]>) -> Self {
        let bids = bids
            .into_iter()
            .map(|b| Data::new(*b.get(0).unwrap(), *b.get(1).unwrap()))
            .collect::<Vec<Data>>();
        let asks = asks
            .into_iter()
            .map(|a| Data::new(*a.get(0).unwrap(), *a.get(1).unwrap()))
            .collect::<Vec<Data>>();

        Self { bids, asks }
    }
}

impl From<BinanceOrderBookData> for OrderBook {
    fn from(value: BinanceOrderBookData) -> Self {
        OrderBook::new_from_string(value.bids, value.asks)
    }
}

impl From<MBOrderBookData> for OrderBook {
    fn from(value: MBOrderBookData) -> Self {
        OrderBook::new_from_f64(value.bids, value.asks)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct BinanceOrderBookData {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Clone, Debug, Deserialize)]
struct MBOrderBookData {
    pub timestamp: u64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Currency {
    BRL,
    BTC,
    USDT,
    ETH,
}

#[derive(Clone, Copy, Debug, Default)]
enum Symbol {
    #[default]
    BTCBRL,
    USDTBRL,
    ETHBRL,
}

impl Symbol {
    fn get_binance_symbol_param(&self) -> &str {
        match self {
            Self::BTCBRL => "BTCBRL",
            Self::USDTBRL => "USDTBRL",
            Self::ETHBRL => "ETHBRL",
        }
    }

    fn get_mb_symbol_param(&self) -> &str {
        match self {
            Self::BTCBRL => "BTC",
            Self::USDTBRL => "USDT",
            Self::ETHBRL => "ETH",
        }
    }
}
