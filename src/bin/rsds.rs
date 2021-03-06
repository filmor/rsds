use std::net::{Ipv4Addr, SocketAddr};
use std::thread;

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use rsds::prelude::*;
use rsds::scheduler::interface::prepare_scheduler_comm;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds", about = "Rust Dask Scheduler")]
struct Opt {
    #[structopt(long, default_value = "7070")]
    port: u16,
}

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let opt = Opt::from_args();

    log::info!("rsds v0.0 started");

    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opt.port);
    log::info!("listening on port {}", address);
    let listener = TcpListener::bind(address).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    thread::spawn(move || {
        let scheduler = rsds::scheduler::implementation::Scheduler::new();
        let mut runtime = Runtime::new().expect("Runtime creation failed");
        runtime
            .block_on(scheduler.start(comm))
            .expect("Scheduler failed");
    });

    let core_ref = CoreRef::new(sender);
    let core_ref2 = core_ref.clone();
    core_ref2.observe_scheduler(receiver).await;

    // tokio::spawn(async move {
        rsds::connection::connection_initiator(listener, core_ref)
            .await
            .expect("Connection initiator failed");
    // });

    Ok(())
}
