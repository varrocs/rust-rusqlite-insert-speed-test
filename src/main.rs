use rusqlite::{params, Connection};
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

enum Command {
    Tick(),
    Value(i32, i32),
    End(),
}

const FEEDERS: i32 = 1200;
const FEEDER_SLEEP_MS: u64 = 10;
const COMMIT_HEARTBEAT_MS: u64 = 1000;
const FULL_TIME: u64 = 10000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::channel(32);
    let tx_timer = tx.clone();

    let now = SystemTime::now();

    // Sqlite task
    tokio::task::spawn_blocking(move || {
        let filename = format!("DB_{:?}.db", now);
        let mut conn = Connection::open(filename).unwrap();
        conn.execute(
            "CREATE TABLE insert_test (
		    id integer primary key autoincrement,
		    coords text null,
		    cre_date DATETIME DEFAULT CURRENT_TIMESTAMP);
	    ",
            params![],
        )
        .unwrap();

        let mut counter = 0;
        let start = Instant::now();
        let mut ended = false;
        while !ended {
            let tr = conn.transaction().unwrap();
            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    Command::Tick() => {
                        println!("TICK");
                        break;
                    }
                    Command::Value(v1, v2) => {
                        counter += 1;
                        let v = format!("'{}, {}'", v1, v2);
                        tr.execute("insert into insert_test(coords) values (?)", params![v])
                            .unwrap();
                    }
                    Command::End() => {
                        ended = true;
                        break;
                    }
                }
            }
            //          }
            tr.commit().unwrap();
        }
        println!(
            "Had {} entries in {} millisecs",
            counter,
            start.elapsed().as_millis()
        );
        println!(
            "Should have {} in {} millisecs",
            (FEEDERS as u64 * FULL_TIME / FEEDER_SLEEP_MS),
            FULL_TIME
        );
    });

    // Heartbeat task
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(COMMIT_HEARTBEAT_MS)).await;
            tx_timer.send(Command::Tick()).await.ok();
        }
    });

    // Feeder tasks
    for i in 1..FEEDERS {
        let tx_feeder = tx.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(FEEDER_SLEEP_MS)).await;
                tx_feeder.send(Command::Value(i, 299)).await.ok();
            }
        });
    }

    sleep(Duration::from_millis(FULL_TIME)).await;
    tx.send(Command::End()).await.ok();
    sleep(Duration::from_millis(100)).await;
    Ok(())
}
