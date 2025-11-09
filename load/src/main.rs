use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[tokio::main]
async fn main() {
    let mut tasks = vec![];

    for i in 0..50 {
        tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect("127.0.0.1:4000")
                .await
                .expect("Failed to connect");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);

            for j in 0..100 {
                let key = format!("key_{}_{}", i, j);
                let val = format!("val_{}_{}", i, j);
                let cmd = format!("SET {} {}\n", key, val);
                writer.write_all(cmd.as_bytes()).await.unwrap();

                let mut resp = String::new();
                if let Ok(_) = reader.read_line(&mut resp).await {
                    // Optionally print every N responses
                    if j % 50 == 0 {
                        println!("Client {i} got response: {}", resp.trim());
                    }
                }
            }

            println!("Client {i} finished");
        }));
    }

    for t in tasks {
        let _ = t.await;
    }

    println!("All clients done");
}
