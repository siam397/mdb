pub mod config {

    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    pub struct Config {
        pub data_file: String,
    }
}
