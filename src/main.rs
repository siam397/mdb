use std::{collections::HashMap, io};

fn main() {

    let mut map:HashMap<String, String> = HashMap::new();

    println!("Welcome to minidb");

    loop{
        let mut user_instruction = String::new();
        let mut user_input_key = String::new();
        let mut user_input_value = String::new();

        io::stdin()
            .read_line(&mut user_instruction)
            .expect("Failed to read instruction");

        if user_instruction.trim() == "GET"{
            println!("Please provide key");

            io::stdin()
                .read_line(&mut user_input_key)
                .expect("Failed to read key");

            let found_value =  map.get(&user_input_key);

            match found_value {
                Some(val)=> {
                    println!("{}", val);
                },
                None=>println!("No key found")
            }

            continue;
        }



        io::stdin()
            .read_line(&mut user_input_key)
            .expect("Failed to read key");


        io::stdin()
            .read_line(&mut user_input_value)
            .expect("Failed to read value");
        
        println!("Key inserted");

        map.insert(user_input_key, user_input_value);

    }

}
