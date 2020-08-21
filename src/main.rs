extern crate rmp_serde as rmps;
 
use rumqtt::{MqttClient, MqttOptions, QoS};
use rumqtt::client::Notification;
use std::io::{stdin,stdout,Write};
use std::sync::Arc;
 
use serde::{Serialize, Deserialize};
use rmps::{Serializer, Deserializer};
 
const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: u16 = 1883;
 
/*** MQTT Structs ***/
 
#[derive(Serialize, Deserialize, Debug)]
struct GetData {
    table:       String,
    start_ts:    u32,
    end_ts:      u32
}
 
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MpdRecordType {
    pub id:         u32,        // Record identifier
    pub datalog:    Vec<u8>,    // Byte array of length 'size'
    pub checksum:   u32,        // CRC-32 checksum of 'datalog'
}
 
/********************/
 
#[allow(unused_assignments)]
fn main() {
    let mqtt_options = MqttOptions::new("LocalClient", SERVER_IP, SERVER_PORT);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).unwrap();
    let mut s = String::new();
 
    loop {
        print!("Please enter some text: ");
        s = String::new();
        let _=stdout().flush();
 
        // Read text
        stdin().read_line(&mut s).expect("Did not enter a correct string");
        if let Some('\n')=s.chars().next_back() {
            s.pop();
        }
        if let Some('\r')=s.chars().next_back() {
            s.pop();
        }
 
        // If "exit" is typed, quit the program
        if s == "exit" {
            print!("Quitting.");
            return;
        }
 
        // If "delete" is typed, delete all the test data in DB
        if s == "delete" {
            let buf = Vec::new();
            mqtt_client.publish("topic_delete", QoS::AtLeastOnce, false, buf).unwrap();
            continue;
        }
 
        // If a number is typed, add that many entries to the DB (all data is fake)
        if s.trim().parse::<u32>().is_ok() {
            let mut buf = Vec::new();
            let mut msg_pack = Serializer::new(&mut buf);
            s.serialize(&mut msg_pack).unwrap();
            mqtt_client.publish("topic_add", QoS::AtLeastOnce, false, buf).unwrap();
            continue;
        }
 
        // if "get data" is typed, get all the fake data from the DB and display it
        if s == "get data" {
            let data = GetData{table: String::from("levels"), start_ts: 1577916000, end_ts: 1577926800};
            let mut buf = Vec::new();
            let mut msg_pack = Serializer::new(&mut buf);
            data.serialize(&mut msg_pack).unwrap();
 
            mqtt_client.publish("topic_getdata", QoS::AtLeastOnce, false, buf).unwrap();
            mqtt_client.subscribe("Client", QoS::AtLeastOnce).unwrap();
            // Parse notifications
            for notification in &notifications {
                match notification {
                    Notification::Publish(publish) =>  {
                            // Get payloads. Will come in multiple payloads if over 50.
                            let payload = Arc::try_unwrap(publish.payload).unwrap();
                            
                            // Keep deserializing until an empty packet is received indicating there is no more to read
                            if Deserialize(payload) {
                                break;
                            }
                        },
                    _ => println!("Received something that's not a publish! {:?}. Ignoring...", notification)
                }
            }
 
            continue;
        }
 
        println!("You typed: {}",s);
    }
}
 
#[allow(non_snake_case, unused_variables)]
fn Deserialize(payload: Vec<u8>) -> bool {
    let mut count = 0;
    let mut de = Deserializer::new(&payload[..]);

    // Deserialize until there is nothing left, break on error
    loop {
        let entry: MpdRecordType = match Deserialize::deserialize(&mut de) {
            Ok(entry) => entry,
            Err(_) => {
                println!("Deserialized {} entries", count);
                break;
            }
        };
        count += 1;
    }
 
    // Return indication of whether or not an empty packet was received
    if count == 0 {
        return true;
    } else {
        return false;
    }
}