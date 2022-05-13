#![allow(dead_code)]

use std::fs::read_to_string;

// mod test_concurrent_put_get;
// mod test_multi_raft_group_concurrent_put_get;

const PORT_CONFIG_PATH: &str = "etc/port.toml";

fn concat_toml(path1: &str, path2: &str) -> String {
    let mut s = String::default();
    s.push_str(&read_to_string(path1).unwrap());
    s.push('\n');
    s.push_str(&read_to_string(path2).unwrap());
    s
}

fn port(name: &str) -> u16 {
    let table = read_to_string(PORT_CONFIG_PATH)
        .unwrap()
        .parse::<toml::Value>()
        .unwrap();
    let value = match table {
        toml::Value::Table(ports) => ports[name].clone(),
        _ => unreachable!(),
    };
    match value {
        toml::Value::Integer(port) => port as u16,
        _ => unreachable!(),
    }
}
