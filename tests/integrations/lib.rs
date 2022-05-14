use std::fs::read_to_string;

mod test_concurrent_put_get;
mod test_multi_raft_group_concurrent_put_get;

const PORT_CONFIG_PATH: &str = "etc/port.toml";

const RUDDER_CONFIG_PATH: &str = "etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "etc/lsm_tree.toml";

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
