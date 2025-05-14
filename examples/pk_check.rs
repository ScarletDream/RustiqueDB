use rustique_db::database::{Database, DataType};

fn main() {
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),   // 主键+非空
        ("name", DataType::Varchar(100), false, false),
    ]);

    // 测试1：正常插入
    db.insert("users", vec!["1", "Alice"]).unwrap();

    // 测试2：主键重复
    let err = db.insert("users", vec!["1", "Bob"]).unwrap_err();
    println!("Error 1: {}", err); // 应输出：Error: Duplicate entry '1' for key 'PRIMARY'

    // 测试3：非空约束
    let err = db.insert("users", vec!["", "Charlie"]).unwrap_err();
    println!("Error 2: {}", err); // 应输出：Field 'id' doesn't have a default value
}
