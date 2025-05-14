use rustique_db::database::{Database, DataType};

fn main() {
    let mut db = Database::new();

    // 创建带数据类型的表
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),   // INT(32) PRIMARY KEY NOT NULL
        ("name", DataType::Varchar(100), false, false), // VARCHAR(100)
    ]);

    // 插入数据（和之前一样）
    db.insert("users", vec!["1", "Alice"]).unwrap();
    db.insert("users", vec!["2", "Bob"]).unwrap();

    println!("Database contents:\n{:#?}", db);
}
