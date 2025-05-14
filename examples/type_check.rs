use rustique_db::database::{Database, DataType};

fn main() {
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(5), false, false), // 最大长度5
    ]);

    // 测试1：正确数据
    db.insert("users", vec!["1", "Alice"]).unwrap();

    // 测试2：INT类型错误
    let err = db.insert("users", vec!["not_number", "Bob"]).unwrap_err();
    println!("Error 1: {}", err); // 应输出：Value 'not_number' is not INT for column 'id'

    // 测试3：VARCHAR长度超限
    let err = db.insert("users", vec!["2", "TooLongName"]).unwrap_err();
    println!("Error 2: {}", err); // 应输出：Value too long for column 'name' (max 5)
}
