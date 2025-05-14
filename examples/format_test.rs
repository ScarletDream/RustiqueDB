use rustique_db::{database::{Database, DataType}, format};

fn main() -> Result<(), String> {
    // 创建数据库和表（与之前select_test.rs相同）
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(100), false, false),
        ("age", DataType::Int(32), false, false),
    ]);

    // 插入测试数据
    db.insert("users", vec!["1", "Alice", "30"])?;
    db.insert("users", vec!["2", "Bob", "25"])?;
    db.insert("users", vec!["3", "Charlie", "35"])?;

    // 执行查询并格式化输出
    let data = db.select("users", vec!["name", "age"], None)?;
    let headers = vec!["Name".to_string(), "Age".to_string()]; // 注意转为String

    println!("{}", format::format_table(headers, data));
    Ok(())
}
