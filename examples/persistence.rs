use rustique_db::database::{Database, DataType};

fn main() -> Result<(), String> {
    // 创建新数据库
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(100), false, false),
    ]);

    // 插入数据
    db.insert("users", vec!["1", "Alice"])?;
    db.insert("users", vec!["2", "Bob"])?;

    // 保存到文件
    db.save()?;
    println!("Database saved to data/db.json");

    // 从文件加载
    let loaded_db = Database::load()?;
    println!("Loaded database: {:#?}", loaded_db);

    Ok(())
}
