use rustique_db::{database::{Database, DataType}, format};

fn main() -> Result<(), String> {
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(100), false, false),
        ("age", DataType::Int(32), false, false), // age 允许为 NULL
    ]);

    // 成功插入 age 为空的记录
    db.insert("users", vec!["3", "Charlie", ""])?;

    // 测试条件查询
    let data = db.select("users", vec!["name", "age"], Some("age IS NULL"))?;
    println!("Age IS NULL:\n{}", format::format_table(vec!["Name".into(), "Age".into()], data));

    Ok(())
}
