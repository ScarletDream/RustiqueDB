use rustique_db::{database::{Database, DataType}, format};

fn main() -> Result<(), String> {
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(100), false, false),
        ("age", DataType::Int(32), false, false),
    ]);

    db.insert("users", vec!["1", "Alice", "30"])?;
    db.insert("users", vec!["2", "Bob", "25"])?;
    db.insert("users", vec!["3", "Charlie", "35"])?;

    // 按年龄升序
    let data = db.select("users", vec!["name", "age"], None, Some(("age", false)))?;
    println!("Age ASC:\n{}", format::format_table_from_db(&db, "users", vec!["name", "age"], data)?);

    // 按姓名降序
    let data = db.select("users", vec!["*"], None, Some(("name", true)))?;
    println!("\nName DESC:\n{}", format::format_table_from_db(&db, "users", vec!["*"], data)?);

    Ok(())
}
