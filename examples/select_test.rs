use rustique_db::database::{Database, DataType};

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

    // 查询所有列
    println!("All columns:");
    let all_data = db.select("users", vec!["*"], None, None)?;
    for row in all_data {
        println!("{:?}", row);
    }

    // 查询特定列
    println!("\nSpecific columns:");
    let some_data = db.select("users", vec!["name", "age"], None, None)?;
    for row in some_data {
        println!("{:?}", row);
    }

    Ok(())
}
