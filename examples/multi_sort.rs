use rustique_db::{database::{Database, DataType}, format};

fn main() -> Result<(), String> {
    let mut db = Database::new();
    db.create_table("users", vec![
        ("id", DataType::Int(32), true, true),
        ("name", DataType::Varchar(100), false, false),
        ("age", DataType::Int(32), false, false),
        ("score", DataType::Int(32), false, false),
    ]);

    db.insert("users", vec!["1", "Alice", "30", "85"])?;
    db.insert("users", vec!["2", "Bob", "25", "90"])?;
    db.insert("users", vec!["3", "Alice", "35", "80"])?;

    // 多列排序：先按name升序，再按age降序
    let data = db.select(
        "users",
        vec!["name", "age", "score"],
        None,
        Some(vec![("name", false), ("age", true)])
    )?;

    println!("Multi-column sort:\n{}",
        format::format_table(
            vec!["Name".into(), "Age".into(), "Score".into()],
            data
        )
    );

    Ok(())
}
