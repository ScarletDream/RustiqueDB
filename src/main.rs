use std::io::{self, Write};
use rustique_db::database::{Database, DataType};
use rustique_db::format::format_table_from_db;
use rustique_db::parser::{parse_sql, SqlAst};

fn main() {
    // 加载或创建数据库
    let mut db = Database::load().unwrap_or_else(|_| {
        println!("Creating new database...");
        Database::new()
    });

    println!("Welcome to RustiqueDB!");
    println!("Database loaded with {} tables", db.tables.len());
    
    println!("Enter SQL commands (type 'exit' to quit, use ; to end commands):");
    
    loop {
        print!("sql> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        
        // 读取初始行
        if io::stdin().read_line(&mut input).is_err() {
            println!("Failed to read input.");
            continue;
        }

        // 检查退出命令
        let trimmed = input.trim_end().to_lowercase();
        if trimmed == "exit" || trimmed == "exit;" {
            // 退出前保存数据库
            if let Err(e) = db.save() {
                eprintln!("Failed to save database: {}", e);
            }
            println!("Goodbye!");
            break;
        }

        // 多行输入处理
        while !input.trim_end().ends_with(';') {
            print!("...> ");
            io::stdout().flush().unwrap();

            let mut next_line = String::new();
            if io::stdin().read_line(&mut next_line).is_err() {
                println!("Failed to read input.");
                break;
            }
            
            input.push_str(&next_line);
            
            // 再次检查退出命令（可能在多行输入中）
            let full_trimmed = input.trim_end().to_lowercase();
            if full_trimmed == "exit" || full_trimmed == "exit;" {
                if let Err(e) = db.save() {
                    eprintln!("Failed to save database: {}", e);
                }
                println!("Goodbye!");
                return;
            }
        }

        // 移除结尾的分号
        let sql_input = input.trim_end().trim_end_matches(';').trim();
        if sql_input.is_empty() {
            continue;
        }

        match parse_sql(sql_input) {
            Ok(ast) => {
                match ast {
                    SqlAst::Select { table, columns, where_clause } => {
                        let cols_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                        let cond_str = where_clause.as_deref();
                        
                        match db.select(&table, cols_ref, cond_str, None) {
                            Ok(data) => {
                                match format_table_from_db(&db, &table, columns.iter().map(|s| s.as_str()).collect(), data) {
                                    Ok(table_str) => println!("{}", table_str),
                                    Err(e) => eprintln!("Format error: {}", e),
                                }
                            }
                            Err(e) => eprintln!("Select error: {}", e),
                        }
                    }
                    SqlAst::CreateTable { table_name, columns } => {
                        // 将列定义转换为数据库需要的格式
                        let col_defs: Vec<(&str, DataType, bool, bool)> = columns.iter()
                            .map(|(name, dt, pk, nn)| (name.as_str(), dt.clone(), *pk, *nn))
                            .collect();
                        
                        // 正确调用 create_table（不处理返回值）
                        db.create_table(&table_name, col_defs);
                        println!("Table '{}' created successfully", table_name);
                        
                        // 保存数据库
                        if let Err(e) = db.save() {
                            eprintln!("Failed to save database: {}", e);
                        }
                    }
                    SqlAst::Insert { table, values } => {
                        // 转换为引用数组
                        let values_ref: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                        
                        // 正确调用 insert（不处理返回值）
                        db.insert(&table, values_ref);
                        println!("1 row inserted");
                        
                        // 保存数据库
                        if let Err(e) = db.save() {
                            eprintln!("Failed to save database: {}", e);
                        }
                    }
                    SqlAst::Update { table, set, where_clause } => {
                        let cond_str = where_clause.as_deref();
                        let set_ref = set;  // 直接使用 Vec<(String, String)>


                        
                        match db.update(&table, set_ref, cond_str) {
                            Ok(count) => {
                                println!("{} row(s) updated", count);
                                // 保存数据库
                                if let Err(e) = db.save() {
                                    eprintln!("Failed to save database: {}", e);
                                }
                            }
                            Err(e) => eprintln!("Update error: {}", e),
                        }
                    }
                    SqlAst::Delete { table, where_clause } => {
                        let cond_str = where_clause.as_deref();
                        
                        match db.delete(&table, cond_str) {
                            Ok(count) => {
                                println!("{} row(s) deleted", count);
                                // 保存数据库
                                if let Err(e) = db.save() {
                                    eprintln!("Failed to save database: {}", e);
                                }
                            }
                            Err(e) => eprintln!("Delete error: {}", e),
                        }
                    }
                }
            }
            Err(e) => eprintln!("Parse error: {}", e),
        }
    }
}
