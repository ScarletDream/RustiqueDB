use std::io::{self, Write};
use rustique_db::database::{Database, DataType};
use rustique_db::format::format_table_from_db;
use rustique_db::parser::{parse_sql, SqlAst};

// 注释处理
fn remove_comments(input: &str) -> &str {
    let mut in_block_comment = false;
    let mut in_line_comment = false;
    let mut last_valid_pos = 0;
    let bytes = input.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        match (b, in_block_comment, in_line_comment) {
            // 检测块注释开始
            (b'/', _, false) if i+1 < bytes.len() && bytes[i+1] == b'*' => {
                in_block_comment = true;
            },
            // 检测块注释结束
            (b'*', true, _) if i+1 < bytes.len() && bytes[i+1] == b'/' => {
                in_block_comment = false;
            },
            // 检测行注释开始
            (b'-', false, false) if i+1 < bytes.len() && bytes[i+1] == b'-' => {
                in_line_comment = true;
            },
            // 处理换行符（行注释结束）
            (b'\n', _, true) => {
                in_line_comment = false;
                last_valid_pos = i + 1; // 保留换行符保证行号正确
            },
            // 有效字符处理
            (_, false, false) => {
                last_valid_pos = i + 1;
            },
            _ => {}
        }
    }

    // 返回原始输入的切片引用（零拷贝）
    &input[..last_valid_pos]
}

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

        // 移除
        let sql_input = remove_comments(&input)
            .trim()
            .trim_end_matches(';')
            .trim();

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
                        let values_ref: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                        
                        match db.insert(&table, values_ref) {
                            Ok(count) => {
                                println!("{} row(s) inserted", count);
                                if let Err(e) = db.save() {
                                    eprintln!("Failed to save database: {}", e);
                                }
                            }
                            Err(e) => eprintln!("Insert error: {}", e),
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
                    SqlAst::Drop { table_name, if_exists } => {
                        match db.drop_table(&table_name, if_exists) {
                            Ok(()) => {
                                println!("Table '{}' dropped successfully", table_name);
                                // 保存数据库（与其他写操作一致）
                                if let Err(e) = db.save() {
                                    eprintln!("Failed to save database: {}", e);
                                    //return Err(e);
                                }
                                //Ok(())
                            }
                            Err(e) => eprintln!("Drop error: {}", e),
                        }
                    }          
                }
            }
            Err(e) => eprintln!("Parse error: {}", e),
        }
    }
}
