pub mod database;
pub mod error;
pub mod format;
pub mod parser;
pub mod history;

use crate::database::{Database, Table};
use crate::format::{format_table, format_table_from_db};
use crate::parser::{parse_sql, SqlAst};
pub use history::CommandHistory;

// 添加注释处理函数
fn remove_comments(input: &str) -> String {
    let mut in_block_comment = false;
    let mut in_line_comment = false;
    let mut result = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match (c, in_block_comment, in_line_comment) {
            // 检测块注释开始
            ('/', false, false) if chars.peek() == Some(&'*') => {
                in_block_comment = true;
                chars.next(); // 跳过'*'
            },
            // 检测块注释结束
            ('*', true, false) if chars.peek() == Some(&'/') => {
                in_block_comment = false;
                chars.next(); // 跳过'/'
            },
            // 检测行注释开始
            ('-', false, false) if chars.peek() == Some(&'-') => {
                in_line_comment = true;
                chars.next(); // 跳过第二个'-'
            },
            // 处理换行符（行注释结束）
            ('\n', _, true) => {
                in_line_comment = false;
                result.push(c); // 保留换行符
            },
            // 有效字符处理
            (c, false, false) => {
                result.push(c);
            },
            _ => {}
        }
    }

    result
}

pub fn execute_sql(
    sql_statement: &str,
    db: &mut database::Database,
    history: &mut history::CommandHistory
) -> bool {
    if sql_statement.trim().to_uppercase() == "HISTORY" {
        return false;
    }
    // 处理注释
    let clean_sql = remove_comments(sql_statement);
    
    // 加载数据库
    let mut db = match Database::load() {
        Ok(db) => db,
        Err(_) => Database::new(),
    };

    // 分割SQL语句（支持分号分隔的多条语句）
    let statements: Vec<&str> = clean_sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut has_output = false;
    let mut has_error = false;
    let statements_len = statements.len();

    // 处理每条SQL语句
    for stmt in statements {
        match parse_sql(stmt) {
            Ok(ast) => {
                match ast {
                    SqlAst::Select { table, columns, where_clause, order_by } => {
                        let cols_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                        let cond_str = where_clause.as_deref();
                        let order_by_ref = order_by.iter()
                            .map(|(col, desc)| (col.as_str(), *desc))
                            .collect::<Vec<_>>();

                        match db.select(&table, cols_ref, cond_str, Some(order_by_ref)) {
                            Ok(data) if !data.is_empty() => {
                                has_output = true;
                                let formatted = format_table_from_db(
                                    &db, 
                                    &table, 
                                    columns.iter().map(|s| s.as_str()).collect(), 
                                    data
                                );
                                match formatted {
                                    Ok(table_str) => println!("{}\n", table_str),
                                    Err(e) => {
                                        eprintln!("{}", e);
                                        has_error = true;
                                    },
                                }
                            }
                            Ok(_) => {} // 空结果不输出
                            Err(e) => {
                                eprintln!("{}", e);
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Calculate { expression, result } => {
                        has_output = true;
                        let headers = vec![expression];
                        let data = vec![vec![result.to_string()]];
                        println!("{}\n", format_table(headers, data));
                    }
                    SqlAst::CreateTable { table_name, columns } => {
                        let col_defs: Vec<(&str, _, bool, bool)> = columns.iter()
                            .map(|(name, dt, pk, nn)| (name.as_str(), dt.clone(), *pk, *nn))
                            .collect();
                        if let Err(e) = db.create_table(&table_name, col_defs) {
                            eprintln!("{}", e);
                            has_error = true;
                        }
                    }
                    SqlAst::Insert { table, columns, values } => {
                        let values_ref: Vec<Vec<&str>> = values.iter()
                            .map(|row| row.iter().map(|s| s.as_str()).collect())
                            .collect();
                        match db.insert(&table, columns, values_ref) {
                            Ok(count) => {
                                has_output = true;
                                println!("{} row(s) inserted\n", count);
                            }
                            Err(e) => {
                                // 特殊处理主键重复错误
                                if e.contains("Duplicate entry") {
                                    let value = e.split("'").nth(1).unwrap_or("");
                                    eprintln!("Error: Duplicate entry '{}' for key 'PRIMARY'", value);
                                } else if e.contains("cannot be null") {
                                    let col_name = e.split("'").nth(1).unwrap_or("");
                                    eprintln!("Field '{}' doesn't have a default value", col_name);
                                } else {
                                    eprintln!("{}", e);
                                }
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Update { table, set, where_clause } => {
                        let cond_str = where_clause.as_deref();
                        match db.update(&table, set, cond_str) {
                            Ok(count) => {
                                has_output = true;
                                println!("{} row(s) updated\n", count);
                            }
                            Err(e) => {
                                if e.contains("Duplicate entry") {
                                    let value = e.split("'").nth(1).unwrap_or("");
                                    eprintln!("Error: Duplicate entry '{}' for key 'PRIMARY'", value);
                                } else if e.contains("cannot be null") {
                                    let col_name = e.split("'").nth(1).unwrap_or("");
                                    eprintln!("Field '{}' doesn't have a default value", col_name);
                                } else {
                                    eprintln!("{}", e);
                                }
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Delete { table, where_clause } => {
                        let cond_str = where_clause.as_deref();
                        match db.delete(&table, cond_str) {
                            Ok(count) => {
                                has_output = true;
                                println!("{} row(s) deleted\n", count);
                            }
                            Err(e) => {
                                eprintln!("{}", e);
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Drop { tables, if_exists } => {
                        match db.drop_tables(&tables, if_exists) {
                            Ok(count) => {
                                has_output = true;
                                println!("Dropped {} table(s)\n", count);
                            }
                            Err(e) => {
                                eprintln!("{}", e);
                                has_error = true;
                            },
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: Syntax error");
                has_error = true;
            }
        }
    }

    // 如果没有输出任何结果（且没有错误），显示提示信息
    if !has_output && !has_error && statements_len > 0 {
        println!("There are no results to be displayed.");
    }

    // 保存数据库
    if let Err(e) = db.save() {
        eprintln!("Failed to save database: {}", e);
        return false;
    }

    !has_error
}
