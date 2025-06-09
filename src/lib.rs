pub mod database;
pub mod error;
pub mod format;
pub mod parser;

use crate::database::{Database, Table};
use crate::format::{format_table, format_table_from_db};
use crate::parser::{parse_sql, SqlAst};

// 注释处理函数保持不变
fn remove_comments(input: &str) -> String {
    let mut in_block_comment = false;
    let mut in_line_comment = false;
    let mut result = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match (c, in_block_comment, in_line_comment) {
            ('/', false, false) if chars.peek() == Some(&'*') => {
                in_block_comment = true;
                chars.next();
            },
            ('*', true, false) if chars.peek() == Some(&'/') => {
                in_block_comment = false;
                chars.next();
            },
            ('-', false, false) if chars.peek() == Some(&'-') => {
                in_line_comment = true;
                chars.next();
            },
            ('\n', _, true) => {
                in_line_comment = false;
                result.push(c);
            },
            (c, false, false) => {
                result.push(c);
            },
            _ => {}
        }
    }

    result
}

pub fn execute_sql(sql_statement: &str) -> bool {
    let clean_sql = remove_comments(sql_statement);
    
    let mut db = match Database::load() {
        Ok(db) => db,
        Err(_) => Database::new(),
    };

    let statements: Vec<&str> = clean_sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut has_output = false;
    let mut has_error = false;
    let statements_len = statements.len();

    for stmt in statements {
        if has_error {
            continue;
        }
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
                                    Ok(table_str) => print!("{}\n", table_str),
                                    Err(e) => {
                                        eprint!("{}\n", e);
                                        has_error = true;
                                    },
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                eprint!("{}\n", e);
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Calculate { expression, result } => {
                        has_output = true;
                        let headers = vec![expression];
                        let data = vec![vec![result.to_string()]];
                        print!("{}\n", format_table(headers, data));
                    }
                    SqlAst::CreateTable { table_name, columns } => {
                        let col_defs: Vec<(&str, _, bool, bool)> = columns.iter()
                            .map(|(name, dt, pk, nn)| (name.as_str(), dt.clone(), *pk, *nn))
                            .collect();
                        if let Err(e) = db.create_table(&table_name, col_defs) {
                            eprint!("{}\n", e);
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
                                //print!("{} row(s) inserted\n", count);
                            }
                            Err(e) => {
                                if e.contains("doesn't have a default value") {
                                    let col_name = e.split('\'').nth(1).unwrap_or("");
                                    eprint!("Error: Field '{}' doesn't have a default value\n", col_name);
                                } else if e.contains("Duplicate entry") {
                                    let value = e.split('\'').nth(1).unwrap_or("");
                                    eprint!("Error: Duplicate entry '{}' for key 'PRIMARY'\n", value);
                                } else {
                                    eprint!("Error: {}\n", e);
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
                                //print!("{} row(s) updated\n", count);
                            }
                            Err(e) => {
                                if e.contains("doesn't have a default value") {
                                    let col_name = e.split('\'').nth(1).unwrap_or("");
                                    eprint!("Error: Field '{}' doesn't have a default value\n", col_name);
                                } else if e.contains("Duplicate entry") {
                                    let value = e.split('\'').nth(1).unwrap_or("");
                                    eprint!("Error: Duplicate entry '{}' for key 'PRIMARY'\n", value);
                                } else {
                                    eprint!("Error: {}\n", e);
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
                                //print!("{} row(s) deleted\n", count);
                            }
                            Err(e) => {
                                eprint!("{}\n", e);
                                has_error = true;
                            },
                        }
                    }
                    SqlAst::Drop { tables, if_exists } => {
                        match db.drop_tables(&tables, if_exists) {
                            Ok(count) => {
                                has_output = true;
                                //print!("Dropped {} table(s)\n", count);
                            }
                            Err(e) => {
                                eprint!("{}\n", e);
                                has_error = true;
                            },
                        }
                    }
                }
            }
            Err(e) => {
                eprint!("Error: Syntax error\n");
                has_error = true;
            }
        }
    }

    if !has_output && !has_error && statements_len > 0 {
        print!("There are no results to be displayed.\n");
    }

    if let Err(e) = db.save() {
        eprint!("Failed to save database: {}\n", e);
        return false;
    }

    !has_error
}
