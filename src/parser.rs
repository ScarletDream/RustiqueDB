use crate::database::DataType;
use std::collections::HashMap;

#[derive(Debug)]
pub enum SqlAst {
    Select {
        table: String,
        columns: Vec<String>,
        where_clause: Option<String>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<(String, DataType, bool, bool)>,
    },
    Insert {
        table: String,
        values: Vec<String>,
    },
    Update {
        table: String,
        set: Vec<(String, String)>,
        where_clause: Option<String>,
    },
    Delete {
        table: String,
        where_clause: Option<String>,
    },
}

pub fn parse_sql(input: &str) -> Result<SqlAst, String> {
    let input = input.trim().trim_end_matches(';').to_string();
    let tokens: Vec<&str> = input.split_whitespace().collect();
    
    if tokens.is_empty() {
        return Err("Empty SQL input".to_string());
    }

    match tokens[0].to_uppercase().as_str() {
        "SELECT" => parse_select(&tokens),
        "CREATE" => parse_create(&tokens),
        "INSERT" => parse_insert(&tokens),
        "UPDATE" => parse_update(&tokens),
        "DELETE" => parse_delete(&tokens),
        _ => Err(format!("Unsupported SQL command: {}", tokens[0])),
    }
}

fn parse_select(tokens: &[&str]) -> Result<SqlAst, String> {
    // 定位FROM子句
    let idx_from = tokens.iter().position(|&t| t.to_uppercase() == "FROM")
        .ok_or("Missing 'FROM' clause")?;
    
    // 解析列名
    let cols_str = tokens[1..idx_from].join(" ");
    let columns: Vec<String> = if cols_str.trim() == "*" {
        vec!["*".to_string()]
    } else {
        cols_str.split(',').map(|s| s.trim().to_string()).collect()
    };
    
    // 解析表名
    let table = tokens.get(idx_from + 1)
        .ok_or("Missing table name after FROM")?
        .to_string();
    
    // 解析WHERE子句
    let where_clause = tokens.iter().position(|&t| t.to_uppercase() == "WHERE")
        .map(|idx| tokens[idx+1..].join(" "));
    
    Ok(SqlAst::Select {
        table,
        columns,
        where_clause,
    })
}

fn parse_create(tokens: &[&str]) -> Result<SqlAst, String> {
    if tokens.len() < 3 || tokens[1].to_uppercase() != "TABLE" {
        return Err("Invalid CREATE statement".into());
    }
    
    let table_name = tokens[2].to_string();
    
    // 查找括号位置
    let start = tokens.iter().position(|&t| t == "(")
        .ok_or("Expected '(' after table name")?;
    let end = tokens.iter().position(|&t| t == ")")
        .ok_or("Expected ')' after column definitions")?;
    
    // 解析列定义
    let col_defs = tokens[start+1..end].join(" ");
    let col_parts: Vec<&str> = col_defs.split(',').collect();
    
    let mut columns = Vec::new();
    for col in col_parts {
        let parts: Vec<&str> = col.trim().split_whitespace().collect();
        if parts.len() < 2 {
            return Err(format!("Invalid column definition: {}", col));
        }
        
        let col_name = parts[0].to_string();
        let data_type = if parts[1].to_uppercase().starts_with("VARCHAR") {
            // 处理 VARCHAR(长度) 或 VARCHAR
            if parts[1].contains('(') && parts[1].contains(')') {
                let len_str = parts[1].split('(').nth(1).unwrap().split(')').next().unwrap();
                let len = len_str.parse::<u32>()
                    .map_err(|_| "Invalid length for VARCHAR")?;
                DataType::Varchar(len)
            } else {
                // 默认长度 255
                DataType::Varchar(255)
            }
        } else {
            match parts[1].to_uppercase().as_str() {
                "INT" => DataType::Int(10),
                _ => return Err(format!("Unsupported data type: {}", parts[1])),
            }
        };
        
        // 解析约束
        let is_primary = col.contains("PRIMARY KEY");
        let not_null = col.contains("NOT NULL");
        
        columns.push((col_name, data_type, is_primary, not_null));
    }
    
    Ok(SqlAst::CreateTable {
        table_name,
        columns,
    })
}

fn parse_insert(tokens: &[&str]) -> Result<SqlAst, String> {
    if tokens.len() < 4 || tokens[1].to_uppercase() != "INTO" {
        return Err("Invalid INSERT statement".into());
    }
    
    let table = tokens[2].to_string();
    
    // 查找VALUES关键字
    let idx_values = tokens.iter().position(|&t| t.to_uppercase() == "VALUES")
        .ok_or("Expected VALUES after table name")?;
    
    // 提取值列表
    let values_str = tokens[idx_values+1..].join(" ");
    let values: Vec<String> = values_str.split(',')
        .map(|s| s.trim().trim_matches(|c| c == '(' || c == ')').to_string())
        .collect();
    
    Ok(SqlAst::Insert { table, values })
}

fn parse_update(tokens: &[&str]) -> Result<SqlAst, String> {
    if tokens.len() < 4 {
        return Err("Invalid UPDATE statement".into());
    }
    
    let table = tokens[1].to_string();
    
    // 查找SET关键字
    let idx_set = tokens.iter().position(|&t| t.to_uppercase() == "SET")
        .ok_or("Expected SET after table name")?;
    
    // 解析SET子句
    let set_clause = tokens[idx_set+1..].join(" ");
    let mut set = Vec::new();
    
    for pair in set_clause.split(',') {
        let parts: Vec<&str> = pair.split('=').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return Err(format!("Invalid SET pair: {}", pair));
        }
        set.push((parts[0].to_string(), parts[1].to_string()));
    }
    
    // 解析WHERE子句
    let where_clause = tokens.iter().position(|&t| t.to_uppercase() == "WHERE")
        .map(|idx| tokens[idx+1..].join(" "));
    
    Ok(SqlAst::Update {
        table,
        set,
        where_clause,
    })
}

fn parse_delete(tokens: &[&str]) -> Result<SqlAst, String> {
    if tokens.len() < 3 || tokens[1].to_uppercase() != "FROM" {
        return Err("Invalid DELETE statement".into());
    }
    
    let table = tokens[2].to_string();
    
    // 解析WHERE子句
    let where_clause = tokens.iter().position(|&t| t.to_uppercase() == "WHERE")
        .map(|idx| tokens[idx+1..].join(" "));
    
    Ok(SqlAst::Delete {
        table,
        where_clause,
    })
}
