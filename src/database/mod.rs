use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

// 为所有需要序列化的类型添加derive
#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub data: Vec<Vec<String>>,  // Vec<String> 本身是可序列化的
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataType {
    Int(u32),
    Varchar(u32),
}

#[derive(Debug, Serialize, Deserialize,Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary: bool,
    pub not_null: bool,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    // 创建表方法
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<(&str, DataType, bool, bool)>, // (列名, 类型, 是否主键, 是否非空)
    )-> Result<(), String>{

        let normalized_name = name.trim().to_lowercase();
        
        // 原子化检查-创建操作
        let exists = self.tables.iter().any(|t| t.name.to_lowercase() == normalized_name);
        if exists {
            return Err(format!("[REJECTED] Table '{}' exists", normalized_name)); // 确保此返回不可跳过
        }
        self.tables.push(Table {
            name: name.to_string(),
            columns: columns
                .into_iter()
                .map(|(name, data_type, is_primary, not_null)| Column {
                    name: name.to_string(),
                    data_type,
                    is_primary,
                    not_null,
                })
                .collect(),
            data: Vec::new(),
        });
        Ok(())
    }

    // 数据插入方法
    pub fn insert(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>, // 新增：可选列名列表
        values: Vec<Vec<&str>>,
    ) -> Result<usize, String> {
        let table = self.tables.iter_mut()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        let mut inserted_rows = 0;

        for row_values in values {
            // 处理部分插入
            let full_row_values = if let Some(col_names) = &columns {
                // 创建完整行数据，未指定的列设为空字符串
                let mut full_row = vec![""; table.columns.len()];
                
                // 检查列名是否匹配
                if col_names.len() != row_values.len() {
                    return Err("Column count mismatch in INSERT statement".into());
                }
                
                for (i, col_name) in col_names.iter().enumerate() {
                    let col_index = table.columns.iter()
                        .position(|c| &c.name == col_name)
                        .ok_or(format!("Column '{}' not found", col_name))?;
                    
                    full_row[col_index] = row_values[i];
                }
                
                full_row
            } else {
                // 全列插入
                if row_values.len() != table.columns.len() {
                    return Err("Column count mismatch".into());
                }
                row_values
            };

            // 检查NOT NULL约束和主键
            for (i, (value, column)) in full_row_values.iter().zip(&table.columns).enumerate() {
                let is_null = value.trim().is_empty() || value.trim().eq_ignore_ascii_case("null");
                
                if column.not_null && is_null {
                    return Err(format!("Column '{}' cannot be null", column.name));
                }
                
                if column.is_primary && is_null {
                    return Err(format!("Primary key '{}' cannot be null", column.name));
                }
            }

            // 主键唯一性检查
            if let Some(pk_index) = table.columns.iter().position(|c| c.is_primary) {
                let pk_value = full_row_values[pk_index];
                if !pk_value.trim().is_empty() && !pk_value.trim().eq_ignore_ascii_case("null") {
                    if table.data.iter().any(|row| row[pk_index] == pk_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", pk_value));
                    }
                }
            }

            let row: Vec<String> = full_row_values.iter().map(|s| {
                if s.trim().eq_ignore_ascii_case("null") {
                    String::new()
                } else {
                    s.to_string()
                }
            }).collect();
            
            table.data.push(row);
            inserted_rows += 1;
        }

        Ok(inserted_rows)
    }

    pub fn update(
        &mut self,
        table_name: &str,
        set: Vec<(String, String)>,
        condition: Option<&str>,
    ) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = self.tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        // 2. 提前收集所有需要的列信息
        let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
        let not_null_flags: Vec<bool> = table.columns.iter().map(|c| c.not_null).collect();
        let is_primary_flags: Vec<bool> = table.columns.iter().map(|c| c.is_primary).collect();

        // 3. 创建列名到索引的映射
        let column_map: std::collections::HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();

        // 4. 预处理set值，移除字符串值的引号
        let processed_set: Vec<(String, String)> = set.into_iter()
            .map(|(col_name, value)| {
                // 统一处理值格式，与insert保持一致
                let processed_value = if value.starts_with('"') && value.ends_with('"') {
                    value.trim_matches('"').to_string()
                } else if value.starts_with('\'') && value.ends_with('\'') {
                    value.trim_matches('\'').to_string()
                } else if value.eq_ignore_ascii_case("null") {
                    String::new() // 存储为""表示NULL
                } else {
                    value
                };
                (col_name, processed_value)
            })
            .collect();

        // 5. 检查主键唯一性
        for (col_name, new_value) in &processed_set {
            if let Some(idx) = column_map.get(col_name) {
                if is_primary_flags[*idx] {
                    if table.data.iter().any(|row| &row[*idx] == new_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", new_value));
                    }
                }
            }
        }

        // 6. 创建过滤闭包
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            let columns = table.columns.clone();
            Box::new(move |row: &[String]| {
                let temp_table = Table {
                    name: String::new(),
                    columns: columns.clone(),
                    data: vec![],
                };
                match Self::parse_condition(cond, &temp_table) {
                    Ok(filter) => filter(row),
                    Err(_) => false,
                }
            })
        } else {
            Box::new(|_| true)
        };

        // 7. 执行更新
        let mut affected_rows = 0;
        for row in &mut table.data {
            if filter_fn(row) {
                affected_rows += 1;
                for (col_name, new_value) in &processed_set {
                    if let Some(idx) = column_map.get(col_name) {
                        // 类型检查
                        match &column_types[*idx] {
                            DataType::Int(_) if new_value.parse::<i32>().is_err() => {
                                return Err(format!("Value '{}' is not INT for column '{}'", 
                                    new_value, col_name));
                            },
                            DataType::Varchar(max_len) if new_value.len() > *max_len as usize => {
                                return Err(format!("Value too long for column '{}' (max {})", 
                                    col_name, max_len));
                            },
                            _ => {}
                        }

                        // 非空检查
                        if not_null_flags[*idx] && new_value.is_empty() {
                            return Err(format!("Column '{}' cannot be null", col_name));
                        }

                        row[*idx] = new_value.clone();
                    }
                }
            }
        }

        Ok(affected_rows)
    }

    pub fn delete(&mut self,table_name: &str,condition: Option<&str>,) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = self.tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        // 2. 提前复制所需的列信息
        let columns = table.columns.clone();

        // 3. 创建过滤闭包
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            // 使用提前复制的列信息
            Box::new(move |row: &[String]| {
                let local_table = Table {
                    name: String::new(),
                    columns: columns.clone(),
                    data: vec![],
                };
                match Self::parse_condition(cond, &local_table) {
                    Ok(filter) => filter(row),
                    Err(_) => false, // 解析失败时不匹配任何行
                }
            })
        } else {
            Box::new(|_| true) // 无条件时匹配所有行
        };

        // 4. 执行删除操作
        let original_len = table.data.len();
        table.data.retain(|row| !filter_fn(row));
        let affected_rows = original_len - table.data.len();

        Ok(affected_rows)
    }

    pub fn save(&self) -> Result<(), String> {
        // 创建data目录（如果不存在）
        fs::create_dir_all("data").map_err(|e| e.to_string())?;

        // 序列化为JSON并保存
        let json = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        fs::write("data/db.json", json).map_err(|e| e.to_string())?;

        Ok(())
    }

    pub fn load() -> Result<Self, String> {
        // 检查文件是否存在
        if !Path::new("data/db.json").exists() {
            return Ok(Database::new());
        }

        // 读取并反序列化
        let json = fs::read_to_string("data/db.json").map_err(|e| e.to_string())?;
        serde_json::from_str(&json).map_err(|e| e.to_string())
    }

    pub fn drop_tables(&mut self, table_names: &[String], if_exists: bool) -> Result<usize, String> {
        let original_count = self.tables.len();
        
        // 只有 if_exists=false 时才检查存在性
        if !if_exists {
            for name in table_names {
                if !self.tables.iter().any(|t| &t.name == name) {
                    return Err(format!("Table '{}' doesn't exist", name));
                }
            }
        }

        // 执行删除（自动跳过不存在的表）
        self.tables.retain(|table| !table_names.contains(&table.name));
        
        let dropped_count = original_count - self.tables.len();
        
        // 如果实际删除数量为0且指定了必须存在，报错
        if dropped_count == 0 && !if_exists {
            return Err("No tables were dropped".into());
        }
        
        Ok(dropped_count)
    }

    pub fn select(
        &self,
        table_name: &str,
        columns: Vec<&str>,
        condition: Option<&str>,
        order_by: Option<Vec<(&str, bool)>>  // (列名, 是否降序)
    ) -> Result<Vec<Vec<String>>, String> {
        let table = self.tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        // 获取结果列索引
        let column_indices: Vec<usize> = if columns == ["*"] {
            (0..table.columns.len()).collect()
        } else {
            columns.iter().map(|col| {
                table.columns.iter().position(|c| &c.name == col)
                    .ok_or(format!("Column '{}' not found", col))
            }).collect::<Result<_, _>>()?
        };

        // 统一返回 Box<dyn Fn> 类型
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            Self::parse_condition(cond, table)?
        } else {
            Box::new(|_| true) // 将闭包装箱
        };

        // 收集原始行数据（带原始行索引）
        let mut rows_with_indices: Vec<(usize, &Vec<String>)> = table.data
            .iter()
            .enumerate()
            .filter(|(_, row)| filter_fn(row))
            .collect();

        // 处理排序（如果需要）
        if let Some(cols) = order_by {
            // 获取排序列元数据
            let sort_specs: Vec<(usize, &DataType, bool)> = cols.into_iter().map(|(col, desc)| {
                let col_idx = table.columns.iter()
                    .position(|c| c.name == col)
                    .ok_or(format!("Sort column '{}' not found", col))?;
                Ok((col_idx, &table.columns[col_idx].data_type, desc))
            }).collect::<Result<_, String>>()?;

            // 排序逻辑（使用原始数据）
            rows_with_indices.sort_by(|(a_idx, _), (b_idx, _)| {
                let a_row = &table.data[*a_idx];
                let b_row = &table.data[*b_idx];

                for (col_idx, data_type, desc) in &sort_specs {
                    let ordering = match data_type {
                        DataType::Int(_) => {
                            a_row[*col_idx].parse::<i32>().unwrap_or(0)
                                .cmp(&b_row[*col_idx].parse::<i32>().unwrap_or(0))
                        },
                        DataType::Varchar(_) => a_row[*col_idx].cmp(&b_row[*col_idx]),
                    };

                    if *desc {
                        return ordering.reverse();
                    } else if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 构建最终结果
        let result = rows_with_indices.into_iter()
            .map(|(_, row)| {
                column_indices.iter().map(|&i| row[i].clone()).collect()
            })
            .collect();

        Ok(result)
    }

    pub fn parse_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        // 首先检查是否包含 AND 关键字（不区分大小写）
        if cond.to_uppercase().contains(" AND ") {
            return Self::parse_and_condition(cond, table);
        }
        Self::parse_single_condition(cond, table)
    }

    fn parse_single_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        // 原有 parse_condition 的实现内容
        let re = regex::Regex::new(r#"(?:("[^"]*")|('[^']*')|(\S+))"#).unwrap();
        let parts: Vec<&str> = re.find_iter(cond)
            .map(|m| m.as_str())
            .collect();

        if parts.len() != 3 && !(parts.len() == 4 && parts[1] == "IS" && (parts[3] == "NULL" || parts[3] == "NOT NULL")) {
            return Err(format!("Invalid WHERE format. Expected 'column op value', got: {:?}", parts));
        }

        let (col, op, raw_val) = (
            parts[0],
            parts[1],
            if parts.len() == 4 {
                parts[2..].join(" ")
            } else {
                parts[2].to_string()
            }
        );

        let val = raw_val.trim_matches(|c| c == '"' || c == '\'').to_string();
        let col_idx = table.columns.iter()
            .position(|c| c.name == col)
            .ok_or(format!("Column '{}' not found in table", col))?;

        Ok(match op {
            ">" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"').parse::<i32>().unwrap_or(0);
                let cond_val = val.parse::<i32>().unwrap_or(0);
                row_val > cond_val
            }),
            "<" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"').parse::<i32>().unwrap_or(0);
                let cond_val = val.parse::<i32>().unwrap_or(0);
                row_val < cond_val
            }),
            "=" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"');
                row_val == val
            }),
            "IS" if val == "NULL" => Box::new(move |row| {
                row[col_idx].trim_matches('"').is_empty()
            }),
            "IS" if val == "NOT NULL" => Box::new(move |row| {
                !row[col_idx].trim_matches('"').is_empty()
            }),
            _ => return Err(format!("Unsupported operator: {}", op)),
        })
    }
    
    fn parse_and_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        //println!("[DEBUG] Original condition: {}", cond);
        
        // 分割条件，处理可能的嵌套情况
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut in_quotes = false;
        let mut paren_depth = 0;
        let mut chars = cond.chars().peekable();

        while let Some(c) = chars.next() {
            //println!("[DEBUG] Processing char: '{}', in_quotes: {}, paren_depth: {}, current_part: '{}'", 
                //c, in_quotes, paren_depth, current_part);

            match c {
                '"' | '\'' => {
                    in_quotes = !in_quotes;
                    current_part.push(c);
                }
                '(' if !in_quotes => {
                    paren_depth += 1;
                    current_part.push(c);
                }
                ')' if !in_quotes => {
                    paren_depth -= 1;
                    current_part.push(c);
                }
                _ if c.to_ascii_uppercase() == 'A' 
                    && !in_quotes 
                    && paren_depth == 0 
                    && current_part.ends_with(' ') => {
                    
                    // 检查是否是完整的AND关键字
                    let mut and_chars = vec!['A'];
                    for _ in 0..2 {
                        if let Some(&next_c) = chars.peek() {
                            and_chars.push(next_c.to_ascii_uppercase());
                            chars.next();
                        }
                    }

                    if and_chars == ['A', 'N', 'D'] && chars.peek().map_or(true, |c| c.is_whitespace()) {
                        // 确认是AND关键字
                        parts.push(current_part.trim().to_string());
                        current_part.clear();
                    } else {
                        // 不是完整的AND，把字符加回去
                        current_part.push(c);
                        current_part.extend(&and_chars[1..]);
                    }
                }
                _ => current_part.push(c),
            }
        }
        parts.push(current_part.trim().to_string());
        
        //println!("[DEBUG] Split parts: {:?}", parts);

        if parts.len() < 2 {
            return Err("Invalid AND condition".into());
        }

        // 解析各个子条件
        let mut conditions = Vec::new();
        for part in parts {
            //println!("[DEBUG] Parsing part: '{}'", part);
            let cond = Self::parse_single_condition(&part, table)?;
            conditions.push(cond);
        }

        // 组合条件
        Ok(Box::new(move |row| {
            conditions.iter().all(|cond| cond(row))
        }))
    }


}
