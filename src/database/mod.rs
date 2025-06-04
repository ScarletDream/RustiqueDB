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
        values: Vec<Vec<&str>>, // 改为接收多行数据
    ) -> Result<usize, String> { // 返回插入的行数
        let table = self.tables.iter_mut()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        let mut inserted_rows = 0;

        for row_values in values {
            // 列数检查
            if row_values.len() != table.columns.len() {
                return Err("Column count mismatch".into());
            }

            // 检查NOT NULL约束和主键
            for (i, (value, column)) in row_values.iter().zip(&table.columns).enumerate() {
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
                let pk_value = row_values[pk_index];
                if !pk_value.trim().is_empty() && !pk_value.trim().eq_ignore_ascii_case("null") {
                    if table.data.iter().any(|row| row[pk_index] == pk_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", pk_value));
                    }
                }
            }

            let row: Vec<String> = row_values.iter().map(|s| {
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
        set: Vec<(String, String)>,  // 已修改为 String
        condition: Option<&str>,
    ) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = self.tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        // 2. 提前收集所有需要的列信息 (无需修改)
        let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
        let not_null_flags: Vec<bool> = table.columns.iter().map(|c| c.not_null).collect();
        let is_primary_flags: Vec<bool> = table.columns.iter().map(|c| c.is_primary).collect();

        // 3. 创建列名到索引的映射 (修改为使用 String)
        let column_map: std::collections::HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();

        // 4. 检查主键唯一性 (修改为使用 String)
        for (col_name, new_value) in &set {
            if let Some(idx) = column_map.get(col_name) {
                if is_primary_flags[*idx] {
                    if table.data.iter().any(|row| &row[*idx] == new_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", new_value));
                    }
                }
            }
        }

        // 5. 过滤函数 (无需修改)
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

        // 6. 执行更新 (修改为使用 String)
        let mut affected_rows = 0;
        for row in &mut table.data {
            if filter_fn(row) {
                affected_rows += 1;
                for (col_name, new_value) in &set {
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

    pub fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), String> {
        let pos = self.tables.iter().position(|t| t.name == table_name);
        
        match pos {
            Some(index) => {
                self.tables.remove(index);
                Ok(())
            }
            None => {
                if if_exists {
                    Ok(())
                } else {
                    Err(format!("Table '{}' doesn't exist", table_name))
                }
            }
        }
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

    // 条件解析器
    fn parse_condition<'a>(
        cond: &'a str,
        table: &'a Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool + 'a>, String> {
        let parts: Vec<&str> = cond.split_whitespace().collect();
        if parts.len() != 3 && !(parts.len() == 4 && parts[1] == "IS" && (parts[3] == "NULL" || parts[3] == "NOT NULL")) {
            return Err("Invalid WHERE format".into());
        }

        let (col, op, val) = if parts.len() == 4 {
            (parts[0], parts[1], parts[2..].join(" "))
        } else {
            (parts[0], parts[1], parts[2].to_string())
        };

        let col_idx = table.columns.iter().position(|c| c.name == col)
            .ok_or(format!("Column '{}' not found", col))?;

        Ok(match op {
            ">" => Box::new(move |row| row[col_idx].parse::<i32>().ok().unwrap_or(0) > val.parse::<i32>().unwrap_or(0)),
            "<" => Box::new(move |row| row[col_idx].parse::<i32>().ok().unwrap_or(0) < val.parse::<i32>().unwrap_or(0)),
            "=" => Box::new(move |row| row[col_idx] == val),
            "IS" => {
                if val == "NULL" {
                    Box::new(move |row| {
                        let val = &row[col_idx];
                        val.is_empty() || val.eq_ignore_ascii_case("NULL")
                    })
                } else if val == "NOT NULL" {
                    Box::new(move |row| {
                        let val = &row[col_idx];
                        !val.is_empty() && !val.eq_ignore_ascii_case("NULL")
                    })
                } else {
                    return Err(format!("Unsupported IS condition: {}", val));
                }
            },
            _ => return Err(format!("Unsupported operator: {}", op)),
        })
    }
}
