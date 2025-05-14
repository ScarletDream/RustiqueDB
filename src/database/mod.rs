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

#[derive(Debug, Serialize, Deserialize)]
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
    ) {
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
    }

    // 数据插入方法
    pub fn insert(&mut self, table_name: &str, values: Vec<&str>) -> Result<(), String> {
        let table = self.tables.iter_mut()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        // 列数检查
        if values.len() != table.columns.len() {
            return Err("Column count mismatch".into());
        }

        // 非空约束检查
        for (value, column) in values.iter().zip(&table.columns) {
            if column.not_null && value.is_empty() {
                return Err(format!("Field '{}' doesn't have a default value", column.name));
            }
        }

        // 类型检查
        for (i, (value, column)) in values.iter().zip(&table.columns).enumerate() {
            if value.trim().is_empty() {
                continue; // 跳过空值检查
            }
            match (&column.data_type, *value) {
                (DataType::Int(_), v) if v.parse::<i32>().is_err() => {
                    return Err(format!("Value '{}' is not INT for column '{}'", v, column.name));
                },
                (DataType::Varchar(max_len), v) if v.len() > *max_len as usize => {
                    return Err(format!("Value too long for column '{}' (max {})", column.name, max_len));
                },
                _ => {} // 类型正确
            }
        }

        // 主键唯一性检查
        if let Some(pk_index) = table.columns.iter().position(|c| c.is_primary) {
            let pk_value = values[pk_index];
            if table.data.iter().any(|row| row[pk_index] == pk_value) {
                return Err(format!("Error: Duplicate entry '{}' for key 'PRIMARY'", pk_value));
            }
        }

        table.data.push(values.iter().map(|s| s.to_string()).collect());
        Ok(())
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

    pub fn select(
        &self,
        table_name: &str,
        columns: Vec<&str>,
        condition: Option<&str>,
        order_by: Option<Vec<(&str, bool)>>  // 多列排序
    ) -> Result<Vec<Vec<String>>, String> {
        let table = self.tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        // 获取列索引
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

        // 获取排序索引和方向
        let sort_specs: Vec<(usize, &DataType, bool)> = if let Some(cols) = order_by {
            cols.into_iter().map(|(col, desc)| {
                let table_col_idx = table.columns.iter().position(|c| c.name == col)
                    .ok_or(format!("Sort column '{}' not found", col))?;
                let result_col_idx = column_indices.iter().position(|&i| i == table_col_idx)
                    .ok_or(format!("Sort column '{}' not in selected columns", col))?;
                Ok((result_col_idx, &table.columns[table_col_idx].data_type, desc))
            }).collect::<Result<_, String>>()?
        } else {
            Vec::new()
        };

        // 过滤并映射结果
        let mut result: Vec<Vec<String>> = table.data
            .iter()
            .filter(|row| filter_fn(row))
            .map(|row| {
                column_indices.iter().map(|&i| row[i].clone()).collect()
            })
            .collect();

        // 执行排序
        if !sort_specs.is_empty() {
            result.sort_by(|a, b| {
                for (idx, data_type, desc) in &sort_specs {
                    let ordering = match data_type {
                        DataType::Int(_) => {
                            let a_val = a[*idx].parse::<i32>().unwrap_or(0);
                            let b_val = b[*idx].parse::<i32>().unwrap_or(0);
                            a_val.cmp(&b_val)
                        },
                        DataType::Varchar(_) => a[*idx].cmp(&b[*idx]),
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

        Ok(result)
    }

    // 条件解析器
    fn parse_condition<'a>(
        cond: &'a str,
        table: &'a Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool + 'a>, String> {
        let parts: Vec<&str> = cond.split_whitespace().collect();
        if parts.len() != 3 && !(parts.len() == 4 && parts[1] == "IS" && parts[3] == "NULL") {
            return Err("Invalid WHERE format".into());
        }

        // 处理 "IS NULL" 和 "IS NOT NULL"
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
            "IS" if val == "NULL" => Box::new(move |row| row[col_idx].is_empty()),
            "IS" if val == "NOT NULL" => Box::new(move |row| !row[col_idx].is_empty()),
            _ => return Err(format!("Unsupported operator: {}", op)),
        })
    }
}
